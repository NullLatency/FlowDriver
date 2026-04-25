package storage

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"sync"
	"time"
)

type MultiBackend struct {
	lanes []*storageLane

	fileLane   map[string]int
	fileLaneMu sync.RWMutex
}

type storageLane struct {
	name           string
	backend        Backend
	unhealthyUntil time.Time
	mu             sync.RWMutex
}

func NewMultiBackend(backends []Backend) (*MultiBackend, error) {
	if len(backends) == 0 {
		return nil, fmt.Errorf("multi backend requires at least one lane")
	}

	lanes := make([]*storageLane, 0, len(backends))
	for i, backend := range backends {
		lanes = append(lanes, &storageLane{
			name:    fmt.Sprintf("lane-%d", i),
			backend: backend,
		})
	}
	return &MultiBackend{
		lanes:    lanes,
		fileLane: make(map[string]int),
	}, nil
}

func (b *MultiBackend) Login(ctx context.Context) error {
	var firstErr error
	for _, lane := range b.lanes {
		if err := lane.backend.Login(ctx); err != nil {
			lane.markUnhealthy()
			if firstErr == nil {
				firstErr = fmt.Errorf("%s login failed: %w", lane.name, err)
			}
			continue
		}
		lane.markHealthy()
	}
	if b.hasHealthyLane() {
		return nil
	}
	return firstErr
}

func (b *MultiBackend) Upload(ctx context.Context, filename string, data io.Reader) error {
	payload, err := io.ReadAll(data)
	if err != nil {
		return err
	}

	start := b.pickLane(filename)
	var firstErr error
	for offset := 0; offset < len(b.lanes); offset++ {
		idx := (start + offset) % len(b.lanes)
		lane := b.lanes[idx]
		if !lane.healthy() && offset < len(b.lanes)-1 {
			continue
		}
		if err := lane.backend.Upload(ctx, filename, bytesReader(payload)); err != nil {
			lane.markUnhealthy()
			if firstErr == nil {
				firstErr = fmt.Errorf("%s upload failed: %w", lane.name, err)
			}
			continue
		}
		lane.markHealthy()
		b.setFileLane(filename, idx)
		return nil
	}
	return firstErr
}

func (b *MultiBackend) ListQuery(ctx context.Context, prefix string) ([]string, error) {
	type result struct {
		idx   int
		names []string
		err   error
	}

	ch := make(chan result, len(b.lanes))
	launched := 0
	allowUnhealthy := !b.hasHealthyLane()
	for idx, lane := range b.lanes {
		if !allowUnhealthy && !lane.healthy() {
			continue
		}
		launched++
		go func(idx int, lane *storageLane) {
			names, err := lane.backend.ListQuery(ctx, prefix)
			ch <- result{idx: idx, names: names, err: err}
		}(idx, lane)
	}

	var names []string
	var firstErr error
	for range launched {
		res := <-ch
		lane := b.lanes[res.idx]
		if res.err != nil {
			lane.markUnhealthy()
			if firstErr == nil {
				firstErr = fmt.Errorf("%s list failed: %w", lane.name, res.err)
			}
			continue
		}
		lane.markHealthy()
		for _, name := range res.names {
			b.setFileLane(name, res.idx)
			names = append(names, name)
		}
	}
	if len(names) == 0 && firstErr != nil && !b.hasHealthyLane() {
		return nil, firstErr
	}
	return names, nil
}

func (b *MultiBackend) Download(ctx context.Context, filename string) (io.ReadCloser, error) {
	return b.readFromMappedLane(ctx, filename, func(lane Backend) (io.ReadCloser, error) {
		return lane.Download(ctx, filename)
	})
}

func (b *MultiBackend) Delete(ctx context.Context, filename string) error {
	mappedIdx, ok := b.getFileLane(filename)
	if ok {
		if err := b.lanes[mappedIdx].backend.Delete(ctx, filename); err == nil {
			b.deleteFileLane(filename)
			return nil
		}
		b.lanes[mappedIdx].markUnhealthy()
	}

	var firstErr error
	for idx, lane := range b.lanes {
		if ok && idx == mappedIdx {
			continue
		}
		if err := lane.backend.Delete(ctx, filename); err != nil {
			lane.markUnhealthy()
			if firstErr == nil {
				firstErr = fmt.Errorf("%s delete failed: %w", lane.name, err)
			}
			continue
		}
		lane.markHealthy()
		b.deleteFileLane(filename)
		return nil
	}
	return firstErr
}

func (b *MultiBackend) CreateFolder(ctx context.Context, name string) (string, error) {
	var firstID string
	var firstErr error
	for _, lane := range b.lanes {
		id, err := lane.backend.CreateFolder(ctx, name)
		if err != nil {
			lane.markUnhealthy()
			if firstErr == nil {
				firstErr = fmt.Errorf("%s create folder failed: %w", lane.name, err)
			}
			continue
		}
		lane.markHealthy()
		if firstID == "" {
			firstID = id
		}
	}
	if firstID != "" {
		return firstID, nil
	}
	return "", firstErr
}

func (b *MultiBackend) FindFolder(ctx context.Context, name string) (string, error) {
	var firstID string
	var firstErr error
	for _, lane := range b.lanes {
		id, err := lane.backend.FindFolder(ctx, name)
		if err != nil {
			lane.markUnhealthy()
			if firstErr == nil {
				firstErr = fmt.Errorf("%s find folder failed: %w", lane.name, err)
			}
			continue
		}
		lane.markHealthy()
		if firstID == "" {
			firstID = id
		}
	}
	if firstID != "" || firstErr == nil {
		return firstID, nil
	}
	return "", firstErr
}

func (b *MultiBackend) readFromMappedLane(ctx context.Context, filename string, fn func(Backend) (io.ReadCloser, error)) (io.ReadCloser, error) {
	if idx, ok := b.getFileLane(filename); ok {
		rc, err := fn(b.lanes[idx].backend)
		if err == nil {
			b.lanes[idx].markHealthy()
			return rc, nil
		}
		b.lanes[idx].markUnhealthy()
	}

	var firstErr error
	for idx, lane := range b.lanes {
		rc, err := fn(lane.backend)
		if err != nil {
			lane.markUnhealthy()
			if firstErr == nil {
				firstErr = fmt.Errorf("%s read failed: %w", lane.name, err)
			}
			continue
		}
		lane.markHealthy()
		b.setFileLane(filename, idx)
		return rc, nil
	}
	return nil, firstErr
}

func (b *MultiBackend) pickLane(filename string) int {
	healthy := b.healthyLaneIndexes()
	if len(healthy) == 0 {
		return int(hashString(filename) % uint32(len(b.lanes)))
	}
	return healthy[int(hashString(filename)%uint32(len(healthy)))]
}

func (b *MultiBackend) healthyLaneIndexes() []int {
	var healthy []int
	for idx, lane := range b.lanes {
		if lane.healthy() {
			healthy = append(healthy, idx)
		}
	}
	return healthy
}

func (b *MultiBackend) hasHealthyLane() bool {
	for _, lane := range b.lanes {
		if lane.healthy() {
			return true
		}
	}
	return false
}

func (b *MultiBackend) setFileLane(filename string, idx int) {
	b.fileLaneMu.Lock()
	if len(b.fileLane) > 10000 {
		b.fileLane = make(map[string]int)
	}
	b.fileLane[filename] = idx
	b.fileLaneMu.Unlock()
}

func (b *MultiBackend) getFileLane(filename string) (int, bool) {
	b.fileLaneMu.RLock()
	idx, ok := b.fileLane[filename]
	b.fileLaneMu.RUnlock()
	return idx, ok
}

func (b *MultiBackend) deleteFileLane(filename string) {
	b.fileLaneMu.Lock()
	delete(b.fileLane, filename)
	b.fileLaneMu.Unlock()
}

func (l *storageLane) healthy() bool {
	l.mu.RLock()
	unhealthyUntil := l.unhealthyUntil
	l.mu.RUnlock()
	return time.Now().After(unhealthyUntil)
}

func (l *storageLane) markUnhealthy() {
	l.mu.Lock()
	l.unhealthyUntil = time.Now().Add(30 * time.Second)
	l.mu.Unlock()
}

func (l *storageLane) markHealthy() {
	l.mu.Lock()
	l.unhealthyUntil = time.Time{}
	l.mu.Unlock()
}

func hashString(value string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(value))
	return h.Sum32()
}

func bytesReader(payload []byte) io.Reader {
	return bytes.NewReader(payload)
}
