package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// LocalBackend implements Backend using the local filesystem.
type LocalBackend struct {
	baseDir string
}

// NewLocalBackend creates a new LocalBackend. It ensures the base directory exists.
func NewLocalBackend(baseDir string) (*LocalBackend, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base dir %s: %w", baseDir, err)
	}
	return &LocalBackend{baseDir: baseDir}, nil
}

func (b *LocalBackend) Login(ctx context.Context) error {
	// For local backend, no auth is needed. We just verify the directory exists.
	info, err := os.Stat(b.baseDir)
	if err != nil {
		return fmt.Errorf("local backend dir not found: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("local backend path %s is not a directory", b.baseDir)
	}
	return nil
}

func (b *LocalBackend) Upload(ctx context.Context, filename string, data []byte) error {
	path := filepath.Join(b.baseDir, filename)
	// Write to a temporary file first, then rename to avoid partial reads by the polling server
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename temp file: %w", err)
	}
	return nil
}

func (b *LocalBackend) ListQuery(ctx context.Context, prefix string) ([]string, error) {
	entries, err := os.ReadDir(b.baseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read dir: %w", err)
	}

	var results []string
	for _, entry := range entries {
		if entry.IsDir() || strings.HasSuffix(entry.Name(), ".tmp") {
			continue
		}
		if prefix == "" || strings.HasPrefix(entry.Name(), prefix) {
			results = append(results, entry.Name())
		}
	}
	return results, nil
}

func (b *LocalBackend) Download(ctx context.Context, filename string) ([]byte, error) {
	path := filepath.Join(b.baseDir, filename)
	file, err := os.Open(path)
	if err != nil {
		// Differentiate between generic errors and not found
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("file not found: %s", filename)
		}
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	return data, nil
}

func (b *LocalBackend) Delete(ctx context.Context, filename string) error {
	path := filepath.Join(b.baseDir, filename)
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete file: %w", err)
	}
	return nil
}

func (b *LocalBackend) CreateFolder(ctx context.Context, name string) (string, error) {
	path := filepath.Join(b.baseDir, name)
	if err := os.MkdirAll(path, 0755); err != nil {
		return "", err
	}
	return name, nil
}

func (b *LocalBackend) FindFolder(ctx context.Context, name string) (string, error) {
	path := filepath.Join(b.baseDir, name)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil // Not found
		}
		return "", err
	}
	if info.IsDir() {
		return name, nil
	}
	return "", nil
}
