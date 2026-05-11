package transport

import (
	"io"
	"net"
	"sync"
	"time"
)

// VirtualConn acts as a bridge fulfilling the net.Conn interface for use by standard
// SOCKS5 libraries, but routing data covertly through the Google Drive array Session.
type VirtualConn struct {
	session *Session
	engine  *Engine
	readBuf []byte
	mu      sync.Mutex
}

func NewVirtualConn(s *Session, e *Engine) *VirtualConn {
	return &VirtualConn{
		session: s,
		engine:  e,
	}
}

func (v *VirtualConn) Read(b []byte) (n int, err error) {
	for {
		v.mu.Lock()
		if len(v.readBuf) > 0 {
			n = copy(b, v.readBuf)
			v.readBuf = v.readBuf[n:]
			v.mu.Unlock()
			return n, nil
		}
		v.mu.Unlock()

		data, ok := <-v.session.RxChan
		if !ok {
			return 0, io.EOF
		}

		if len(data) > 0 {
			v.mu.Lock()
			n = copy(b, data)
			if n < len(data) {
				// Save remainder for subsequent reads
				v.readBuf = data[n:]
			}
			v.mu.Unlock()
			return n, nil
		}

		// Received 0-byte packet (e.g. heartbeat/close initialization).
		// We loop to read again so we don't violate io.Reader interface.
		v.session.mu.Lock()
		closed := v.session.closed
		v.session.mu.Unlock()

		if closed {
			return 0, io.EOF
		}
	}
}

func (v *VirtualConn) Write(b []byte) (n int, err error) {
	if len(b) > 0 {
		firstPacket := v.session.EnqueueTx(b)
		if firstPacket {
			v.engine.TriggerWarmPoll()
			v.engine.ForceFlush()
		} else {
			v.engine.RequestFlush()
		}
	}
	return len(b), nil
}

func (v *VirtualConn) Close() error {
	v.session.mu.Lock()
	v.session.closed = true
	v.session.txCond.Broadcast() // Wake up any writers blocked on backpressure
	v.session.mu.Unlock()
	v.engine.ForceFlush()

	// A closed connection no longer accepts writes efficiently
	// Next periodic engine flush will securely remove context
	return nil
}

func (v *VirtualConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 65535}
}
func (v *VirtualConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 65535}
}
func (v *VirtualConn) SetDeadline(t time.Time) error      { return nil }
func (v *VirtualConn) SetReadDeadline(t time.Time) error  { return nil }
func (v *VirtualConn) SetWriteDeadline(t time.Time) error { return nil }
