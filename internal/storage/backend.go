package storage

import (
	"context"
)

// Backend defines the interface for our pluggable storage mechanism that acts as the
// covert transport layer.
type Backend interface {
	// Login performs any necessary authentication.
	Login(ctx context.Context) error

	// Upload writes a new file to the storage backend.
	// filename is typically of the format request-<session>-<seq>-<timestamp>.json
	Upload(ctx context.Context, filename string, data []byte) error

	// ListQuery searches the backend for files matching a specific prefix or criteria.
	// We use this to discover new request or response payloads.
	ListQuery(ctx context.Context, prefix string) ([]string, error)

	// Download reads the file content from the backend.
	Download(ctx context.Context, filename string) ([]byte, error)

	// Delete removes a file from the backend after it has been read or expired.
	Delete(ctx context.Context, filename string) error

	// CreateFolder creates a storage container (e.g. Google Drive folder) and returns its ID.
	CreateFolder(ctx context.Context, name string) (string, error)

	// FindFolder searches for an existing storage container by name and returns its ID.
	FindFolder(ctx context.Context, name string) (string, error)
}
