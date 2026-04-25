package config

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/NullLatency/flow-driver/internal/httpclient"
)

const (
	StorageGoogle = "google"
	StorageLocal  = "local"

	DefaultListenAddr      = "127.0.0.1:1080"
	DefaultIdleTimeoutSec  = 60
	DefaultStaleFileTTLSec = 300

	MinRefreshRateMs = 100
	MinFlushRateMs   = 50
)

// AppConfig defines the application-level overarching configuration.
type AppConfig struct {
	// ListenAddr is the SOCKS5 listening address for the client.
	// Example: "127.0.0.1:1080"
	ListenAddr string `json:"listen_addr,omitempty"`

	// ClientID identifies this client node to allow multi-client folder sharing.
	ClientID string `json:"client_id,omitempty"`

	// StorageType defines the backend: "local" or "google".
	StorageType string `json:"storage_type"`

	// LocalDir is the path used when StorageType is "local".
	LocalDir string `json:"local_dir,omitempty"`

	// GoogleFolderID is the Drive Folder ID when StorageType is "google".
	GoogleFolderID string `json:"google_folder_id,omitempty"`

	// RefreshRateMs is the polling RX interval in milliseconds.
	RefreshRateMs int `json:"refresh_rate_ms,omitempty"`

	// FlushRateMs is the TX flush interval in milliseconds.
	FlushRateMs int `json:"flush_rate_ms,omitempty"`

	// IdleTimeoutSec controls when an inactive session is closed.
	// Default: 60 seconds.
	IdleTimeoutSec int `json:"idle_timeout_sec,omitempty"`

	// StaleFileTTLSec controls when old transport files are considered stale.
	// Default: 300 seconds.
	StaleFileTTLSec int `json:"stale_file_ttl_sec,omitempty"`

	// Transport configures the HTTP client transport.
	Transport httpclient.TransportConfig `json:"transport,omitempty"`
}

func (c *AppConfig) Normalize() {
	c.StorageType = strings.ToLower(strings.TrimSpace(c.StorageType))

	if c.ListenAddr == "" {
		c.ListenAddr = DefaultListenAddr
	}

	if c.IdleTimeoutSec == 0 {
		c.IdleTimeoutSec = DefaultIdleTimeoutSec
	}

	if c.StaleFileTTLSec == 0 {
		c.StaleFileTTLSec = DefaultStaleFileTTLSec
	}
}

func (c *AppConfig) Validate() error {
	c.Normalize()

	switch c.StorageType {
	case StorageGoogle:
		// google_folder_id may be empty because the app can auto-create it.
	case StorageLocal:
		if strings.TrimSpace(c.LocalDir) == "" {
			return fmt.Errorf("local_dir is required when storage_type is %q", StorageLocal)
		}
	default:
		return fmt.Errorf("storage_type must be %q or %q, got %q", StorageGoogle, StorageLocal, c.StorageType)
	}

	host, port, err := net.SplitHostPort(c.ListenAddr)
	if err != nil {
		return fmt.Errorf("invalid listen_addr %q: %w", c.ListenAddr, err)
	}
	if strings.TrimSpace(port) == "" {
		return fmt.Errorf("listen_addr %q is missing a port", c.ListenAddr)
	}

	ip := net.ParseIP(host)
	if host == "" || host == "::" || host == "0.0.0.0" || (ip != nil && ip.IsUnspecified()) {
		return fmt.Errorf("listen_addr must not bind to all interfaces by default; use 127.0.0.1:<port>")
	}

	if c.RefreshRateMs > 0 && c.RefreshRateMs < MinRefreshRateMs {
		return fmt.Errorf("refresh_rate_ms must be >= %dms", MinRefreshRateMs)
	}

	if c.FlushRateMs > 0 && c.FlushRateMs < MinFlushRateMs {
		return fmt.Errorf("flush_rate_ms must be >= %dms", MinFlushRateMs)
	}

	if c.IdleTimeoutSec < 0 {
		return fmt.Errorf("idle_timeout_sec must be >= 0")
	}

	if c.StaleFileTTLSec < 0 {
		return fmt.Errorf("stale_file_ttl_sec must be >= 0")
	}

	return nil
}

// Save writes the config back to a JSON file.
func (c *AppConfig) Save(path string) error {
	c.Normalize()

	b, err := json.MarshalIndent(c, "", " ")
	if err != nil {
		return err
	}

	// 0600 is safer because config may contain folder IDs and transport settings.
	return os.WriteFile(path, b, 0600)
}

// Load reads, parses, normalizes, and validates a JSON config file.
func Load(path string) (*AppConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	var cfg AppConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config JSON: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config %s: %w", path, err)
	}

	return &cfg, nil
}
