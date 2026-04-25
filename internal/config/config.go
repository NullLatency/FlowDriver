package config

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/NullLatency/flow-driver/internal/httpclient"
)

// AppConfig defines the application-level overarching configuration.
type AppConfig struct {
	// PerformanceProfile applies conservative runtime defaults.
	// Valid values: "fast", "balanced", "quota-saver".
	PerformanceProfile string `json:"performance_profile,omitempty"`

	// ListenAddr is the SOCKS5 listening address for the client. E.g., "127.0.0.1:1080"
	ListenAddr string `json:"listen_addr,omitempty"`

	// ClientID identifies this client node to allow multi-tenant folder sharing.
	ClientID string `json:"client_id,omitempty"`

	// StorageType defines the backend ("local" or "google").
	StorageType string `json:"storage_type"`

	// LocalDir is the path used when StorageType is "local".
	LocalDir string `json:"local_dir,omitempty"`

	// GoogleFolderID is the Drive Folder ID when StorageType is "google".
	GoogleFolderID string `json:"google_folder_id,omitempty"`

	// GoogleLanes optionally configures multiple Google Drive lanes.
	GoogleLanes []GoogleLaneConfig `json:"google_lanes,omitempty"`

	// RefreshRateMs is the polling (RX) interval in milliseconds for the engine.
	RefreshRateMs int `json:"refresh_rate_ms,omitempty"`

	// FlushRateMs is the gathering (TX) interval in milliseconds for the engine.
	FlushRateMs int `json:"flush_rate_ms,omitempty"`

	// IdlePollMaxMs caps the server-side adaptive polling delay when no sessions are active.
	IdlePollMaxMs int `json:"idle_poll_max_ms,omitempty"`

	// IdlePollStepMs is the backoff step for server-side adaptive polling.
	IdlePollStepMs int `json:"idle_poll_step_ms,omitempty"`

	// SessionIdleTimeoutSec closes inactive sessions after this many seconds.
	SessionIdleTimeoutSec int `json:"session_idle_timeout_sec,omitempty"`

	// CleanupFileMaxAgeSec deletes stale transport files after this many seconds.
	CleanupFileMaxAgeSec int `json:"cleanup_file_max_age_sec,omitempty"`

	// StorageRetryMax is the maximum number of retry attempts for transient storage errors.
	StorageRetryMax int `json:"storage_retry_max,omitempty"`

	// StorageRetryBaseMs is the initial retry backoff delay in milliseconds.
	StorageRetryBaseMs int `json:"storage_retry_base_ms,omitempty"`

	// StorageOpTimeoutSec caps each storage operation before it is treated as failed.
	StorageOpTimeoutSec int `json:"storage_op_timeout_sec,omitempty"`

	// MaxPayloadBytes caps each session payload written into a single transport file.
	MaxPayloadBytes int `json:"max_payload_bytes,omitempty"`

	// MaxActiveSessions caps concurrent SOCKS sessions on the client side.
	MaxActiveSessions int `json:"max_active_sessions,omitempty"`

	// SessionWaitTimeoutSec is how long a new SOCKS connection waits for capacity.
	SessionWaitTimeoutSec int `json:"session_wait_timeout_sec,omitempty"`

	// BackpressureBytes blocks application writes when a session buffer grows past this size.
	BackpressureBytes int `json:"backpressure_bytes,omitempty"`

	// ImmediateFlush uploads new data promptly instead of waiting for the next flush tick.
	// Keep this disabled for browser workloads; Google Drive performs better with batching.
	ImmediateFlush bool `json:"immediate_flush,omitempty"`

	// MetricsLogSec logs throughput and storage operation counters every N seconds.
	MetricsLogSec int `json:"metrics_log_sec,omitempty"`

	// HealthListenAddr exposes local health and metrics HTTP endpoints when set.
	HealthListenAddr string `json:"health_listen_addr,omitempty"`

	// Transport configures the dpi-evasion layer.
	Transport httpclient.TransportConfig `json:"transport,omitempty"`
}

type GoogleLaneConfig struct {
	CredentialsPath string                     `json:"credentials_path,omitempty"`
	GoogleFolderID  string                     `json:"google_folder_id,omitempty"`
	Transport       httpclient.TransportConfig `json:"transport,omitempty"`
}

// ApplyProfile fills unset tuning fields from a named profile while preserving
// explicitly configured values.
func (c *AppConfig) ApplyProfile() {
	switch c.PerformanceProfile {
	case "fast":
		setDefault(&c.RefreshRateMs, 100)
		setDefault(&c.FlushRateMs, 100)
		setDefault(&c.IdlePollMaxMs, 1000)
		setDefault(&c.IdlePollStepMs, 200)
		setDefault(&c.SessionIdleTimeoutSec, 60)
		setDefault(&c.CleanupFileMaxAgeSec, 45)
		setDefault(&c.StorageRetryMax, 4)
		setDefault(&c.StorageRetryBaseMs, 200)
		setDefault(&c.StorageOpTimeoutSec, 45)
		setDefault(&c.MaxPayloadBytes, 512*1024)
		setDefault(&c.MaxActiveSessions, 24)
		setDefault(&c.SessionWaitTimeoutSec, 12)
		setDefault(&c.BackpressureBytes, 4*1024*1024)
		setDefault(&c.MetricsLogSec, 30)
	case "quota-saver":
		setDefault(&c.RefreshRateMs, 750)
		setDefault(&c.FlushRateMs, 500)
		setDefault(&c.IdlePollMaxMs, 5000)
		setDefault(&c.IdlePollStepMs, 750)
		setDefault(&c.SessionIdleTimeoutSec, 45)
		setDefault(&c.CleanupFileMaxAgeSec, 30)
		setDefault(&c.StorageRetryMax, 3)
		setDefault(&c.StorageRetryBaseMs, 500)
		setDefault(&c.StorageOpTimeoutSec, 45)
		setDefault(&c.MaxPayloadBytes, 1024*1024)
		setDefault(&c.MaxActiveSessions, 16)
		setDefault(&c.SessionWaitTimeoutSec, 10)
		setDefault(&c.BackpressureBytes, 4*1024*1024)
		setDefault(&c.MetricsLogSec, 60)
	default:
		setDefault(&c.RefreshRateMs, 200)
		setDefault(&c.FlushRateMs, 300)
		setDefault(&c.IdlePollMaxMs, 2000)
		setDefault(&c.IdlePollStepMs, 500)
		setDefault(&c.SessionIdleTimeoutSec, 60)
		setDefault(&c.CleanupFileMaxAgeSec, 30)
		setDefault(&c.StorageRetryMax, 3)
		setDefault(&c.StorageRetryBaseMs, 300)
		setDefault(&c.StorageOpTimeoutSec, 45)
		setDefault(&c.MaxPayloadBytes, 768*1024)
		setDefault(&c.MaxActiveSessions, 24)
		setDefault(&c.SessionWaitTimeoutSec, 12)
		setDefault(&c.BackpressureBytes, 4*1024*1024)
		setDefault(&c.MetricsLogSec, 30)
	}
}

func setDefault(target *int, value int) {
	if *target <= 0 {
		*target = value
	}
}

// Save writes the config back to a JSON file.
func (c *AppConfig) Save(path string) error {
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0644)
}

// Load reads and parses a JSON config file.
func Load(path string) (*AppConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	var cfg AppConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config JSON: %w", err)
	}

	return &cfg, nil
}
