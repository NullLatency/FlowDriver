package httpclient

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

// TransportConfig defines the rules for bypassing censorship
// to reach the underlying APIs.
type TransportConfig struct {
	// TargetIP is the IP address to dial instead of the DNS resolved IP.
	// E.g., "216.239.38.120:443"
	TargetIP string

	// SNI is the Server Name Indication to use during the TLS handshake.
	// E.g., "google.com"
	SNI string

	// HostHeader is the Host header to inject in HTTP requests.
	// E.g., "www.googleapis.com"
	HostHeader string

	// InsecureSkipVerify allows bypassing certificate validation if necessary.
	InsecureSkipVerify bool
}

// hostRewriteTransport is an http.RoundTripper that rewrites the Host header.
type hostRewriteTransport struct {
	Transport  http.RoundTripper
	HostHeader string
}

func (t *hostRewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.HostHeader != "" {
		req.Host = t.HostHeader
	}
	return t.Transport.RoundTrip(req)
}

// NewCustomClient creates an http.Client configured to bypass DNS
// and manipulate TLS/HTTP headers as specified in the config.
func NewCustomClient(cfg TransportConfig) *http.Client {
	dialer := &net.Dialer{
		Timeout:   8 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	transport := &http.Transport{
		// Force dialing the TargetIP instead of resolving the request URL's host.
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if cfg.TargetIP != "" {
				return dialer.DialContext(ctx, "tcp", cfg.TargetIP)
			}
			return dialer.DialContext(ctx, network, addr)
		},
		TLSClientConfig: &tls.Config{
			ServerName:         cfg.SNI,
			InsecureSkipVerify: cfg.InsecureSkipVerify,
		},
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   20,
		ResponseHeaderTimeout: 10 * time.Second,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   8 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	var rt http.RoundTripper = transport
	if cfg.HostHeader != "" {
		rt = &hostRewriteTransport{
			Transport:  transport,
			HostHeader: cfg.HostHeader,
		}
	}

	return &http.Client{
		Transport: rt,
		Timeout:   15 * time.Second,
	}
}
