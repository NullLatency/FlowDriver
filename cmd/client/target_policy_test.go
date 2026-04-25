package main

import "testing"

func TestTargetPolicyMatchesHostAndPortPatterns(t *testing.T) {
	policy := newTargetPolicy(
		[]string{"*.doubleclick.net", "*:5228"},
		[]string{"mtalk.google.com:*", "optimizationguide-pa.googleapis.com"},
	)

	tests := []struct {
		name        string
		addr        string
		blocked     bool
		lowPriority bool
	}{
		{
			name:    "blocked host glob",
			addr:    "googleads.g.doubleclick.net:443",
			blocked: true,
		},
		{
			name:    "blocked port glob",
			addr:    "push.example.com:5228",
			blocked: true,
		},
		{
			name:        "low priority host with any port",
			addr:        "mtalk.google.com:443",
			lowPriority: true,
		},
		{
			name:        "low priority host without explicit port pattern",
			addr:        "optimizationguide-pa.googleapis.com:443",
			lowPriority: true,
		},
		{
			name: "unmatched target",
			addr: "www.youtube.com:443",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, port, _ := splitTarget(tt.addr)
			if got := policy.blocked(host, port, tt.addr); got != tt.blocked {
				t.Fatalf("blocked=%v, want %v", got, tt.blocked)
			}
			if got := policy.lowPriority(host, port, tt.addr); got != tt.lowPriority {
				t.Fatalf("lowPriority=%v, want %v", got, tt.lowPriority)
			}
		})
	}
}
