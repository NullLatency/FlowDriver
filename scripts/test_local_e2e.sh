#!/bin/bash
set -e

echo "Building binaries..."
/opt/homebrew/bin/go build -o bin/client ./cmd/client
/opt/homebrew/bin/go build -o bin/server ./cmd/server

# Create a temporary config for the test
TMP_CONFIG=$(mktemp)
cat <<EOF > "$TMP_CONFIG"
{
  "listen_addr": "127.0.0.1:10800",
  "storage_type": "local",
  "local_dir": "/tmp/flow-e2e-test",
  "refresh_rate_ms": 100,
  "flush_rate_ms": 100
}
EOF

# Ensure clean state
rm -rf /tmp/flow-e2e-test
mkdir -p /tmp/flow-e2e-test

echo "Starting Server..."
./bin/server -c "$TMP_CONFIG" > server.log 2>&1 &
SERVER_PID=$!

echo "Starting Client..."
./bin/client -c "$TMP_CONFIG" > client.log 2>&1 &
CLIENT_PID=$!

function cleanup {
  echo "Cleaning up..."
  kill $SERVER_PID $CLIENT_PID 2>/dev/null || true
  rm -rf /tmp/flow-e2e-test "$TMP_CONFIG"
}
trap cleanup EXIT

# Wait for listeners to start and initial polling to settle
sleep 5

echo "Testing curl through proxy to ifconfig.me..."
# Increased timeout for curl to handle the file-polling latency
IP=$(curl -s --connect-timeout 15 -x socks5h://127.0.0.1:10800 http://ifconfig.me)

if [ -n "$IP" ]; then
  echo "SUCCESS! Extracted IP: $IP"
else
  echo "FAILED! Curl returned empty or error. Logs follow:"
  echo "--- SERVER LOG ---"
  cat server.log
  echo "--- CLIENT LOG ---"
  cat client.log
  exit 1
fi
