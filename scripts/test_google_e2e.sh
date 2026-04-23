#!/bin/bash
set -e
set -m

if [ ! -f "config.json" ]; then
    echo "ERROR: config.json not found! Please map config.json properly."
    exit 1
fi

GC_FILE=$(ls client_secret*.json 2>/dev/null | head -n 1)
if [ -z "$GC_FILE" ]; then
    echo "ERROR: Could not find any client_secret*.json or credentials.json file!"
    exit 1
fi

echo "Using Secret File: $GC_FILE"

echo "Building binaries..."
/opt/homebrew/bin/go build -o bin/client ./cmd/client
/opt/homebrew/bin/go build -o bin/server ./cmd/server

TOKEN_FILE="$GC_FILE.token"
if [ ! -f "$TOKEN_FILE" ]; then
    echo ""
    echo "=========================================================="
    echo "FIRST-TIME AUTHENTICATION REQUIRED"
    echo "You must authorize the application manually once."
    echo "=========================================================="
    ./bin/server -c config.json -gc "$GC_FILE" &
    TEMP_SERVER_PID=$!
    
    # Bring server to foreground to collect stdin
    fg %1 || true
    
    # We exit here after auth so the script doesn't continue with closed background processes
    echo ""
    echo "Auth finished! Please run ./scripts/test_google_e2e.sh again to start the automated E2E test."
    exit 0
fi

echo "Starting Server (Google Mode)..."
./bin/server -c config.json -gc "$GC_FILE" &
SERVER_PID=$!

echo "Starting Client (Google Mode)..."
./bin/client -c config.json -gc "$GC_FILE" &
CLIENT_PID=$!

function cleanup {
  echo "Cleaning up..."
  kill $SERVER_PID $CLIENT_PID 2>/dev/null || true
}
trap cleanup EXIT

echo "Waiting 5 seconds for initialization..."
sleep 5

echo "Testing curl through proxy to ifconfig.me..."
IP=$(curl -s -x socks5h://127.0.0.1:1080 http://ifconfig.me)

if [ -n "$IP" ]; then
  echo "SUCCESS! Extracted IP from ifconfig.me: $IP"
else
  echo "FAILED! Curl returned empty or error."
  exit 1
fi
