#!/usr/bin/env bash
set -u

OUT_ROOT="${1:-diagnostics}"
STAMP="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="${OUT_ROOT}/flowdriver-client-${STAMP}"
ARCHIVE=""
CLIENT_LOG="${FLOWDRIVER_CLIENT_LOG:-client.log}"
CLIENT_METRICS_URL="${FLOWDRIVER_CLIENT_METRICS_URL:-http://127.0.0.1:18081/metrics}"
CLIENT_HEALTH_URL="${FLOWDRIVER_CLIENT_HEALTH_URL:-http://127.0.0.1:18081/healthz}"
SOCKS_PROXY="${FLOWDRIVER_SOCKS_PROXY:-socks5h://127.0.0.1:1080}"
CURL_MAX_TIME="${FLOWDRIVER_CURL_MAX_TIME:-60}"
RUN_CURLS="${FLOWDRIVER_RUN_CURLS:-0}"
TAIL_LINES="${FLOWDRIVER_TAIL_LINES:-1200}"
SAMPLE_SECONDS="${FLOWDRIVER_SAMPLE_SECONDS:-30}"
SAMPLE_INTERVAL="${FLOWDRIVER_SAMPLE_INTERVAL:-5}"
SERVER_SSH="${FLOWDRIVER_SERVER_SSH:-}"
SSH_PROXY_HOSTPORT="${FLOWDRIVER_SSH_PROXY_HOSTPORT:-127.0.0.1:1080}"

mkdir -p "$OUT_DIR"

finalize() {
  if [ -n "$ARCHIVE" ] && [ -f "$ARCHIVE" ]; then
    return
  fi

  if command -v zip >/dev/null 2>&1; then
    ARCHIVE="${OUT_DIR}.zip"
    (cd "$OUT_ROOT" && zip -qr "$(basename "$ARCHIVE")" "$(basename "$OUT_DIR")")
  else
    ARCHIVE="${OUT_DIR}.tar.gz"
    tar -czf "$ARCHIVE" -C "$OUT_ROOT" "$(basename "$OUT_DIR")"
  fi

  echo "Diagnostics written to: $OUT_DIR"
  echo "Archive: $ARCHIVE"
}
trap finalize EXIT
trap 'finalize; exit 130' INT TERM

run_capture() {
  local name="$1"
  shift
  echo "Collecting ${name}..."
  {
    echo "\$ $*"
    "$@"
  } > "${OUT_DIR}/${name}" 2>&1
}

run_server_ssh_capture() {
  local name="$1"
  local remote_cmd="$2"
  local out="${OUT_DIR}/${name}"
  local ssh_opts=(-o BatchMode=yes -o ConnectTimeout=5 -o ServerAliveInterval=5 -o ServerAliveCountMax=1)
  local ssh_proxy_opts=(-o BatchMode=yes -o ConnectTimeout=20 -o ServerAliveInterval=5 -o ServerAliveCountMax=1 -o "ProxyCommand=nc -x ${SSH_PROXY_HOSTPORT} -X 5 %h %p")

  {
    echo "Collecting ${name}..."
    echo "\$ ssh ${SERVER_SSH} ${remote_cmd}"
    echo "--- direct ssh attempt ---"
    if ssh "${ssh_opts[@]}" "$SERVER_SSH" "$remote_cmd"; then
      exit 0
    fi

    echo "--- direct ssh failed; trying through SOCKS5 proxy ${SSH_PROXY_HOSTPORT} ---"
    echo "\$ ssh -o ProxyCommand='nc -x ${SSH_PROXY_HOSTPORT} -X 5 %h %p' ${SERVER_SSH} ${remote_cmd}"
    ssh "${ssh_proxy_opts[@]}" "$SERVER_SSH" "$remote_cmd" || true
  } > "$out" 2>&1
}

write_note() {
  cat > "${OUT_DIR}/README.txt" <<EOF
FlowDriver client diagnostics
created_at=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

Safe to share with the developer:
- client_tail.log
- client_metrics_samples.jsonl
- curl_*.txt
- process.txt
- ports.txt
- server_metrics.txt / server_tail.log if SERVER_SSH was set

Not collected:
- credentials.json
- credentials.json.token
EOF
}

write_note

run_capture uname.txt uname -a
run_capture go_version.txt go version
run_capture process.txt ps aux
run_capture ports.txt lsof -nP -iTCP:1080 -iTCP:18081 -sTCP:LISTEN
run_capture health.txt curl -sS --max-time 5 "$CLIENT_HEALTH_URL"
run_capture metrics_initial.json curl -sS --max-time 5 "$CLIENT_METRICS_URL"

if [ -f "$CLIENT_LOG" ]; then
  echo "Collecting client_tail_initial.log..."
  tail -n "$TAIL_LINES" "$CLIENT_LOG" > "${OUT_DIR}/client_tail_initial.log"
else
  echo "Client log not found: $CLIENT_LOG" > "${OUT_DIR}/client_tail_initial.log"
fi

if [ -f client_config.json ]; then
  echo "Collecting client_config.redacted.json..."
  sed -E \
    -e 's/("client_secret"[[:space:]]*:[[:space:]]*")[^"]+/\1REDACTED/g' \
    -e 's/("refresh_token"[[:space:]]*:[[:space:]]*")[^"]+/\1REDACTED/g' \
    -e 's/("client_id"[[:space:]]*:[[:space:]]*")[^"]+/\1REDACTED/g' \
    client_config.json > "${OUT_DIR}/client_config.redacted.json"
fi

echo "Sampling client metrics for ${SAMPLE_SECONDS}s..."
END=$((SECONDS + SAMPLE_SECONDS))
while [ "$SECONDS" -lt "$END" ]; do
  METRICS="$(curl -sS --max-time 5 "$CLIENT_METRICS_URL" 2>> "${OUT_DIR}/client_metrics_errors.log" || true)"
  if [ -z "$METRICS" ]; then
    METRICS="null"
  fi
  printf '{"ts":"%s","metrics":%s}\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "$METRICS" >> "${OUT_DIR}/client_metrics_samples.jsonl"
  sleep "$SAMPLE_INTERVAL"
done

if [ "$RUN_CURLS" = "1" ]; then
  echo "Running curl benchmarks; each one can take up to ${CURL_MAX_TIME}s..."
  run_capture curl_google.txt curl -L --max-time "$CURL_MAX_TIME" -x "$SOCKS_PROXY" https://www.google.com -o /dev/null -w 'time_total=%{time_total} starttransfer=%{time_starttransfer} size=%{size_download} speed=%{speed_download}\n'
  run_capture curl_facebook.txt curl -L --max-time "$CURL_MAX_TIME" -x "$SOCKS_PROXY" https://www.facebook.com -o /dev/null -w 'time_total=%{time_total} starttransfer=%{time_starttransfer} size=%{size_download} speed=%{speed_download}\n'
  run_capture curl_youtube.txt curl -L --max-time "$CURL_MAX_TIME" -x "$SOCKS_PROXY" https://youtube.com -o /dev/null -w 'time_total=%{time_total} starttransfer=%{time_starttransfer} size=%{size_download} speed=%{speed_download}\n'
else
  echo "Skipping curl benchmarks. Set FLOWDRIVER_RUN_CURLS=1 to enable them."
fi

run_capture metrics_final.json curl -sS --max-time 5 "$CLIENT_METRICS_URL"

if [ -f "$CLIENT_LOG" ]; then
  echo "Collecting client_tail.log..."
  tail -n "$TAIL_LINES" "$CLIENT_LOG" > "${OUT_DIR}/client_tail.log"
else
  echo "Client log not found: $CLIENT_LOG" > "${OUT_DIR}/client_tail.log"
fi

if [ -n "$SERVER_SSH" ]; then
  run_server_ssh_capture server_metrics.txt 'curl -sS --max-time 5 http://127.0.0.1:18080/metrics'
  run_server_ssh_capture server_tail.log 'tail -n 1200 ~/flowdriver/server.log 2>/dev/null || true'
fi
