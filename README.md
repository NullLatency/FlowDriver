# Flow Driver 🌊

**Flow Driver** is a covert transport system designed to tunnel network traffic (SOCKS5) through common cloud storage platforms like Google Drive. It allows for reliable communication in restrictive environments by leveraging legitimate API traffic.

**Flow Driver** یک سیستم انتقال پنهان (Covert Transport) است که برای تونل کردن ترافیک شبکه (SOCKS5) از طریق پلتفرم‌های ذخیره‌سازی ابری رایج مانند گوگل درایو طراحی شده است. این ابزار با بهره‌گیری از ترافیک قانونی API، امکان ارتباط مطمئن در محیط‌های محدود شده را فراهم می‌کند.

---

## ⚠️ Disclaimer / سلب مسئولیت

**English**: This project is intended for personal usage and research purposes only. Please do not use it for illegal purposes, and do not use it in a production environment. The authors are not responsible for any misuse of this tool.

**فارسی**: این پروژه صرفاً برای استفاده شخصی و اهداف تحقیقاتی در نظر گرفته شده است. لطفاً از آن برای مقاصد غیرقانونی استفاده نکنید و در محیط‌های عملیاتی (Production) از آن استفاده نشود. نویسندگان هیچ مسئولیتی در قبال سوء استفاده از این ابزار ندارند.

---

## How it Works / نحوه عملکرد

### English
Flow Driver works by treating a cloud storage folder as a data queue:
1.  **Client**: Captures local SOCKS5 requests and bundles them into a compact **Binary Protocol**. These binary "packets" are uploaded to a specific Google Drive folder.
2.  **Server**: Continuously polls the Drive folder. When it finds a request from a client, it downloads it, opens a real TCP connection to the destination, and sends back the result as a response file.

### فارسی
نحوه عملکرد این ابزار به این صورت است که از یک پوشه در فضای ابری به عنوان صف داده‌ها استفاده می‌کند:
1.  **کلاینت**: درخواست‌های SOCKS5 محلی را دریافت کرده و آن‌ها را در قالب یک **پروتکل باینری** فشرده بسته‌بندی می‌کند. این بسته‌ها در یک پوشه خاص در گوگل درایو آپلود می‌شوند.
2.  **سرور**: به طور مداوم پوشه درایو را بررسی می‌کند. با یافتن درخواست جدید، آن را دانلود کرده، اتصال TCP واقعی را برقرار می‌کند و نتیجه را در قالب فایل‌های پاسخ به درایو بازمی‌گرداند.

---

## Setup & Installation / نصب و راه‌اندازی

This section is a complete client-to-server setup guide for the Google Drive backend.

### Prerequisites / پیش‌نیازها

- Go 1.25 or newer on the build machine.
- A Linux upstream server or VPS. The examples below assume Ubuntu and user `ubuntu`.
- A Google Cloud project with the Google Drive API enabled.
- An OAuth Desktop App credential downloaded as `credentials.json`.

Do not commit `credentials.json`, `credentials.json.token`, real config files, or diagnostics. They are ignored by `.gitignore`.

### 1. Create Google OAuth Credentials

1. Open Google Cloud Console and create or select a project.
2. Enable **Google Drive API**.
3. Go to **APIs & Services -> OAuth consent screen** and configure the app name, support email, and developer contact email.
4. If the app is in **Testing**, add the Google account you will use under **Test users**. Otherwise Google may return `Error 403: access_denied`.
5. Go to **APIs & Services -> Credentials -> Create Credentials -> OAuth client ID**.
6. Select **Desktop app**.
7. Download the JSON file and save it in the repository root as `credentials.json`.

If you want long-lived tokens for your own account, publish the OAuth app after the consent screen is complete. Apps left in Testing can require re-authorization.

### 2. Build Binaries

Build the local client for your machine:

```bash
go build -o bin/client ./cmd/client
```

Build the Linux server binary from macOS or another build host:

```bash
GOOS=linux GOARCH=amd64 go build -o bin/server-linux-amd64 ./cmd/server
go build -o bin/purge ./cmd/purge
```

### 3. Create Config Files

Start from the examples:

```bash
cp client_config.json.example client_config.json
cp server_config.json.example server_config.json
```

Recommended balanced client config:

```json
{
  "listen_addr": "127.0.0.1:1080",
  "storage_type": "google",
  "performance_profile": "balanced",
  "refresh_rate_ms": 200,
  "flush_rate_ms": 300,
  "idle_poll_max_ms": 2000,
  "idle_poll_step_ms": 500,
  "session_idle_timeout_sec": 25,
  "cleanup_file_max_age_sec": 60,
  "startup_stale_max_age_sec": 20,
  "storage_retry_max": 3,
  "storage_retry_base_ms": 300,
  "storage_op_timeout_sec": 45,
  "max_payload_bytes": 786432,
  "max_active_sessions": 0,
  "target_metrics_top_n": 10,
  "blocked_targets": [],
  "low_priority_targets": [],
  "session_wait_timeout_sec": 15,
  "backpressure_bytes": 4194304,
  "immediate_flush": false,
  "cold_start_burst_ms": 10000,
  "cold_start_poll_ms": 100,
  "metrics_log_sec": 30,
  "health_listen_addr": "127.0.0.1:18081",
  "transport": {
    "TargetIP": "216.239.38.120:443",
    "SNI": "google.com",
    "HostHeader": "www.googleapis.com",
    "InsecureSkipVerify": false
  },
  "google_lanes": []
}
```

Recommended balanced server config:

```json
{
  "storage_type": "google",
  "performance_profile": "balanced",
  "refresh_rate_ms": 200,
  "flush_rate_ms": 300,
  "idle_poll_max_ms": 2000,
  "idle_poll_step_ms": 500,
  "session_idle_timeout_sec": 60,
  "cleanup_file_max_age_sec": 60,
  "startup_stale_max_age_sec": 20,
  "storage_retry_max": 3,
  "storage_retry_base_ms": 300,
  "storage_op_timeout_sec": 45,
  "max_payload_bytes": 786432,
  "max_active_sessions": 20,
  "session_wait_timeout_sec": 15,
  "backpressure_bytes": 4194304,
  "immediate_flush": false,
  "cold_start_burst_ms": 10000,
  "cold_start_poll_ms": 100,
  "metrics_log_sec": 30,
  "health_listen_addr": "127.0.0.1:18080",
  "google_lanes": []
}
```

Leave `google_folder_id` empty for the first local client run. The client will find or create a Google Drive folder named `Flow-Data` and write the folder ID back to `client_config.json`. Copy that same folder ID into `server_config.json`.

### 4. First Local OAuth Login

Run the client once locally:

```bash
./bin/client -c client_config.json -gc credentials.json
```

The client prints a Google OAuth URL. Open it in a browser, approve access, then copy the full redirected `http://localhost/?code=...` URL back into the terminal. It is fine if the browser page itself cannot connect.

After success, the client writes:

```text
credentials.json.token
```

Keep both `credentials.json` and `credentials.json.token` private.

### 5. Deploy the Server

Create the remote directory and copy the server files:

```bash
ssh ubuntu@YOUR_SERVER_IP 'mkdir -p ~/flowdriver'
scp bin/server-linux-amd64 server_config.json credentials.json credentials.json.token ubuntu@YOUR_SERVER_IP:~/flowdriver/
```

On the server:

```bash
ssh ubuntu@YOUR_SERVER_IP
cd ~/flowdriver
mv server-linux-amd64 server
chmod +x server
./server -c server_config.json -gc credentials.json
```

The server is running correctly when it prints `Starting Flow Server...` and keeps waiting. It only logs more when client traffic arrives.

### 6. Run the Server with systemd

For an always-on Ubuntu service:

```bash
scp scripts/flowdriver-server.service ubuntu@YOUR_SERVER_IP:/tmp/
ssh ubuntu@YOUR_SERVER_IP
sudo mv /tmp/flowdriver-server.service /etc/systemd/system/flowdriver-server.service
sudo systemctl daemon-reload
sudo systemctl enable --now flowdriver-server
sudo systemctl status flowdriver-server --no-pager
```

Useful service commands:

```bash
sudo systemctl restart flowdriver-server
sudo systemctl stop flowdriver-server
sudo systemctl status flowdriver-server --no-pager
journalctl -u flowdriver-server -f
curl http://127.0.0.1:18080/metrics
```

To update the server binary later:

```bash
GOOS=linux GOARCH=amd64 go build -o bin/server-linux-amd64 ./cmd/server
scp bin/server-linux-amd64 server_config.json ubuntu@YOUR_SERVER_IP:~/flowdriver/
ssh ubuntu@YOUR_SERVER_IP 'cd ~/flowdriver && mv server-linux-amd64 server && chmod +x server && sudo systemctl restart flowdriver-server'
```

### 7. Run the Client

Run the client locally and keep it open:

```bash
./bin/client -c client_config.json -gc credentials.json
```

For logs that are easier to inspect:

```bash
./bin/client -c client_config.json -gc credentials.json 2>&1 | tee client.log
```

The local SOCKS5 proxy listens on:

```text
127.0.0.1:1080
```

Test with curl:

```bash
curl -L -x socks5h://127.0.0.1:1080 https://www.google.com -o /dev/null -w 'time_total=%{time_total} starttransfer=%{time_starttransfer} size=%{size_download}\n'
curl -L -x socks5h://127.0.0.1:1080 https://www.facebook.com -o /dev/null -w 'time_total=%{time_total} starttransfer=%{time_starttransfer} size=%{size_download}\n'
```

Configure your browser to use SOCKS5 `127.0.0.1:1080`. For browser workloads, expect initial page load latency; once streams start, throughput should be steadier.

### 8. Diagnostics

Client health and metrics:

```bash
curl http://127.0.0.1:18081/healthz
curl http://127.0.0.1:18081/metrics
```

Collect a 5-minute client diagnostic bundle:

```bash
FLOWDRIVER_SERVER_SSH=ubuntu@YOUR_SERVER_IP \
FLOWDRIVER_SAMPLE_SECONDS=300 \
FLOWDRIVER_SAMPLE_INTERVAL=5 \
FLOWDRIVER_TAIL_LINES=3000 \
./scripts/collect_client_diagnostics.sh
```

The script writes a timestamped directory and archive under `diagnostics/`. It does not collect `credentials.json` or `.token` files.

Key metrics to watch:

- `upload_errors`, `download_errors`, `list_errors`, `delete_errors`: should stay at or near zero.
- `avg_first_upload_ms`: local queue-to-Drive upload latency.
- `avg_first_server_seen_ms`: how long it takes the server to see a new client request file.
- `avg_first_response_ms`: time until the client receives first response bytes.
- `top_targets`: busiest destination hosts with session counts and first-response timing.
- `poll_files_stale`: old transport leftovers ignored and deleted.
- `max_file_age_ms`: large values can indicate backlog or old files.

### Runtime Tuning / تنظیمات اجرا

`performance_profile` can be set to:

- `fast`: lower startup latency, higher Google API usage.
- `balanced`: recommended default for normal browsing and downloads.
- `quota-saver`: lower API usage, higher startup latency.

Important options:

- `refresh_rate_ms`: how often each side polls for incoming files while active.
- `flush_rate_ms`: how often buffered data is uploaded.
- `idle_poll_max_ms`: server-side maximum polling delay while idle.
- `session_idle_timeout_sec`: inactive connection timeout.
- `cleanup_file_max_age_sec`: deletes stale transport files.
- `startup_stale_max_age_sec`: ignores and deletes old transport leftovers before they can pollute cold starts.
- `storage_retry_max` and `storage_retry_base_ms`: retry policy for transient Google API failures.
- `storage_op_timeout_sec`: fail-fast timeout for individual Google Drive operations.
- `max_payload_bytes`: maximum per-session payload size written into one transport file.
- `max_active_sessions`: cap for concurrent sessions. `0` means unlimited.
- `target_metrics_top_n`: number of destination hosts exposed under `/metrics` as `top_targets`.
- `blocked_targets`: optional host globs to reject before tunneling, for example `["*.doubleclick.net"]`.
- `low_priority_targets`: optional host globs to tunnel without extending the cold-start burst, for noisy browser background services.
- `session_wait_timeout_sec`: how long new SOCKS sessions wait for capacity.
- `backpressure_bytes`: per-session buffer limit before application writes wait.
- `immediate_flush`: uploads new data promptly instead of waiting for the next flush tick. Keep this disabled for browser/video/download workloads because Google Drive generally performs better with batched files.
- `cold_start_burst_ms`: temporary fast-polling window after a new session starts.
- `cold_start_poll_ms`: polling interval used during the cold-start burst.
- `metrics_log_sec`: periodic operational metrics log interval.
- `health_listen_addr`: optional local HTTP endpoint for `/healthz` and `/metrics`.

Target patterns are lowercase host globs with optional ports. Examples: `*.doubleclick.net`, `mtalk.google.com:*`, `*:5228`. Prefer `low_priority_targets` before `blocked_targets`; blocking can break page assets, while low priority only prevents background connections from consuming cold-start acceleration.

For higher throughput or resilience, configure multiple Google Drive lanes on both client and server:

```json
{
  "google_lanes": [
    {
      "credentials_path": "credentials.json",
      "google_folder_id": "LANE_1_FOLDER_ID"
    },
    {
      "credentials_path": "credentials-lane2.json",
      "google_folder_id": "LANE_2_FOLDER_ID"
    }
  ]
}
```

Each lane can use a separate Google account or folder. Uploads are distributed across healthy lanes, and a lane with transient failures is temporarily avoided.

### Troubleshooting

- `Error 403: access_denied`: add your Google account as an OAuth test user or publish the app.
- Browser redirects to `localhost` and fails to load: this is expected. Copy the full URL from the address bar back into the client prompt.
- Server appears stuck after `Starting Flow Server...`: that is normal while idle. Generate client traffic and watch `journalctl -u flowdriver-server -f`.
- `scp: stat local ... no such file`: run commands from the repository root, or use full paths.
- Client metrics show many `context deadline exceeded` errors: reduce browser load, restart both sides, and check Google Drive/API reachability.
- Large `poll_files_stale` or `max_file_age_ms`: old request/response files are being cleaned up; restart both sides and retest after the folder drains.
