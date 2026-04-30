# Flow Driver 🌊

**Flow Driver** is a covert transport system designed to tunnel network traffic (SOCKS5) through common cloud storage platforms like Google Drive. It allows for reliable communication in restrictive environments by leveraging legitimate API traffic.

**Flow Driver** یک سیستم انتقال پنهان (Covert Transport) است که برای تونل کردن ترافیک شبکه (SOCKS5) از طریق پلتفرم‌های ذخیره‌سازی ابری رایج مانند گوگل درایو طراحی شده است. این ابزار با بهره‌گیری از ترافیک قانونی API، امکان ارتباط مطمئن در محیط‌های محدود شده را فراهم می‌کند.
v
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

### Prerequisites / پیش‌نیازها
- **Go** (1.25 or higher)
- **Google Drive API Credentials**: You need a `credentials.json` (OAuth2) file.
- **Shared Folder (Auto)**: If you leave `google_folder_id` empty, the tool will automatically create a folder named **"Flow-Data"** and save its ID to your config!

### 1. Obtain Credentials / دریافت فایل اعتبارنامه
To get your `credentials.json`, follow the instructions on the [Google Drive API Go Quickstart](https://developers.google.com/workspace/drive/api/quickstart/go) or follow these steps:

برای دریافت فایل `credentials.json` می‌توانید طبق دستورالعمل‌های موجود در [شروع سریع Google Drive API برای Go](https://developers.google.com/workspace/drive/api/quickstart/go) عمل کنید یا مراحل زیر را انجام دهید:

**English:**
1.  **Enable the API**: Go to the [Google Cloud Console](https://console.cloud.google.com/), create a project, and enable the **Google Drive API**.
2.  **Configure Consent Screen**: Go to "APIs & Services" > "OAuth consent screen." Fill in the app name and user support email (Branding).
3.  **Create Credentials**: Go to "Credentials" > "Create Credentials" > **OAuth client ID**. Select **Desktop App** as the application type.
4.  **Download JSON**: Download the client secret file and rename it to `credentials.json`.
5.  **Publish App (Optional but Recommended)**: If your app status is "Testing," your token will expire every 7 days. Go to the OAuth consent screen and click "Publish App" to make the authorization permanent for your account.

**فارسی:**
1.  **فعال‌سازی API**: به [کنسول گوگل کلاود](https://console.cloud.google.com/) بروید، یک پروژه بسازید و **Google Drive API** را فعال کنید.
2.  **تنظیم صفحه رضایت**: به بخش "APIs & Services" > "OAuth consent screen" بروید. نام برنامه و ایمیل پشتیبانی را وارد کنید (بخش Branding).
3.  **ساخت اعتبارنامه**: به بخش "Credentials" > "Create Credentials" > **OAuth client ID** بروید. نوع برنامه را **Desktop App** انتخاب کنید.
4.  **دانلود فایل**: فایل کلاینت سکرت را دانلود کرده و نام آن را به `credentials.json` تغییر دهید.
5.  **انتشار برنامه (پیشنهادی)**: اگر وضعیت برنامه روی "Testing" باشد، توکن شما هر ۷ روز منقضی می‌شود. در صفحه OAuth consent screen بر روی "Publish App" کلیک کنید تا دسترسی برای اکانت شما دائمی شود.

### 2. Build Binaries / ساخت فایل‌های اجرایی
```bash
go build -o bin/client ./cmd/client
go build -o bin/server ./cmd/server
```

### 2. Configuration / پیکربندی

Create your `config.json` based on the provided examples:

**Client Side (`client_config.json`):**
```json
{
  "listen_addr": "127.0.0.1:1080",
  "storage_type": "google",
  "google_folder_id": "YOUR_FOLDER_ID",
  "refresh_rate_ms": 100,
  "flush_rate_ms": 300,
  "transport": {
    "TargetIP": "216.239.38.120:443",
    "SNI": "google.com",
    "HostHeader": "www.googleapis.com"
  }
}
```
---

## Performance & Quotas / عملکرد و سهمیه‌ها

### English
**Important**: Google Drive has strict API rate limits (quotas). 
- Using very low values (e.g., `refresh_rate_ms: 100`) will consume your API quota very quickly.
- To avoid connections being limited or blocked, it is recommended to keep these values above **100ms** at all times.
- For heavy usage or multiple concurrent users, you should set these to **200ms or higher**.

### فارسی
**نکته مهم**: گوگل درایو محدودیت‌های سفت‌وسختی برای تعداد درخواست‌های API (Quota) دارد.
- استفاده از مقادیر بسیار پایین (مثلاً `100ms`) باعث می‌شود سهمیه API شما به سرعت تمام شود.
- برای جلوگیری از محدود شدن یا قطع شدن اتصال، توصیه می‌شود این مقادیر همیشه بالای **100ms** باشند.
- برای استفاده‌های سنگین یا زمانی که چندین کاربر به صورت هم‌زمان متصل هستند، بهتر است این مقادیر را روی **200ms یا بالاتر** تنظیم کنید.

**Server Side (`server_config.json`):**
```json
{
  "storage_type": "google",
  "google_folder_id": "YOUR_FOLDER_ID",
  "refresh_rate_ms": 100,
  "flush_rate_ms": 300
}
```

### 3. Run / اجرا

**Server:**
```bash
./bin/server -c server_config.json -gc credentials.json
```

**Client:**
```bash
./bin/client -c client_config.json -gc credentials.json
```

---

## Usage & Authentication / نحوه استفاده و احراز هویت

### 1. First-Time Authentication / احراز هویت اولیه
The project uses OAuth2 "3-legged" flow. You only need to do this once on your local machine:

**English:**
1.  Run the client: `./bin/client -c client_config.json -gc credentials.json`
2.  A link will appear in your terminal. **Copy and open it** in your web browser.
3.  Log in to your Google account and grant permissions.
4.  You will be redirected to an address starting with `http://localhost` (it's okay if the page doesn't load).
5.  **Copy the entire URL** from your browser's address bar and paste it back into your terminal.
6.  The program will create a `.token` file next to your `credentials.json`. Authorization is now complete.

**فارسی:**
1. کلاینت را اجرا کنید: `./bin/client -c client_config.json -gc credentials.json`
2. یک لینک در ترمینال ظاهر می‌شود. آن را کپی کرده و در مرورگر خود باز کنید.
3. وارد اکانت گوگل خود شوید و دسترسی‌های لازم را تایید کنید.
4. شما به آدرسی که با `http://localhost` شروع می‌شود هدایت می‌شوید (اشکالی ندارد اگر صفحه باز نشود).
5. **کل آدرس URL** را از نوار آدرس مرورگر کپی کرده و در ترمینال پیست کنید.
6. برنامه یک فایل با پسوند `.token` در کنار `credentials.json` شما می‌سازد. احراز هویت تمام شد.

### 2. Deploying to Server / استقرار در سرور
Once you have the `.token` file, you don't need to log in again.

**English:**
To run the server on a remote upstream machine:
1.  Copy `credentials.json` **AND** the `.token` file to the server.
2.  **Crucial**: Make sure your `server_config.json` has the **SAME** `google_folder_id` that the client just created and saved in your local config.
3.  Run: `./bin/server -c server_config.json -gc credentials.json`
4.  The server will automatically use the existing token and start immediately.

**فارسی:**
پس از دریافت فایل `.token` دیگر نیازی به لاگین مجدد نیست. برای اجرای سرور در یک ماشین دور (Upstream):
1. فایل `credentials.json` **و** فایل `.token` ساخته شده را به سرور منتقل کنید.
2. **خیلی مهم**: مطمئن شوید که در فایل `server_config.json` مقدار `google_folder_id` دقیقاً همان مقداری باشد که کلاینت به طور خودکار ساخته و در فایل کانفیگ شما ذخیره کرده است.
3. اجرا کنید: `./bin/server -c server_config.json -gc credentials.json`
4. سرور به صورت خودکار از توکن موجود استفاده کرده و بلافاصله شروع به کار می‌کند.
