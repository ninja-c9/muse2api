# Muse2API Cookie Importer Extension

This browser extension reads your `muse.ai` session cookies (including `HttpOnly` cookies) and pushes them directly to your `muse2api` server.

## 1. Load Unpacked Extension

### Chrome / Brave / Vivaldi
1. Open `chrome://extensions` in your address bar.
2. Enable **Developer mode** in the top-right corner.
3. Click **Load unpacked** in the top-left corner.
4. Select the `extension` folder containing `manifest.json`, `popup.html`, and `popup.js`.

### Microsoft Edge
1. Open `edge://extensions`.
2. Enable **Developer mode** in the left sidebar.
3. Click **Load unpacked** and select the `extension` folder.

## 2. Usage
1. Log in to [https://muse.ai/](https://muse.ai/) in the same browser until the chat interface is visible.
2. Copy the **Base URL** and **API Key** from your `muse2api` Admin Console (`/admin`).
3. Click the extension icon in the toolbar, paste the **Base URL** and **API Key**, and click **Read & Import**.

## 3. Troubleshooting
- **"No muse.ai cookies found"**: Make sure you are logged in to `https://muse.ai/` in this browser profile.
- **"Missing core cookies (`hatch_sess`, etc.)"**: Complete the login flow until the chat screen appears.
- **"Invalid API Key (HTTP 401)"**: Verify your `m2a_...` key from the Admin Console.
