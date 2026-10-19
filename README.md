# muse2api

<p align="center">
  <img src="https://img.shields.io/badge/Go-1.22%2B-00ADD8?logo=go" alt="Go Version" />
  <img src="https://img.shields.io/badge/Stdlib-Zero%20Deps-009688" alt="Zero External Deps" />
  <img src="https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker" alt="Docker" />
  <img src="https://img.shields.io/badge/API-OpenAI%20%26%20Anthropic-green" alt="API Compatible" />
  <img src="https://img.shields.io/badge/License-MIT-orange" alt="License" />
</p>

High-performance **Golang** reverse proxy that exposes **[muse.ai](https://muse.ai/)** multimodal capabilities as standard **OpenAI & Anthropic compatible RESTful APIs** — built with **100% Go standard library (zero external dependencies)**.

Powered by headless Chromium CDP WebSocket tunneling, isolated browser contexts, warm-tab affinity, and automated session keepalive.

---

## Features

- **Chat Completions, Responses & Anthropic Messages API**
  - Compatible with `/v1/chat/completions`, `/v1/responses` (Codex CLI), and `/v1/messages` (Claude Code / Anthropic SDK).
  - Native SSE streaming (`stream: true`) and synchronous responses with warm-tab and Noise WebSocket tunnel reuse (~2–3s time-to-first-token).
  - Multi-turn context, system prompt handling, and local tool-calling bridge (`<local_action>` protocol).
  - Smart model alias routing (`gpt-4o`, `gpt-5`, `claude-sonnet-4`, `deepseek-chat`, etc.).
- **Image Generation & Editing (`/v1/images/generations`, `/v1/images/edits`, `/v1/images/tasks`)**
  - Aspect ratios `1:1`, `16:9`, `9:16`, `4:3`, `3:4` with reference image upload and watermark-free output.
  - Supports both `url` and `b64_json` output formats, plus asynchronous task polling (`/v1/images/tasks`) with `Idempotency-Key` support for long-running requests behind CDNs/reverse proxies.
- **Video Generation (`/v1/videos`)**
  - Text-to-video and first-frame image-to-video (`16:9` landscape and `9:16` portrait, 5s / 6s / 8s / 10s duration hints).
  - Asynchronous task queue (`POST /v1/videos` + `GET /v1/videos/{task_id}`) and built-in media server (`/v1/media/{filename}`).
- **True Multi-Account Parallel Execution**
  - Per-account isolated Chromium `BrowserContext` sandboxes over a single headless browser process.
  - Zero cookie/session crosstalk across concurrent chat, image, and video jobs.
  - Configurable concurrency via `MUSE2API_CONCURRENCY` (default `2`) with warm-tab affinity & automatic failover.
- **Automated 48h Session Keepalive & Cloud VM Wake**
  - Background keepalive goroutine renews `hatch_vml` (+48h) and `hatch_sess` (+30d) via `/api/session` and wakes the cloud workspace VM via `/api/hatch/vm/wake`.
- **Web Admin Console & Browser Cookie Importers**
  - Built-in dark-mode Web UI (`/admin`) for account pool management, quota inspection, keepalive triggers, task logs, media gallery, and interactive API playground.
  - Includes a Chrome/Edge extension (`extension/`), Tampermonkey userscript (`tools/muse2api_cookie_importer.user.js`), and standalone Go CLI helper (`tools/get_muse_cookie.go`).

---

## Quick Start

### Option 1: Docker Compose (Recommended)

1. **Clone the repository**:
   ```bash
   git clone https://github.com/ninja-c9/muse2api.git
   cd muse2api
   ```

2. **Configure environment (optional)**:
   ```bash
   cp .env.example .env
   # Edit .env to set your own MUSE2API_KEY
   ```

3. **Start the container**:
   ```bash
   docker compose up -d --build
   ```

4. **Open the Admin Console**:
   Visit `http://<your-server-ip>:18610/admin?key=<your-MUSE2API_KEY>`

---

### Option 2: Bare-Metal / Systemd (Linux / macOS)

1. **Install Chromium and Go 1.22+**:
   ```bash
   sudo apt-get update
   sudo apt-get install -y chromium fonts-wqy-zenhei golang-go
   ```

2. **Build the binary**:
   ```bash
   go build -o muse2api .
   ```

3. **Run directly or via systemd**:
   ```bash
   ./muse2api
   # Or install systemd service:
   sudo cp deploy/muse2api.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now muse2api
   ```

4. **Nginx Reverse Proxy (Optional)**:
   See `deploy/nginx.example.conf` — set `proxy_read_timeout 600s;` and `proxy_buffering off;` for smooth SSE streaming and image/video generation.

---

## Importing Accounts

### Method A: Chrome / Edge Extension
1. Open `chrome://extensions` and enable **Developer mode**.
2. Click **Load unpacked** and select the `extension/` folder.
3. Log in to [muse.ai](https://muse.ai/) until the chat interface loads.
4. Click the extension icon, enter your server `BASE URL` and `MUSE2API_KEY`, and click **Import Cookies**.

### Method B: Tampermonkey Userscript
1. Install the Tampermonkey browser extension.
2. Create a new script from `tools/muse2api_cookie_importer.user.js`.
3. Open [muse.ai](https://muse.ai/) and click the floating **Import to muse2api** button in the bottom-right corner.

### Method C: Standalone Go CLI
```bash
go run tools/get_muse_cookie.go --base http://localhost:18610 --key m2a_your_key --label muse-01
```

---

## API Examples

Include your API key in the `Authorization` header:
```http
Authorization: Bearer <MUSE2API_KEY>
```

### 1. Chat Completions (`POST /v1/chat/completions`)

```bash
curl -X POST "http://localhost:18610/v1/chat/completions" \
  -H "Authorization: Bearer m2a_your_secret_key" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "muse-spark",
    "messages": [
      {"role": "user", "content": "Write a haiku about concurrency in Go."}
    ],
    "stream": false
  }'
```

### 2. Image Generation (`POST /v1/images/generations`)

```bash
curl -X POST "http://localhost:18610/v1/images/generations" \
  -H "Authorization: Bearer m2a_your_secret_key" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "muse-image",
    "prompt": "A futuristic cyberpunk Shiba Inu in an anti-gravity spacesuit, cinematic lighting, 8k",
    "size": "16:9",
    "response_format": "url"
  }'
```

### 3. Video Generation (`POST /v1/videos`)

```bash
# Create video task
curl -X POST "http://localhost:18610/v1/videos" \
  -H "Authorization: Bearer m2a_your_secret_key" \
  -H "Content-Type: application/json" \
  -d '{
    "prompt": "Golden autumn leaves drifting gently in the breeze with sunlight filtering through trees",
    "duration": 5,
    "size": "16:9"
  }'

# Poll task status
curl "http://localhost:18610/v1/videos/<task_id>" \
  -H "Authorization: Bearer m2a_your_secret_key"
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `MUSE2API_KEY` | Auto-generated | Admin console & API bearer key (`m2a_...`) |
| `MUSE2API_HOST` | `127.0.0.1` | Listen address (`0.0.0.0` inside Docker) |
| `MUSE2API_PORT` | `18610` | HTTP server port |
| `MUSE2API_PUBLIC_BASE` | *(empty)* | Public base URL prefix for generated media URLs |
| `MUSE2API_CHROMIUM` | Auto-detected | Path to Chromium/Chrome executable |
| `MUSE2API_CDP_PORT` | `19210` | Internal Chromium DevTools Protocol port |
| `MUSE2API_CONCURRENCY` | `2` | Maximum concurrent generation jobs |
| `MUSE2API_IMAGE_TIMEOUT` | `240` | Image generation timeout (seconds) |
| `MUSE2API_VIDEO_TIMEOUT` | `600` | Video generation timeout (seconds) |
| `MUSE2API_CHAT_TIMEOUT` | `300` | Chat completion timeout (seconds) |
| `MUSE2API_KEEPALIVE_DISABLED_ACCOUNTS` | `0` | Set to `1` to run background keepalive on disabled accounts |
| `MUSE2API_TOOL_PROTOCOL` | `1` | Enable `<local_action>` tool bridge for coding agents |

---

## Testing

Run the full offline unit & regression test suite (no external network calls or account quota required):

```bash
go test -v .
```

---

## License

Released under the [MIT License](LICENSE).
