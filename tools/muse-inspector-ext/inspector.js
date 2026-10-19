/**
 * Muse.ai Network & Tool Call Inspector (v2 - Noise E2EE Plaintext Hook)
 *
 * Phát hiện quan trọng từ bản chụp v1:
 *   muse.ai giao tiếp với Cloud VM qua WebSocket:
 *   wss://hatch.metaaivm.com/v1/noise?vm_id=...
 *   Kênh này được mã hóa đầu-cuối bằng giao thức Noise (binary frames).
 *
 * Do đó v2 đặt hook trực tiếp vào tầng giải mã/mã hóa ứng dụng trong MAIN world:
 *   1. TextEncoder.prototype.encode / encodeInto (bắt plaintext TRƯỚC khi Noise mã hóa gửi đi)
 *   2. TextDecoder.prototype.decode (bắt plaintext SAU khi Noise giải mã nhận về)
 *   3. JSON.stringify & JSON.parse (bắt mọi object RPC/chat/tool kể cả khi giải mã bằng WASM)
 *   4. Tự động lọc bỏ các frame nhị phân đã mã hóa (ciphertext) để log gọn và đọc được 100%.
 */
(function () {
  if (window.__MUSE_INSPECTOR_INSTALLED__) return;
  window.__MUSE_INSPECTOR_INSTALLED__ = true;

  const origJSONParse = window.JSON.parse;
  const origJSONStringify = window.JSON.stringify;
  const origEncode = window.TextEncoder && window.TextEncoder.prototype.encode;
  const origEncodeInto = window.TextEncoder && window.TextEncoder.prototype.encodeInto;
  const origDecode = window.TextDecoder && window.TextDecoder.prototype.decode;

  const logs = [];
  let seq = 0;
  let encryptedWsCount = 0;
  let paused = false;
  let hideHeartbeats = true;
  let activeFilter = 'all'; // 'all' | 'out' | 'in' | 'tool'
  let searchQuery = '';
  let selectedId = null;
  let inInternalHook = false;

  // Chống trùng lặp khi JSON.stringify -> TextEncoder.encode hoặc TextDecoder.decode -> JSON.parse
  const recentFingerprints = new Map();

  const TOOL_KEYWORDS = [
    'tool_calls',
    'tool_call',
    'tool_use',
    'tool_result',
    'tools',
    'function_call',
    'function_call_output',
    'tool_name',
    'tool_input',
    'imagine_media',
    'deliverable',
    'artifact',
    'mcp',
    'subagent',
    'execute_command',
    'run_command',
    'bash'
  ];

  function nowTime() {
    const d = new Date();
    return (
      d.toTimeString().split(' ')[0] +
      '.' +
      String(d.getMilliseconds()).padStart(3, '0')
    );
  }

  function isBinaryGarbage(str) {
    if (typeof str !== 'string' || !str.length) return false;
    let bad = 0;
    const len = Math.min(str.length, 500);
    for (let i = 0; i < len; i++) {
      const code = str.charCodeAt(i);
      if (
        code === 0xfffd ||
        (code < 32 && code !== 9 && code !== 10 && code !== 13)
      ) {
        bad++;
      }
    }
    return bad / len > 0.04;
  }

  function isNoiseOrTelemetry(rawStr) {
    if (!rawStr) return true;
    if (
      rawStr.includes('sentry.javascript.nextjs') ||
      rawStr.includes('@vercel/analytics') ||
      rawStr.includes('needs_cookie_consent') ||
      rawStr.includes('__muse_inspector')
    ) {
      return true;
    }
    return false;
  }

  function isDuplicate(direction, rawStr) {
    const now = Date.now();
    const key = direction + ':' + rawStr.slice(0, 300) + ':' + rawStr.length;
    const prev = recentFingerprints.get(key);
    recentFingerprints.set(key, now);
    if (recentFingerprints.size > 400) {
      for (const [k, ts] of recentFingerprints.entries()) {
        if (now - ts > 2000) recentFingerprints.delete(k);
      }
    }
    return prev !== undefined && now - prev < 150;
  }

  function safeParseJson(str) {
    if (typeof str !== 'string') return null;
    const s = str.trim();
    if (!s) return null;
    if (
      (s.startsWith('{') && s.endsWith('}')) ||
      (s.startsWith('[') && s.endsWith(']'))
    ) {
      try {
        return origJSONParse(s);
      } catch (_) {}
    }
    return null;
  }

  function detectToolSignals(rawText, parsedObj) {
    const signals = new Set();
    const checkObj = (obj, depth = 0) => {
      if (!obj || typeof obj !== 'object' || depth > 7) return;
      for (const [k, v] of Object.entries(obj)) {
        const kl = k.toLowerCase();
        if (
          kl === 'tool' ||
          kl === 'tools' ||
          kl === 'tool_calls' ||
          kl === 'tool_call' ||
          kl === 'tool_use' ||
          kl === 'tool_result' ||
          kl === 'function_call' ||
          kl === 'tool_name' ||
          kl === 'tool_input' ||
          kl === 'functions'
        ) {
          signals.add(`key:${k}`);
        }
        if (
          (kl === 'type' || kl === 'role' || kl === 'event' || kl === 'method' || kl === 'kind') &&
          typeof v === 'string' &&
          /tool|function|action|call|artifact|imagine|exec|bash|command/i.test(v)
        ) {
          signals.add(`${k}:${v}`);
        }
        if (typeof v === 'object' && v !== null) {
          checkObj(v, depth + 1);
        }
      }
    };

    if (parsedObj) {
      checkObj(parsedObj);
    }
    if (typeof rawText === 'string' && rawText.length > 0) {
      for (const kw of TOOL_KEYWORDS) {
        if (rawText.includes(kw)) {
          signals.add(`match:${kw}`);
        }
      }
    }
    return Array.from(signals);
  }

  function isHeartbeatFrame(rawText, parsedObj, url) {
    if (!rawText && !parsedObj) return false;
    const s = typeof rawText === 'string' ? rawText.trim().toLowerCase() : '';
    if (s === 'ping' || s === 'pong' || s === '2' || s === '3' || s === '{}' || s === '[]') {
      return true;
    }
    if (parsedObj && typeof parsedObj === 'object' && !Array.isArray(parsedObj)) {
      const keys = Object.keys(parsedObj);
      const t = String(
        parsedObj.type || parsedObj.event || parsedObj.op || parsedObj.method || ''
      ).toLowerCase();
      if (['ping', 'pong', 'heartbeat', 'keepalive', 'ack'].includes(t) && keys.length <= 4) {
        return true;
      }
    }
    if (url && /analytics|telemetry|sentry|monitoring|consent|save-account/i.test(url)) {
      return true;
    }
    return false;
  }

  function summarizeEntry(entry) {
    if (entry.parsed && typeof entry.parsed === 'object') {
      const p = entry.parsed;
      const typeTag =
        p.type || p.event || p.method || p.action || p.kind || p.role || '';
      const textPreview =
        p.text ||
        p.content ||
        p.delta ||
        p.prompt ||
        p.message ||
        (p.params && (p.params.text || p.params.prompt || p.params.content)) ||
        (p.data && (p.data.text || p.data.content || p.data.delta)) ||
        '';
      const prefix = typeTag ? `[${typeTag}] ` : '';
      const bodyStr =
        typeof textPreview === 'string' && textPreview
          ? textPreview.slice(0, 130)
          : origJSONStringify(p).slice(0, 130);
      return prefix + bodyStr;
    }
    if (typeof entry.raw === 'string') {
      return entry.raw.replace(/\s+/g, ' ').slice(0, 140);
    }
    return entry.url || '';
  }

  function addLog(item) {
    if (paused || inInternalHook) return;
    inInternalHook = true;
    try {
      const rawStr =
        typeof item.raw === 'string'
          ? item.raw
          : item.raw != null
          ? origJSONStringify(item.raw)
          : '';

      if (!rawStr || isBinaryGarbage(rawStr) || isNoiseOrTelemetry(rawStr)) {
        return;
      }
      if (isDuplicate(item.direction, rawStr)) {
        return;
      }

      const parsed = item.parsed !== undefined ? item.parsed : safeParseJson(rawStr);
      const toolSignals = detectToolSignals(rawStr, parsed);
      const heartbeat = isHeartbeatFrame(rawStr, parsed, item.url);

      const entry = {
        id: ++seq,
        time: nowTime(),
        ts: Date.now(),
        channel: item.channel,
        direction: item.direction, // 'out' | 'in' | 'info'
        url: item.url || '',
        method: item.method || '',
        status: item.status || null,
        raw: rawStr.length > 20000 ? rawStr.slice(0, 20000) + '...[TRUNCATED]' : rawStr,
        parsed,
        toolSignals,
        heartbeat
      };
      entry.summary = summarizeEntry(entry);

      logs.push(entry);
      if (logs.length > 800) {
        logs.shift();
      }
      scheduleRender();
    } finally {
      inInternalHook = false;
    }
  }

  // ==================== 1. HOOK TEXTENCODER & TEXTDECODER (NOISE PLAINTEXT) ====================
  if (origEncode) {
    window.TextEncoder.prototype.encode = function (str) {
      if (!inInternalHook && typeof str === 'string' && str.length >= 2) {
        const s = str.trim();
        if (s.length >= 2 && !isBinaryGarbage(s)) {
          addLog({
            channel: 'NOISE_OUT',
            direction: 'out',
            url: 'wss://hatch.metaaivm.com/v1/noise (Plaintext before encrypt)',
            raw: s
          });
        }
      }
      return origEncode.apply(this, arguments);
    };
  }

  if (origEncodeInto) {
    window.TextEncoder.prototype.encodeInto = function (str, dest) {
      if (!inInternalHook && typeof str === 'string' && str.length >= 2) {
        const s = str.trim();
        if (s.length >= 2 && !isBinaryGarbage(s)) {
          addLog({
            channel: 'NOISE_OUT',
            direction: 'out',
            url: 'wss://hatch.metaaivm.com/v1/noise (Plaintext encodeInto)',
            raw: s
          });
        }
      }
      return origEncodeInto.apply(this, arguments);
    };
  }

  if (origDecode) {
    window.TextDecoder.prototype.decode = function () {
      const res = origDecode.apply(this, arguments);
      if (!inInternalHook && typeof res === 'string' && res.length >= 2) {
        const s = res.trim();
        if (s.length >= 2 && !isBinaryGarbage(s)) {
          addLog({
            channel: 'NOISE_IN',
            direction: 'in',
            url: 'wss://hatch.metaaivm.com/v1/noise (Plaintext after decrypt)',
            raw: s
          });
        }
      }
      return res;
    };
  }

  // ==================== 2. HOOK JSON.STRINGIFY & JSON.PARSE ====================
  function looksLikeChatOrRpcObject(obj) {
    if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return false;
    const keys = Object.keys(obj);
    if (keys.length === 0 || keys.length > 40) return false;
    // Bỏ qua các object cấu hình của Next.js / React / Sentry
    if ('buildId' in obj || 'assetPrefix' in obj || 'dsn' in obj || 'sdkn' in obj) {
      return false;
    }
    const interestingKeys = [
      'type',
      'event',
      'method',
      'action',
      'kind',
      'role',
      'content',
      'text',
      'prompt',
      'message',
      'messages',
      'delta',
      'thread_id',
      'threadId',
      'turn_id',
      'turnId',
      'tool',
      'tools',
      'tool_calls',
      'tool_call',
      'tool_use',
      'tool_result',
      'function_call',
      'attachments',
      'payload',
      'data',
      'params',
      'result',
      'status',
      'id'
    ];
    return keys.some((k) => interestingKeys.includes(k));
  }

  window.JSON.stringify = function (value, replacer, space) {
    const res = origJSONStringify.apply(this, arguments);
    if (
      !inInternalHook &&
      !space &&
      typeof res === 'string' &&
      res.length >= 10 &&
      res.length <= 100000 &&
      looksLikeChatOrRpcObject(value)
    ) {
      addLog({
        channel: 'JSON_OUT',
        direction: 'out',
        url: 'JS JSON.stringify',
        raw: res,
        parsed: value
      });
    }
    return res;
  };

  window.JSON.parse = function (text, reviver) {
    const res = origJSONParse.apply(this, arguments);
    if (
      !inInternalHook &&
      typeof text === 'string' &&
      text.length >= 10 &&
      text.length <= 100000 &&
      looksLikeChatOrRpcObject(res)
    ) {
      addLog({
        channel: 'JSON_IN',
        direction: 'in',
        url: 'JS JSON.parse',
        raw: text,
        parsed: res
      });
    }
    return res;
  };

  // ==================== 3. HOOK WEBSOCKET ====================
  const NativeWebSocket = window.WebSocket;
  function PatchedWebSocket(url, protocols) {
    const ws =
      protocols !== undefined
        ? new NativeWebSocket(url, protocols)
        : new NativeWebSocket(url);

    const wsUrl = String(url);
    addLog({
      channel: 'WS_OPEN',
      direction: 'info',
      url: wsUrl.split('?')[0],
      raw: `WebSocket connected: ${wsUrl.split('?')[0]}`
    });

    const origSend = ws.send;
    ws.send = function (data) {
      try {
        if (typeof data === 'string' && !isBinaryGarbage(data)) {
          addLog({
            channel: 'WS_SEND',
            direction: 'out',
            url: wsUrl.split('?')[0],
            raw: data
          });
        } else {
          encryptedWsCount++;
          scheduleRender();
        }
      } catch (_) {}
      return origSend.apply(this, arguments);
    };

    ws.addEventListener('message', function (ev) {
      try {
        const data = ev.data;
        if (typeof data === 'string' && !isBinaryGarbage(data)) {
          addLog({
            channel: 'WS_RECV',
            direction: 'in',
            url: wsUrl.split('?')[0],
            raw: data
          });
        } else {
          encryptedWsCount++;
          scheduleRender();
        }
      } catch (_) {}
    });

    return ws;
  }
  PatchedWebSocket.prototype = NativeWebSocket.prototype;
  Object.assign(PatchedWebSocket, NativeWebSocket);
  window.WebSocket = PatchedWebSocket;

  // ==================== 4. HOOK FETCH ====================
  const origFetch = window.fetch;
  window.fetch = async function (input, init) {
    let url = '';
    let method = 'GET';
    let reqBody = '';

    try {
      if (typeof input === 'string') {
        url = input;
      } else if (input && typeof input.url === 'string') {
        url = input.url;
        method = input.method || method;
      }
      if (init && init.method) {
        method = init.method;
      }
      if (init && typeof init.body === 'string') {
        reqBody = init.body;
      }
    } catch (_) {}

    const isStaticAsset = /\.(js|css|woff2?|png|jpe?g|svg|webp|ico)(\?|$)/i.test(url);

    if (!isStaticAsset && reqBody && !isNoiseOrTelemetry(reqBody)) {
      addLog({
        channel: 'FETCH_REQ',
        direction: 'out',
        url,
        method: method.toUpperCase(),
        raw: reqBody
      });
    }

    const res = await origFetch.apply(this, arguments);

    if (!isStaticAsset && /\/api\/|thread|chat|hatch|session|message|tool/i.test(url)) {
      try {
        const ctype = (res.headers.get('content-type') || '').toLowerCase();
        if (ctype.includes('json') || ctype.includes('text')) {
          res
            .clone()
            .text()
            .then((txt) => {
              if (txt && !isNoiseOrTelemetry(txt)) {
                addLog({
                  channel: 'FETCH_RES',
                  direction: 'in',
                  url,
                  method: method.toUpperCase(),
                  status: res.status,
                  raw: txt
                });
              }
            })
            .catch(() => {});
        }
      } catch (_) {}
    }

    return res;
  };

  // ==================== 5. UI FLOATING PANEL ====================
  let uiRoot = null;
  let renderPending = false;

  function filteredLogs() {
    return logs.filter((item) => {
      if (hideHeartbeats && item.heartbeat) return false;
      if (activeFilter === 'out' && item.direction !== 'out') return false;
      if (activeFilter === 'in' && item.direction !== 'in') return false;
      if (activeFilter === 'tool' && (!item.toolSignals || item.toolSignals.length === 0)) {
        return false;
      }
      if (searchQuery) {
        const q = searchQuery.toLowerCase();
        const hay = `${item.channel} ${item.url} ${item.raw}`.toLowerCase();
        if (!hay.includes(q)) return false;
      }
      return true;
    });
  }

  function scheduleRender() {
    if (renderPending || !uiRoot) return;
    renderPending = true;
    requestAnimationFrame(() => {
      renderPending = false;
      renderList();
    });
  }

  function buildReportObject() {
    const visible = filteredLogs();
    const toolEntries = logs.filter((l) => l.toolSignals && l.toolSignals.length > 0);
    return {
      inspectorVersion: '2.0-noise-plaintext',
      capturedAt: new Date().toISOString(),
      pageUrl: location.href,
      encryptedNoiseFramesSkipped: encryptedWsCount,
      totalPlaintextCaptured: logs.length,
      visibleCount: visible.length,
      toolSignalCount: toolEntries.length,
      entries: visible.map((e) => ({
        id: e.id,
        time: e.time,
        channel: e.channel,
        direction: e.direction,
        toolSignals: e.toolSignals,
        summary: e.summary,
        parsed: e.parsed,
        raw: e.parsed ? undefined : e.raw
      }))
    };
  }

  function mountUI() {
    if (document.getElementById('__muse_inspector_host__')) return;
    const host = document.createElement('div');
    host.id = '__muse_inspector_host__';
    host.style.cssText =
      'position:fixed;z-index:2147483647;bottom:14px;right:14px;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;';
    const shadow = host.attachShadow({ mode: 'open' });

    shadow.innerHTML = `
      <style>
        * { box-sizing: border-box; }
        .pill {
          background: #0f172a;
          color: #f8fafc;
          border: 1px solid #3b82f6;
          border-radius: 999px;
          padding: 8px 14px;
          font-size: 12px;
          font-weight: 600;
          cursor: pointer;
          box-shadow: 0 8px 24px rgba(0,0,0,0.45);
          display: none;
          align-items: center;
          gap: 8px;
          user-select: none;
        }
        .pill .dot {
          width: 8px;
          height: 8px;
          border-radius: 50%;
          background: #22c55e;
        }
        .panel {
          width: 700px;
          max-width: calc(100vw - 28px);
          height: 500px;
          max-height: calc(100vh - 28px);
          background: #0b0f19;
          color: #e2e8f0;
          border: 1px solid #334155;
          border-radius: 10px;
          box-shadow: 0 16px 40px rgba(0,0,0,0.65);
          display: flex;
          flex-direction: column;
          overflow: hidden;
          font-size: 12px;
        }
        .header {
          padding: 8px 12px;
          background: #111827;
          border-bottom: 1px solid #1e293b;
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 8px;
        }
        .title {
          font-weight: 700;
          color: #60a5fa;
          display: flex;
          align-items: center;
          gap: 8px;
        }
        .badge-tool {
          background: #7c2d12;
          color: #fdba74;
          border: 1px solid #ea580c;
          padding: 1px 6px;
          border-radius: 4px;
          font-size: 11px;
        }
        .badge-noise {
          background: #1e293b;
          color: #94a3b8;
          padding: 1px 6px;
          border-radius: 4px;
          font-size: 10.5px;
        }
        .toolbar {
          padding: 6px 10px;
          background: #0f172a;
          border-bottom: 1px solid #1e293b;
          display: flex;
          flex-wrap: wrap;
          align-items: center;
          gap: 6px;
        }
        button, .tab {
          background: #1e293b;
          color: #cbd5e1;
          border: 1px solid #334155;
          border-radius: 5px;
          padding: 4px 8px;
          font-size: 11px;
          cursor: pointer;
          font-family: inherit;
        }
        button:hover, .tab:hover {
          background: #334155;
          color: #fff;
        }
        .tab.active {
          background: #2563eb;
          border-color: #3b82f6;
          color: #fff;
        }
        .btn-primary {
          background: #059669;
          border-color: #10b981;
          color: #fff;
        }
        .btn-primary:hover {
          background: #047857;
        }
        input[type="text"] {
          background: #090d16;
          color: #e2e8f0;
          border: 1px solid #334155;
          border-radius: 5px;
          padding: 4px 8px;
          font-size: 11px;
          flex: 1;
          min-width: 110px;
        }
        label.chk {
          display: inline-flex;
          align-items: center;
          gap: 4px;
          font-size: 11px;
          color: #94a3b8;
          cursor: pointer;
          user-select: none;
        }
        .body {
          flex: 1;
          display: flex;
          min-height: 0;
        }
        .list {
          width: 46%;
          border-right: 1px solid #1e293b;
          overflow-y: auto;
        }
        .detail {
          width: 54%;
          overflow-y: auto;
          padding: 8px 10px;
          background: #070a12;
          white-space: pre-wrap;
          word-break: break-all;
          font-size: 11.5px;
          line-height: 1.45;
          color: #cbd5e1;
        }
        .row {
          padding: 6px 8px;
          border-bottom: 1px solid #131c2e;
          cursor: pointer;
          display: flex;
          flex-direction: column;
          gap: 2px;
        }
        .row:hover {
          background: #131c2e;
        }
        .row.selected {
          background: #1e293b;
          border-left: 3px solid #3b82f6;
        }
        .row-top {
          display: flex;
          align-items: center;
          gap: 6px;
          font-size: 10.5px;
        }
        .tag {
          padding: 1px 5px;
          border-radius: 3px;
          font-weight: 700;
          font-size: 10px;
        }
        .tag.out { background: #1e3a8a; color: #93c5fd; }
        .tag.in { background: #064e3b; color: #6ee7b7; }
        .tag.info { background: #334155; color: #cbd5e1; }
        .tag.tool { background: #9a3412; color: #fed7aa; }
        .time { color: #64748b; }
        .row-sum {
          color: #e2e8f0;
          font-size: 11px;
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
        }
        .empty {
          padding: 20px 12px;
          color: #64748b;
          text-align: center;
        }
      </style>
      <div class="pill" id="pill">
        <span class="dot"></span>
        <span id="pill-text">🔍 Muse Inspector v2 (0)</span>
      </div>
      <div class="panel" id="panel">
        <div class="header">
          <div class="title">
            <span>🔓 Muse Inspector v2 (Noise Decrypted)</span>
            <span class="badge-tool" id="tool-count">🛠️ Tools: 0</span>
            <span class="badge-noise" id="noise-count" title="Số frame nhị phân Noise đã lọc bỏ">🔒 0 bin skipped</span>
          </div>
          <div style="display:flex;gap:6px;">
            <button id="btn-copy" class="btn-primary" title="Copy JSON đã giải mã">📋 Copy JSON</button>
            <button id="btn-dl" title="Tải file .json">📥 Tải file</button>
            <button id="btn-clear" title="Xóa log cũ">🧹 Clear</button>
            <button id="btn-min" title="Thu gọn">_ Thu gọn</button>
          </div>
        </div>
        <div class="toolbar">
          <button class="tab active" data-filter="all">All (<span id="cnt-all">0</span>)</button>
          <button class="tab" data-filter="out">⬆️ Out/Prompt (<span id="cnt-out">0</span>)</button>
          <button class="tab" data-filter="in">⬇️ In/Decrypted (<span id="cnt-in">0</span>)</button>
          <button class="tab" data-filter="tool">🛠️ Tool Signals (<span id="cnt-tool">0</span>)</button>
          <input type="text" id="search" placeholder="Lọc (tool, prompt, role...)" />
          <label class="chk">
            <input type="checkbox" id="chk-hb" checked /> Ẩn Ping
          </label>
        </div>
        <div class="body">
          <div class="list" id="list"></div>
          <div class="detail" id="detail">v2 đã mở khóa kênh mã hóa Noise (wss://hatch.metaaivm.com/v1/noise)!\nHãy bấm Clear, gửi 1 prompt trên muse.ai rồi bấm Copy JSON.</div>
        </div>
      </div>
    `;

    document.documentElement.appendChild(host);
    uiRoot = shadow;

    const pill = shadow.getElementById('pill');
    const panel = shadow.getElementById('panel');

    shadow.getElementById('btn-min').addEventListener('click', () => {
      panel.style.display = 'none';
      pill.style.display = 'inline-flex';
    });
    pill.addEventListener('click', () => {
      pill.style.display = 'none';
      panel.style.display = 'flex';
      renderList();
    });

    shadow.getElementById('btn-clear').addEventListener('click', () => {
      logs.length = 0;
      encryptedWsCount = 0;
      selectedId = null;
      shadow.getElementById('detail').textContent =
        'Đã xóa log. Hãy gửi 1 prompt trên muse.ai rồi bấm Copy JSON!';
      renderList();
    });

    shadow.getElementById('chk-hb').addEventListener('change', (e) => {
      hideHeartbeats = e.target.checked;
      renderList();
    });

    shadow.getElementById('search').addEventListener('input', (e) => {
      searchQuery = e.target.value.trim();
      renderList();
    });

    shadow.querySelectorAll('.tab').forEach((btn) => {
      btn.addEventListener('click', () => {
        shadow.querySelectorAll('.tab').forEach((b) => b.classList.remove('active'));
        btn.classList.add('active');
        activeFilter = btn.getAttribute('data-filter');
        renderList();
      });
    });

    shadow.getElementById('btn-copy').addEventListener('click', async () => {
      const btn = shadow.getElementById('btn-copy');
      inInternalHook = true;
      let report = '';
      try {
        report = origJSONStringify(buildReportObject(), null, 2);
      } finally {
        inInternalHook = false;
      }
      try {
        await navigator.clipboard.writeText(report);
        const old = btn.textContent;
        btn.textContent = '✓ Đã Copy!';
        setTimeout(() => (btn.textContent = old), 1500);
      } catch (_) {
        alert('Không thể copy tự động, hãy bấm "📥 Tải file"');
      }
    });

    shadow.getElementById('btn-dl').addEventListener('click', () => {
      inInternalHook = true;
      let report = '';
      try {
        report = origJSONStringify(buildReportObject(), null, 2);
      } finally {
        inInternalHook = false;
      }
      const blob = new Blob([report], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `muse-plaintext-${Date.now()}.json`;
      a.click();
      setTimeout(() => URL.revokeObjectURL(url), 3000);
    });

    renderList();
  }

  function renderDetail(item) {
    if (!uiRoot || !item) return;
    const detailEl = uiRoot.getElementById('detail');
    inInternalHook = true;
    try {
      const lines = [
        `=== [${item.channel}] #${item.id} (${item.time}) ===`,
        `Direction : ${item.direction.toUpperCase()}`,
        `Source    : ${item.url}`,
        item.toolSignals && item.toolSignals.length
          ? `🛠️ Tool Signals: ${item.toolSignals.join(', ')}`
          : `🛠️ Tool Signals: (không phát hiện)`,
        `----------------------------------------`,
        item.parsed ? origJSONStringify(item.parsed, null, 2) : item.raw
      ].filter(Boolean);
      detailEl.textContent = lines.join('\n');
    } finally {
      inInternalHook = false;
    }
  }

  function renderList() {
    if (!uiRoot) return;
    const baseLogs = hideHeartbeats ? logs.filter((l) => !l.heartbeat) : logs;
    const outCnt = baseLogs.filter((l) => l.direction === 'out').length;
    const inCnt = baseLogs.filter((l) => l.direction === 'in').length;
    const toolCnt = baseLogs.filter((l) => l.toolSignals && l.toolSignals.length > 0).length;

    uiRoot.getElementById('cnt-all').textContent = String(baseLogs.length);
    uiRoot.getElementById('cnt-out').textContent = String(outCnt);
    uiRoot.getElementById('cnt-in').textContent = String(inCnt);
    uiRoot.getElementById('cnt-tool').textContent = String(toolCnt);
    uiRoot.getElementById('tool-count').textContent = `🛠️ Tools: ${toolCnt}`;
    uiRoot.getElementById('noise-count').textContent = `🔒 ${encryptedWsCount} bin skipped`;
    uiRoot.getElementById('pill-text').textContent = `🔓 Muse v2 (${baseLogs.length} | 🛠️ ${toolCnt})`;

    const items = filteredLogs();
    const listEl = uiRoot.getElementById('list');
    listEl.innerHTML = '';

    if (!items.length) {
      const empty = document.createElement('div');
      empty.className = 'empty';
      empty.textContent = 'Chưa có gói tin plaintext nào. Hãy gửi thử 1 prompt!';
      listEl.appendChild(empty);
      return;
    }

    for (let i = items.length - 1; i >= 0; i--) {
      const it = items[i];
      const row = document.createElement('div');
      row.className = 'row' + (it.id === selectedId ? ' selected' : '');
      const hasTool = it.toolSignals && it.toolSignals.length > 0;
      row.innerHTML = `
        <div class="row-top">
          <span class="tag ${it.direction}">${it.channel}</span>
          ${hasTool ? `<span class="tag tool">🛠️ ${it.toolSignals[0]}</span>` : ''}
          <span class="time">${it.time}</span>
        </div>
        <div class="row-sum"></div>
      `;
      row.querySelector('.row-sum').textContent = it.summary || it.url;
      row.addEventListener('click', () => {
        selectedId = it.id;
        renderList();
        renderDetail(it);
      });
      listEl.appendChild(row);
    }
  }

  if (document.readyState === 'loading') {
    window.addEventListener('DOMContentLoaded', mountUI, { once: true });
  } else {
    mountUI();
  }
})();
