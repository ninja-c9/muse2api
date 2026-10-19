const $ = (id) => document.getElementById(id);
const STORE = 'muse2api_ext_cfg';

const ESSENTIAL = ['hatch_sess', 'hatch_gw', 'hatch_vml', 'hatch_native_auth_device'];

function log(html, cls) {
  const el = $('log');
  el.className = 'show';
  el.innerHTML = cls ? `<span class="${cls}">${html}</span>` : html;
}

function normBase(v) {
  let s = (v || '').trim();
  if (!s) return '';
  if (!/^https?:\/\//i.test(s)) s = 'https://' + s;
  s = s.replace(/\/+$/, '');
  s = s.replace(/\/v1$/i, '');
  return s;
}

async function loadCfg() {
  const o = await chrome.storage.local.get(STORE);
  const c = o[STORE] || {};
  if (c.base) $('base').value = c.base;
  if (c.key) $('key').value = c.key;
  if (c.label) $('label').value = c.label;
  if (!c.base) {
    try {
      const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
      const u = tab && tab.url ? new URL(tab.url) : null;
      if (u && /\/admin/.test(u.pathname)) {
        $('base').value = u.origin;
        const k = new URLSearchParams(u.search).get('key');
        if (k) $('key').value = k;
      }
    } catch (e) {}
  }
}

async function saveCfg() {
  await chrome.storage.local.set({
    [STORE]: {
      base: normBase($('base').value),
      key: $('key').value.trim(),
      label: $('label').value.trim(),
    },
  });
}

async function grabCookies() {
  const all = await chrome.cookies.getAll({ domain: 'muse.ai' });
  const out = {}, exp = {};
  for (const c of all) {
    const dom = (c.domain || '').replace(/^\./, '');
    if (!dom.endsWith('muse.ai')) continue;
    out[c.name] = c.value;
    if (c.expirationDate) exp[c.name] = Math.floor(c.expirationDate);
  }
  return { cookies: out, expires: exp };
}

async function run() {
  const base = normBase($('base').value);
  const key = $('key').value.trim();
  const label = $('label').value.trim();

  if (!base) return log('Please enter the Server URL first.', 'bad');
  if (!key) return log('Please enter the API Key first.', 'bad');

  $('go').disabled = true;
  log('Reading muse.ai cookies...');

  try {
    const { cookies, expires } = await grabCookies();
    const names = Object.keys(cookies);
    if (!names.length) {
      return log('No muse.ai cookies found.\nPlease open https://muse.ai/ and log in first.', 'bad');
    }
    const missing = ESSENTIAL.filter((n) => !(n in cookies));
    if (missing.length) {
      log(`Found ${names.length} cookies, but missing core cookies: ${missing.join(', ')}\n`
          + 'Please complete login on muse.ai until the chat view appears.', 'warn');
      return;
    }

    log(`Found ${names.length} cookies, uploading to ${base} ...`);

    const r = await fetch(base + '/admin/accounts', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + key,
      },
      body: JSON.stringify({ label, cookies, expires }),
    });

    const text = await r.text();
    let data;
    try { data = JSON.parse(text); } catch (e) { data = { raw: text }; }

    if (r.status === 401) {
      return log('Invalid API Key (HTTP 401).\nPlease copy the API Key from your Admin Console.', 'bad');
    }
    if (!r.ok) {
      return log(`Import failed: HTTP ${r.status}\n${text.slice(0, 300)}`, 'bad');
    }

    const a = (data.added && data.added[0]) || {};
    await saveCfg();
    log(`✓ Imported successfully\nLabel: ${a.label || label || '(auto)'}\n`
        + `Account ID: ${a.id || '?'}\nCookies: ${a.cookie_count || names.length}\n`
        + `Expires at: ${a.expires_at ? new Date(a.expires_at * 1000).toLocaleString() : 'Unknown'}\n`
        + (data.warning ? `\nNote: ${data.warning}` : ''), 'ok');
  } catch (e) {
    log('Error: ' + (e && e.message ? e.message : String(e))
        + '\n\nCommon causes:\n'
        + '· Server URL is incorrect or unreachable\n'
        + '· Untrusted HTTPS certificate\n'
        + '· Request blocked by browser CORS policy', 'bad');
  } finally {
    $('go').disabled = false;
  }
}

$('go').addEventListener('click', run);
loadCfg();
