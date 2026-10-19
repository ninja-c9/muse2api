package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const AttSel = `[data-testid^="hatch-chat-attachment-presentation-"]`

type MuseAuthError struct {
	Msg string
}

func (e *MuseAuthError) Error() string {
	return e.Msg
}

type MuseGenerationError struct {
	Msg string
}

func (e *MuseGenerationError) Error() string {
	return e.Msg
}

func isAuthErr(err error) bool {
	var ae *MuseAuthError
	return errors.As(err, &ae)
}

func isGenErr(err error) bool {
	var ge *MuseGenerationError
	return errors.As(err, &ge)
}

// sleepSec allows tests to advance a virtual clock instead of sleeping in real time.
var sleepSec = func(sec float64) {
	if sec > 0 {
		time.Sleep(time.Duration(sec * float64(time.Second)))
	}
}

// httpDo executes HTTP requests and can be overridden in offline unit tests.
var httpDo = func(req *http.Request, timeout time.Duration, followRedirects bool) (*http.Response, error) {
	client := &http.Client{Timeout: timeout}
	if !followRedirects {
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}
	}
	return client.Do(req)
}

type PageSession struct {
	Page      *CDP
	AccountID string
	ContextID string
}

type MuseEngine struct {
	cfg            *Config
	proc           *exec.Cmd
	browser        *CDP
	startMu        sync.Mutex
	contextsMu     sync.Mutex
	activeContexts map[string]bool
	renewMu        sync.Mutex
	lastHTTPRenew  map[string]float64
	logFile        *os.File

	// Default / test-injected page and per-account isolated sessions
	sessionsMu       sync.Mutex
	sessions         map[string]*PageSession
	Page             *CDP
	CurrentAccID     string
	CurrentContextID string

	// Optional test overrides
	StartFunc       func() error
	OpenPageFunc    func() (*CDP, string, error)
	ApplyCookiesFn  func(page *CDP, cookies map[string]string, expires map[string]int64)
	EnsurePageFunc  func(cookies map[string]string, expires map[string]int64, accountID string) (*CDP, error)
	RefreshFunc     func(cookies map[string]string, expires map[string]int64) (*CDP, error)
	AttachmentsFunc func() []map[string]any
	ScrollBottomFn  func()
	ClickPointFn    func(x, y int)
	NormalizeImgFn  func(img string) (string, string)
	GenerateFunc    func(cookies map[string]string, prompt, expect string, timeout int, expires map[string]int64, accountID string, onProgress func(int), referenceImage string, stopCh <-chan struct{}) (map[string]any, error)
	QuotaFunc       func(cookies map[string]string, expires map[string]int64, accountID string) (map[string]any, error)
}

func NewMuseEngine(cfg *Config) *MuseEngine {
	if cfg != nil && cfg.ProfileDir() != "" {
		_ = os.MkdirAll(cfg.ProfileDir(), 0o755)
	}
	return &MuseEngine{
		cfg:            cfg,
		activeContexts: map[string]bool{},
		lastHTTPRenew:  map[string]float64{},
		sessions:       map[string]*PageSession{},
	}
}

func (e *MuseEngine) getPage(accountID string) *CDP {
	e.sessionsMu.Lock()
	defer e.sessionsMu.Unlock()
	if accountID != "" {
		if s, ok := e.sessions[accountID]; ok && s.Page != nil {
			return s.Page
		}
	}
	return e.Page
}

func (e *MuseEngine) setSession(accountID string, page *CDP, ctxID string) {
	e.sessionsMu.Lock()
	defer e.sessionsMu.Unlock()
	if accountID != "" {
		e.sessions[accountID] = &PageSession{
			Page:      page,
			AccountID: accountID,
			ContextID: ctxID,
		}
	}
	e.Page = page
	e.CurrentAccID = accountID
	e.CurrentContextID = ctxID
}

func (e *MuseEngine) BrowserRunning() bool {
	e.startMu.Lock()
	defer e.startMu.Unlock()
	if e.proc == nil || e.proc.Process == nil {
		return false
	}
	if e.proc.ProcessState != nil && e.proc.ProcessState.Exited() {
		return false
	}
	return true
}

func (e *MuseEngine) debugURL() string {
	return fmt.Sprintf("http://127.0.0.1:%d/json/version", e.cfg.CDPPort)
}

func (e *MuseEngine) Start() error {
	if e.StartFunc != nil {
		return e.StartFunc()
	}
	e.startMu.Lock()
	defer e.startMu.Unlock()

	if e.proc != nil && (e.proc.ProcessState == nil || !e.proc.ProcessState.Exited()) && e.browser != nil {
		return nil
	}

	if e.proc == nil {
		if v, err := httpJSON(e.debugURL(), 1*time.Second); err == nil {
			if wsURL, ok := v["webSocketDebuggerUrl"].(string); ok && wsURL != "" {
				if b, err := NewCDP(wsURL, 180*time.Second); err == nil {
					e.browser = b
					return nil
				}
			}
		}
	}

	for _, lockName := range []string{"SingletonLock", "SingletonSocket", "SingletonCookie"} {
		lp := filepath.Join(e.cfg.ProfileDir(), lockName)
		if _, err := os.Lstat(lp); err == nil {
			_ = os.Remove(lp)
		}
	}

	_ = os.MkdirAll(e.cfg.DataDir(), 0o755)
	logPath := filepath.Join(e.cfg.DataDir(), "chromium.log")
	lf, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err == nil {
		e.logFile = lf
	}

	args := []string{
		"--headless=new", "--no-sandbox", "--disable-gpu",
		"--disable-dev-shm-usage", "--disable-background-networking",
		"--no-first-run", "--no-default-browser-check",
		"--autoplay-policy=no-user-gesture-required",
		"--window-size=1440,2400",
		fmt.Sprintf("--remote-debugging-port=%d", e.cfg.CDPPort),
		"--remote-allow-origins=*",
		fmt.Sprintf("--user-data-dir=%s", e.cfg.ProfileDir()),
		"about:blank",
	}

	cmd := exec.Command(e.cfg.Chromium, args...)
	env := os.Environ()
	hasHome := false
	for i, kv := range env {
		if strings.HasPrefix(kv, "HOME=") {
			hasHome = true
		} else if strings.HasPrefix(kv, "PATH=") && e.cfg.ExtraPath != "" {
			env[i] = "PATH=" + e.cfg.ExtraPath + string(os.PathListSeparator) + strings.TrimPrefix(kv, "PATH=")
		}
	}
	if !hasHome && e.cfg.HomeDir != "" {
		env = append(env, "HOME="+e.cfg.HomeDir)
	}
	cmd.Env = env
	if e.cfg.HomeDir != "" {
		if st, err := os.Stat(e.cfg.HomeDir); err == nil && st.IsDir() {
			cmd.Dir = e.cfg.HomeDir
		}
	}
	if e.logFile != nil {
		cmd.Stdout = e.logFile
		cmd.Stderr = e.logFile
	}

	if err := cmd.Start(); err != nil {
		return &MuseGenerationError{Msg: fmt.Sprintf("Failed to start Chromium: %v", err)}
	}
	e.proc = cmd
	go func(c *exec.Cmd) {
		_ = c.Wait()
	}(cmd)

	var lastErr error
	for i := 0; i < 90; i++ {
		v, err := httpJSON(e.debugURL(), 2*time.Second)
		if err == nil {
			if wsURL, ok := v["webSocketDebuggerUrl"].(string); ok && wsURL != "" {
				b, err := NewCDP(wsURL, 180*time.Second)
				if err == nil {
					e.browser = b
					return nil
				}
				lastErr = err
			}
		} else {
			lastErr = err
		}
		sleepSec(1.0)
	}
	return &MuseGenerationError{Msg: fmt.Sprintf("Failed to start Chromium: %v", lastErr)}
}

func (e *MuseEngine) Stop() {
	e.startMu.Lock()
	defer e.startMu.Unlock()

	e.CloseCurrentPage("")
	e.sessionsMu.Lock()
	for accID := range e.sessions {
		e.closeSessionLocked(accID)
	}
	e.sessionsMu.Unlock()

	if e.browser != nil {
		e.contextsMu.Lock()
		for ctxID := range e.activeContexts {
			_, _ = e.browser.Send("Target.disposeBrowserContext", map[string]any{"browserContextId": ctxID}, 5*time.Second)
		}
		e.activeContexts = map[string]bool{}
		e.contextsMu.Unlock()
		e.browser.Close()
		e.browser = nil
	}
	if e.proc != nil && e.proc.Process != nil {
		_ = e.proc.Process.Kill()
		e.proc = nil
	}
	if e.logFile != nil {
		_ = e.logFile.Close()
		e.logFile = nil
	}
}

func (e *MuseEngine) closeSessionLocked(accountID string) {
	var p *CDP
	var ctxID string
	if accountID != "" {
		if s, ok := e.sessions[accountID]; ok {
			p = s.Page
			ctxID = s.ContextID
			delete(e.sessions, accountID)
		}
	}
	if p == nil && (accountID == "" || e.CurrentAccID == accountID) {
		p = e.Page
		ctxID = e.CurrentContextID
	}
	if p != nil {
		p.Close()
	}
	if e.Page == p {
		e.Page = nil
		e.CurrentAccID = ""
		e.CurrentContextID = ""
	}
	if ctxID != "" && e.browser != nil {
		_, _ = e.browser.Send("Target.disposeBrowserContext", map[string]any{"browserContextId": ctxID}, 5*time.Second)
		e.contextsMu.Lock()
		delete(e.activeContexts, ctxID)
		e.contextsMu.Unlock()
	}
}

func (e *MuseEngine) CloseCurrentPage(accountID string) {
	e.sessionsMu.Lock()
	defer e.sessionsMu.Unlock()
	e.closeSessionLocked(accountID)
}

func (e *MuseEngine) openPage() (*CDP, string, error) {
	if e.OpenPageFunc != nil {
		return e.OpenPageFunc()
	}
	if e.browser != nil {
		res, err := e.browser.Send("Target.createBrowserContext", nil, 15*time.Second)
		if err == nil {
			resMap, _ := res["result"].(map[string]any)
			ctxID, _ := resMap["browserContextId"].(string)
			if ctxID != "" {
				e.contextsMu.Lock()
				e.activeContexts[ctxID] = true
				e.contextsMu.Unlock()

				tRes, err := e.browser.Send("Target.createTarget", map[string]any{
					"url":              "about:blank",
					"browserContextId": ctxID,
				}, 15*time.Second)
				if err == nil {
					tMap, _ := tRes["result"].(map[string]any)
					targetID, _ := tMap["targetId"].(string)
					if targetID != "" {
						wsURL := fmt.Sprintf("ws://127.0.0.1:%d/devtools/page/%s", e.cfg.CDPPort, targetID)
						page, err := NewCDP(wsURL, 180*time.Second)
						if err == nil {
							_, _ = page.Send("Network.enable", nil, 10*time.Second)
							_, _ = page.Send("Page.enable", nil, 10*time.Second)
							_, _ = page.Send("Runtime.enable", nil, 10*time.Second)
							_, _ = page.Send("Browser.setDownloadBehavior", map[string]any{
								"behavior":     "allow",
								"downloadPath": e.cfg.DownloadDir(),
							}, 10*time.Second)
							return page, ctxID, nil
						}
					}
				}
			}
		} else {
			log.Printf("[WARN] Target.createBrowserContext failed, falling back to default tab: %v", err)
		}
	}

	u := fmt.Sprintf("http://127.0.0.1:%d/json/new?about:blank", e.cfg.CDPPort)
	req, err := http.NewRequest(http.MethodPut, u, nil)
	if err != nil {
		return nil, "", err
	}
	resp, err := httpDo(req, 10*time.Second, true)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	var tgt map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&tgt); err != nil {
		return nil, "", err
	}
	wsURL, _ := tgt["webSocketDebuggerUrl"].(string)
	page, err := NewCDP(wsURL, 180*time.Second)
	if err != nil {
		return nil, "", err
	}
	_, _ = page.Send("Network.enable", nil, 10*time.Second)
	_, _ = page.Send("Page.enable", nil, 10*time.Second)
	_, _ = page.Send("Runtime.enable", nil, 10*time.Second)
	_, _ = page.Send("Browser.setDownloadBehavior", map[string]any{
		"behavior":     "allow",
		"downloadPath": e.cfg.DownloadDir(),
	}, 10*time.Second)
	return page, "", nil
}

func formatCookieHeader(cookies map[string]string) string {
	var parts []string
	for k, v := range cookies {
		if v != "" {
			parts = append(parts, k+"="+v)
		}
	}
	return strings.Join(parts, "; ")
}

func RenewSessionHTTP(cookies map[string]string, expires map[string]int64, wakeVM bool) (map[string]any, error) {
	curCookies := map[string]string{}
	for k, v := range cookies {
		curCookies[k] = v
	}
	curExp := map[string]int64{}
	for k, v := range expires {
		if v > 0 {
			curExp[k] = v
		}
	}

	makeHeaders := func() http.Header {
		h := http.Header{}
		h.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36")
		h.Set("Origin", "https://muse.ai")
		h.Set("Referer", "https://muse.ai/thread/new")
		h.Set("Sec-Fetch-Site", "same-origin")
		h.Set("Sec-Fetch-Mode", "cors")
		h.Set("Sec-Fetch-Dest", "empty")
		h.Set("Cookie", formatCookieHeader(curCookies))
		return h
	}

	req, err := http.NewRequest(http.MethodGet, "https://muse.ai/api/session", nil)
	if err != nil {
		return nil, &MuseGenerationError{Msg: "Failed to build /api/session request"}
	}
	req.Header = makeHeaders()

	resp, err := httpDo(req, 12*time.Second, false)
	if err != nil {
		return nil, &MuseGenerationError{Msg: "/api/session network request failed; please check server connectivity/proxy"}
	}
	bodyBytes, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode == 401 {
		return nil, &MuseAuthError{Msg: "Session authentication failed (/api/session HTTP 401); please verify login on muse.ai and re-import cookies"}
	}
	if resp.StatusCode != 200 {
		hint := "Upstream request failed; please retry later and check server connectivity"
		if resp.StatusCode == 403 {
			hint = "Access denied; check server IP/region/access restrictions (does not indicate expired cookies)"
		}
		return nil, &MuseGenerationError{Msg: fmt.Sprintf("/api/session HTTP %d: %s", resp.StatusCode, hint)}
	}
	if readErr != nil {
		return nil, &MuseGenerationError{Msg: "/api/session HTTP 200 returned non-JSON; session state unconfirmed"}
	}

	var sj map[string]any
	if err := json.Unmarshal(bodyBytes, &sj); err != nil || sj == nil {
		return nil, &MuseGenerationError{Msg: "/api/session HTTP 200 returned non-JSON; session state unconfirmed"}
	}
	status, _ := sj["status"].(string)
	if status != "assigned" {
		return nil, &MuseGenerationError{Msg: "/api/session HTTP 200 did not return an assigned session; please check workspace status on muse.ai"}
	}

	for _, c := range resp.Cookies() {
		if c.Value != "" {
			curCookies[c.Name] = c.Value
		}
		if !c.Expires.IsZero() && c.Expires.Unix() > 0 {
			curExp[c.Name] = c.Expires.Unix()
		} else if c.MaxAge > 0 {
			curExp[c.Name] = nowUnix() + int64(c.MaxAge)
		}
	}

	vmID, _ := sj["vm_id"].(string)
	vmState, _ := sj["vm_state"].(string)
	wakeOK := false
	wakeRequested := false

	if wakeVM && vmID != "" && vmState != "DISABLED" {
		wakeBody, _ := json.Marshal(map[string]any{
			"vm_id":              vmID,
			"retry_count":        0,
			"connect_attempt_id": randomHex(32),
		})
		wReq, err := http.NewRequest(http.MethodPost, "https://muse.ai/api/hatch/vm/wake", bytes.NewReader(wakeBody))
		if err == nil {
			wReq.Header = makeHeaders()
			wReq.Header.Set("Content-Type", "application/json")
			wResp, wErr := httpDo(wReq, 8*time.Second, true)
			if wErr == nil {
				_ = wResp.Body.Close()
				wakeRequested = wResp.StatusCode == 200
				if wakeRequested {
					vReq, err := http.NewRequest(http.MethodGet, "https://muse.ai/api/session", nil)
					if err == nil {
						vReq.Header = makeHeaders()
						vResp, vErr := httpDo(vReq, 12*time.Second, false)
						if vErr == nil {
							vBytes, _ := io.ReadAll(vResp.Body)
							_ = vResp.Body.Close()
							if vResp.StatusCode == 200 {
								var vs map[string]any
								if json.Unmarshal(vBytes, &vs) == nil && vs != nil {
									if vs["status"] == "assigned" {
										if st, ok := vs["vm_state"].(string); ok && st != "" {
											vmState = st
										}
										wakeOK = vmState == "RUNNING"
									}
								}
							}
						}
					}
				}
			}
		}
	}

	return map[string]any{
		"ok":             status == "assigned",
		"status":         status,
		"vm_id":          vmID,
		"vm_state":       vmState,
		"wake_ok":        wakeOK,
		"wake_requested": wakeRequested,
		"cookies":        curCookies,
		"cookies_exp":    curExp,
	}, nil
}

func (e *MuseEngine) applyCookies(page *CDP, cookies map[string]string, expires map[string]int64) {
	if e.ApplyCookiesFn != nil {
		e.ApplyCookiesFn(page, cookies, expires)
		return
	}
	_, _ = page.Send("Network.clearBrowserCookies", nil, 5*time.Second)
	now := nowFloat()
	for name, value := range cookies {
		if value == "" {
			continue
		}
		expVal := now + 7*86400
		if expires != nil {
			if exp, ok := expires[name]; ok && float64(exp) > now+3600 {
				expVal = float64(exp)
			}
		}
		for _, dom := range []string{".muse.ai", "muse.ai"} {
			_, _ = page.Send("Network.setCookie", map[string]any{
				"name":    name,
				"value":   value,
				"domain":  dom,
				"path":    "/",
				"secure":  true,
				"expires": expVal,
			}, 5*time.Second)
		}
	}
}

func (e *MuseEngine) ReadCookies(accountID string) map[string]map[string]any {
	p := e.getPage(accountID)
	if p == nil {
		return map[string]map[string]any{}
	}
	siteURL := "https://muse.ai/"
	if e.cfg != nil && e.cfg.SiteURL != "" {
		siteURL = e.cfg.SiteURL
	}
	msg, err := p.Send("Network.getCookies", map[string]any{"urls": []string{siteURL}}, 20*time.Second)
	if err != nil {
		return map[string]map[string]any{}
	}
	out := map[string]map[string]any{}
	resMap, _ := msg["result"].(map[string]any)
	cookiesList, _ := resMap["cookies"].([]any)
	for _, item := range cookiesList {
		c, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name, _ := c["name"].(string)
		if name == "" {
			continue
		}
		exp, ok := toInt64(c["expires"])
		if !ok {
			exp = -1
		}
		val, _ := c["value"].(string)
		out[name] = map[string]any{
			"value":   val,
			"expires": exp,
		}
	}
	return out
}

const WaitWSReadyJS = `(function(){
    if (document.readyState !== 'complete') return 'loading';
    if (!document.querySelector('textarea')) return 'no-ta';
    var h = document.querySelector('[data-hatch-shell-hydration-state]');
    if (h && h.getAttribute('data-hatch-shell-hydration-state') !== 'hydrated') return 'hydrating';
    var b = document.body ? (document.body.innerText || '') : '';
    if (b.indexOf('Connecting...') !== -1) return 'connecting';
    // Empty composers may not render Send until text has been entered.
    if (document.querySelector('textarea').disabled) return 'input-disabled';
    return 'ready';
})()`

func (e *MuseEngine) waitWSReady(page *CDP, timeoutSec float64) bool {
	deadline := nowFloat() + timeoutSec
	for nowFloat() < deadline {
		st := page.JSStr(WaitWSReadyJS)
		if st == "ready" {
			return true
		}
		sleepSec(0.1)
	}
	return false
}

func (e *MuseEngine) ResetThread(accountID string, forChat bool) {
	p := e.getPage(accountID)
	if p == nil {
		return
	}
	arg := "false"
	if forChat {
		arg = "true"
	}
	js := fmt.Sprintf(`(function(forChat){
        var d = document.querySelector('[role="dialog"]');
        if (d) {
            var b = d.querySelector('button[aria-label*="close" i], button');
            if (b) b.click();
        }
        var scope = document.querySelector('main,[class*="chat-scroll"],[class*="hatch-chat-scroll"]') || document.body;
        var bubbleCount = scope ? scope.querySelectorAll('div[class*="hatch-chat-groupable-bubble"]').length : 0;
        var hasAtts = document.querySelectorAll('[data-testid^="hatch-chat-attachment-presentation-"]').length > 0;
        var hasStop = !!(document.querySelector('[data-testid="hatch-composer-stop-button"]')
            || document.querySelector('button[aria-label*="Stop" i]')
            || document.querySelector('button[aria-label*="停止"]'));
        var bodyTxt = document.body ? (document.body.innerText || '') : '';
        var hasStuck = bodyTxt.indexOf('Still sending') !== -1 || bodyTxt.indexOf('Connecting...') !== -1;
        if (hasStop || hasStuck || hasAtts) return true;
        if (forChat) {
            return bubbleCount >= 24;
        }
        return (window.location.pathname !== '/thread/new') || bubbleCount > 0;
    })(%s)`, arg)

	if p.JSBool(js) {
		_, _ = p.Send("Page.navigate", map[string]any{"url": "https://muse.ai/thread/new"}, 15*time.Second)
		tEnd := nowFloat() + 10.0
		for nowFloat() < tEnd {
			sleepSec(0.08)
			ready := p.JSBool(`(function(){
                return document.readyState === 'complete'
                    && !!document.querySelector('textarea')
                    && document.querySelectorAll('div[class*="hatch-chat-groupable-bubble"]').length === 0;
            })()`)
			if ready {
				break
			}
		}
		e.waitWSReady(p, 12.0)
	}
}

var loggedOutRe = regexp.MustCompile(`(?i)log in|sign in|create an account|登录|use another account`)

func (e *MuseEngine) EnsurePage(cookies map[string]string, expires map[string]int64, accountID string) (*CDP, error) {
	if e.EnsurePageFunc != nil {
		return e.EnsurePageFunc(cookies, expires, accountID)
	}
	existing := e.getPage(accountID)
	if existing != nil && (accountID == "" || e.CurrentAccID == accountID) {
		if existing.JSBool("!!document.querySelector('textarea')") && e.waitWSReady(existing, 8.0) {
			return existing, nil
		}
	}
	e.CloseCurrentPage(accountID)

	nowTS := nowFloat()
	e.renewMu.Lock()
	if e.lastHTTPRenew == nil {
		e.lastHTTPRenew = map[string]float64{}
	}
	lastRenew := e.lastHTTPRenew[accountID]
	e.renewMu.Unlock()

	if accountID == "" || (nowTS-lastRenew > 600) {
		renewed, err := RenewSessionHTTP(cookies, expires, true)
		if err != nil {
			if isAuthErr(err) {
				return nil, err
			}
			log.Printf("[WARN] Pre-renewal of /api/session failed (continuing with browser load): %v", err)
		} else {
			if c, ok := renewed["cookies"].(map[string]string); ok && len(c) > 0 {
				cookies = c
			}
			if ce, ok := renewed["cookies_exp"].(map[string]int64); ok && len(ce) > 0 {
				expires = ce
			}
			if accountID != "" {
				e.renewMu.Lock()
				e.lastHTTPRenew[accountID] = nowTS
				e.renewMu.Unlock()
			}
		}
	}

	page, ctxID, err := e.openPage()
	if err != nil {
		return nil, &MuseGenerationError{Msg: fmt.Sprintf("Failed to open browser tab: %v", err)}
	}
	e.applyCookies(page, cookies, expires)
	_, _ = page.Send("Page.navigate", map[string]any{"url": "https://muse.ai/thread/new"}, 30*time.Second)
	e.setSession(accountID, page, ctxID)

	loginWait := 60.0
	if e.cfg != nil && e.cfg.LoginWait >= 0 {
		loginWait = float64(e.cfg.LoginWait)
	}
	if e.waitWSReady(page, loginWait) {
		return page, nil
	}

	body := strings.ToLower(page.JSStr("document.body ? document.body.innerText.slice(0,1200) : ''"))
	e.CloseCurrentPage(accountID)
	if strings.Contains(body, "connecting...") {
		return nil, &MuseGenerationError{Msg: "Page gateway connection timed out (Connecting...); switching account"}
	}
	if loggedOutRe.MatchString(body) {
		return nil, &MuseAuthError{Msg: "Session logged out by muse.ai; please re-import cookies"}
	}
	return nil, &MuseGenerationError{Msg: "muse.ai page load timed out (chat input did not appear); please check server connectivity"}
}

func (e *MuseEngine) Refresh(cookies map[string]string, expires map[string]int64, accountID string) (*CDP, error) {
	if e.RefreshFunc != nil {
		return e.RefreshFunc(cookies, expires)
	}
	e.CloseCurrentPage(accountID)
	return e.EnsurePage(cookies, expires, accountID)
}

func (e *MuseEngine) clickPoint(page *CDP, x, y int) {
	if e.ClickPointFn != nil {
		e.ClickPointFn(x, y)
		return
	}
	for _, t := range []string{"mousePressed", "mouseReleased"} {
		_, _ = page.Send("Input.dispatchMouseEvent", map[string]any{
			"type":       t,
			"x":          x,
			"y":          y,
			"button":     "left",
			"clickCount": 1,
		}, 5*time.Second)
	}
}

const ClickJSFmt = `(function(){var sel=%s;var b=[...document.querySelectorAll(sel)].filter(function(x){return x.offsetParent!==null;})[0];if(!b)return null;var r=b.getBoundingClientRect();return JSON.stringify({x:Math.round(r.x+r.width/2),y:Math.round(r.y+r.height/2)});})()`

func (e *MuseEngine) Quota(cookies map[string]string, expires map[string]int64, accountID string) (map[string]any, error) {
	if e.QuotaFunc != nil {
		return e.QuotaFunc(cookies, expires, accountID)
	}
	p, err := e.EnsurePage(cookies, expires, accountID)
	if err != nil {
		return nil, err
	}
	sleepSec(1.0)

	selJSON, _ := json.Marshal(`button[aria-label="Settings"]`)
	raw := p.JSStr(fmt.Sprintf(ClickJSFmt, string(selJSON)))
	if raw == "" {
		return nil, &MuseGenerationError{Msg: "Settings button not found"}
	}
	var pt struct {
		X int `json:"x"`
		Y int `json:"y"`
	}
	if err := json.Unmarshal([]byte(raw), &pt); err != nil {
		return nil, &MuseGenerationError{Msg: "Settings button not found"}
	}
	e.clickPoint(p, pt.X, pt.Y)
	sleepSec(1.6)

	raw = p.JSStr(`(function(){` +
		`var els=[...document.querySelectorAll('div,span,li,[role=menuitem],button')]` +
		`.filter(function(e){return e.offsetParent!==null` +
		`&&(e.textContent||'').trim()==='Settings'` +
		`&&e.getAttribute('aria-label')!=='Settings'` +
		`&&e.children.length<=3;});` +
		`if(!els.length)return null;` +
		`var el=els[els.length-1];var r=el.getBoundingClientRect();` +
		`return JSON.stringify({x:Math.round(r.x+r.width/2),` +
		`y:Math.round(r.y+r.height/2)});})()`)
	if raw == "" {
		return nil, &MuseGenerationError{Msg: "Settings menu did not open"}
	}
	if err := json.Unmarshal([]byte(raw), &pt); err != nil {
		return nil, &MuseGenerationError{Msg: "Settings menu did not open"}
	}
	e.clickPoint(p, pt.X, pt.Y)
	sleepSec(3.0)

	txt := ""
	for i := 0; i < 6; i++ {
		txt = p.JSStr(`(function(){var d=document.querySelector('[role=dialog],[aria-modal=true]');return d?(d.innerText||''):'';})()`)
		if strings.Contains(txt, "Usage") || strings.Contains(txt, "used") {
			break
		}
		sleepSec(1.2)
	}

	for _, t := range []string{"keyDown", "keyUp"} {
		_, _ = p.Send("Input.dispatchKeyEvent", map[string]any{
			"type":                  t,
			"key":                   "Escape",
			"code":                  "Escape",
			"windowsVirtualKeyCode": 27,
			"nativeVirtualKeyCode":  27,
		}, 5*time.Second)
	}
	sleepSec(0.5)

	return ParseQuota(txt), nil
}

var (
	planRe        = regexp.MustCompile(`(?i)^(.+?)\s*plan$`)
	weeklyResetRe = regexp.MustCompile(`Weekly limit resets? on (.+)`)
	weeklyUsedRe  = regexp.MustCompile(`(\d+)%\s*used`)
	extraUsedRe   = regexp.MustCompile(`(\d+)%\s*used\s*\(([^)]+)\)`)
)

func ParseQuota(txt string) map[string]any {
	var lines []string
	for _, ln := range strings.Split(txt, "\n") {
		s := strings.TrimSpace(ln)
		if s != "" {
			lines = append(lines, s)
		}
	}
	rawLines := lines
	if len(rawLines) > 40 {
		rawLines = rawLines[:40]
	}
	out := map[string]any{
		"raw": strings.Join(rawLines, "\n"),
	}
	for _, ln := range lines {
		if planRe.MatchString(ln) {
			out["plan"] = ln
			break
		}
	}
	if m := weeklyResetRe.FindStringSubmatch(txt); len(m) > 1 {
		out["weekly_reset"] = strings.TrimSpace(m[1])
	}
	if m := weeklyUsedRe.FindStringSubmatch(txt); len(m) > 1 {
		if n, err := strconv.Atoi(m[1]); err == nil {
			out["weekly_used_pct"] = n
		}
	}
	if m := extraUsedRe.FindStringSubmatch(txt); len(m) > 2 {
		if n, err := strconv.Atoi(m[1]); err == nil {
			out["extra_used_pct"] = n
		}
		out["extra_left"] = strings.TrimSpace(m[2])
	}
	if strings.Contains(txt, "Never expires") {
		out["extra_expires"] = "never"
	}
	_, hasPlan := out["plan"]
	_, hasWeekly := out["weekly_used_pct"]
	out["found"] = hasPlan || hasWeekly
	return out
}

const AttJS = `(function(){` +
	`var list = []; var seen = new Set();` +
	`function addEl(el, tid){` +
	`  if(!el || seen.has(el)) return;` +
	`  seen.add(el);` +
	`  if(el.closest('form, [class*=chat-user-bubble], [class*="group/msg"]')) return;` +
	`  var v = el.querySelector('video') || (el.tagName === 'VIDEO' ? el : null);` +
	`  var img = el.querySelector('img') || (el.tagName === 'IMG' ? el : null);` +
	`  var isVid = (tid || '').includes('video') || !!v;` +
	`  var primary = isVid ? (v || img) : (img || v);` +
	`  var src = primary ? (primary.currentSrc || primary.src || '') : '';` +
	`  if(src && !seen.has(src)){` +
	`    seen.add(src);` +
	`    list.push({` +
	`      tid: tid || el.getAttribute('data-testid') || (isVid ? 'video' : 'image'),` +
	`      hasVideo: !!v,` +
	`      hasImg: !!img,` +
	`      src: src,` +
	`      vSrc: v ? (v.currentSrc || v.src || '') : '',` +
	`      iSrc: img ? (img.currentSrc || img.src || '') : '',` +
	`      w: primary ? (primary.videoWidth || primary.naturalWidth || 0) : 0,` +
	`      h: primary ? (primary.videoHeight || primary.naturalHeight || 0) : 0` +
	`    });` +
	`  }` +
	`}` +
	`document.querySelectorAll('[data-testid^="hatch-chat-attachment-presentation-"]').forEach(function(a){ addEl(a, a.getAttribute('data-testid')); });` +
	`document.querySelectorAll('div[class*="hatch-agent-bubble-bg"] img, div[class*="hatch-agent-bubble-bg"] video').forEach(function(m){` +
	`  var s = m.currentSrc || m.src || '';` +
	`  if(s && !s.includes('avatar') && !s.includes('emoji')) addEl(m.parentElement || m, 'agent-media');` +
	`});` +
	`return JSON.stringify(list);` +
	`})()`

func (e *MuseEngine) Attachments(accountID string) []map[string]any {
	if e.AttachmentsFunc != nil {
		return e.AttachmentsFunc()
	}
	p := e.getPage(accountID)
	if p == nil {
		return nil
	}
	raw := p.JSStr(AttJS)
	if raw == "" {
		return nil
	}
	var out []map[string]any
	_ = json.Unmarshal([]byte(raw), &out)
	return out
}

func (e *MuseEngine) sendPrompt(page *CDP, prompt string) (string, error) {
	_, _ = page.JS(`(function(){
        var ta = document.querySelector('textarea');
        if (ta) {
            ta.scrollIntoView({block: 'center', inline: 'nearest'});
            ta.focus();
        }
    })()`, false, 0)
	sleepSec(0.1)

	rect := page.JSStr(`(function(){var t=document.querySelector('textarea');if(!t)return null;` +
		`var r=t.getBoundingClientRect();` +
		`return JSON.stringify({x:Math.round(r.left+r.width/2),` +
		`y:Math.round(r.top+r.height/2)});})()`)
	if rect == "" {
		return "", &MuseGenerationError{Msg: "Chat input box not found"}
	}
	var c struct {
		X int `json:"x"`
		Y int `json:"y"`
	}
	if err := json.Unmarshal([]byte(rect), &c); err != nil {
		return "", &MuseGenerationError{Msg: "Chat input box not found"}
	}
	e.clickPoint(page, c.X, c.Y)
	sleepSec(0.1)

	setterFmt := `(function(t){var ta=document.querySelector('textarea');` +
		`if(!ta) return 0;` +
		`ta.focus();` +
		`var s=Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype,'value').set;` +
		`s.call(ta,t);` +
		`ta.dispatchEvent(new Event('input',{bubbles:true}));` +
		`ta.dispatchEvent(new Event('change',{bubbles:true}));` +
		`return (ta.value||'').length;})(%s)`
	promptJSON, _ := json.Marshal(prompt)
	valRaw, _ := page.JS(fmt.Sprintf(setterFmt, string(promptJSON)), false, 0)
	valLen, _ := toInt64(valRaw)
	if valLen == 0 && len(prompt) < 500 {
		_, _ = page.Send("Input.insertText", map[string]any{"text": prompt}, 10*time.Second)
		_, _ = page.JS(fmt.Sprintf(setterFmt, string(promptJSON)), false, 0)
	}

	clicked := "no-button"
	tDeadline := nowFloat() + 3.0
	for nowFloat() < tDeadline {
		res := page.JSStr(`(function(){var b=[...document.querySelectorAll('button,[role=button]')]` +
			`.filter(function(x){return x.offsetParent!==null;})` +
			`.find(function(x){return /发送|send/i.test(x.getAttribute('aria-label')||'')` +
			`||/发送|send/i.test(x.getAttribute('data-testid')||'')` +
			`||/send/i.test(x.innerText||'');});` +
			`if(!b)return 'no-button';` +
			`if(b.disabled)return 'disabled';` +
			`b.click();return 'clicked';})()`)
		if res == "clicked" {
			clicked = "clicked"
			break
		}
		sleepSec(0.08)
	}

	if clicked != "clicked" {
		for _, mod := range []int{1, 0} {
			for _, t := range []string{"keyDown", "char", "keyUp"} {
				params := map[string]any{
					"type":                  t,
					"key":                   "Enter",
					"code":                  "Enter",
					"windowsVirtualKeyCode": 13,
					"nativeVirtualKeyCode":  13,
					"modifiers":             mod,
				}
				if t == "char" {
					params["text"] = "\r"
					params["unmodifiedText"] = "\r"
				}
				_, _ = page.Send("Input.dispatchKeyEvent", params, 5*time.Second)
			}
			sleepSec(1.0)
			if page.JSBool(`!(document.querySelector('textarea')||{value:''}).value`) {
				clicked = "enter-sent"
				break
			}
		}
	}
	sleepSec(0.3)
	return clicked, nil
}

func (e *MuseEngine) scrollBottom(page *CDP) {
	if e.ScrollBottomFn != nil {
		e.ScrollBottomFn()
		return
	}
	if page == nil {
		return
	}
	_, _ = page.JS(`(function(){`+
		`var els=[...document.querySelectorAll('*')].filter(function(e){`+
		`var s=getComputedStyle(e);`+
		`return (s.overflowY==='auto'||s.overflowY==='scroll')`+
		`&&e.scrollHeight>e.clientHeight+100;});`+
		`els.sort(function(a,b){return b.scrollHeight-a.scrollHeight;});`+
		`if(els[0])els[0].scrollTop=els[0].scrollHeight;`+
		`var s=document.scrollingElement||document.body;`+
		`s.scrollTop=s.scrollHeight;`+
		`var el=document.querySelector('textarea');`+
		`if(el)el.scrollIntoView({block:'end'});return 1;})()`, false, 0)
}

var (
	outOfCreditsRe = regexp.MustCompile(`(?i)额度不足|积分不足|out of credits|达到上限|token limit`)
	stillSendingRe = regexp.MustCompile(`(?i)Still sending|Delivery not confirmed|正在发送|未确认送达`)
	mediaReportRe  = regexp.MustCompile(`(?i)\.(?:webp|png|jpe?g|mp4|webm)|imagine_media|deliverable|generated\s+.*image|verified\s+generated|artifact`)
)

func hasVideoExt(s string) bool {
	s = strings.ToLower(s)
	return strings.HasSuffix(s, ".mp4") || strings.HasSuffix(s, ".webm") || strings.HasSuffix(s, ".mov")
}

func (e *MuseEngine) WaitAttachment(accountID, baselineSrc string, timeoutSec float64, expect string, onProgress func(int), baseAgentCnt, baseAttCnt int, stopCh <-chan struct{}, baselineSources map[string]bool) (map[string]any, error) {
	baseSet := map[string]bool{baselineSrc: true}
	for k, v := range baselineSources {
		if v {
			baseSet[k] = true
		}
	}
	p := e.getPage(accountID)
	tStart := nowFloat()
	deadline := tStart + timeoutSec
	stableSrc, stableN := "", 0
	lastTxt, txtStable := "", 0

	for nowFloat() < deadline {
		if stopCh != nil {
			select {
			case <-stopCh:
				return nil, &MuseGenerationError{Msg: "Client disconnected; generation aborted"}
			default:
			}
		}
		sleepSec(0.6)
		e.scrollBottom(p)
		atts := e.Attachments(accountID)

		var matchingAtts []map[string]any
		for _, a := range atts {
			tid := strings.ToLower(fmt.Sprint(a["tid"]))
			hasVideo, _ := a["hasVideo"].(bool)
			src, _ := a["src"].(string)
			vSrc, _ := a["vSrc"].(string)
			var isMatch bool
			if expect == "video" {
				isMatch = hasVideo || vSrc != "" || hasVideoExt(src)
			} else {
				isMatch = strings.Contains(tid, "image") || (!hasVideo && !hasVideoExt(src))
			}
			if isMatch {
				matchingAtts = append(matchingAtts, a)
			}
		}

		if len(matchingAtts) > 0 {
			att := matchingAtts[len(matchingAtts)-1]
			src, _ := att["src"].(string)
			vSrc, _ := att["vSrc"].(string)
			w, _ := toInt64(att["w"])
			h, _ := toInt64(att["h"])
			checkSrc := src
			if expect == "video" {
				if vSrc != "" {
					checkSrc = vSrc
				} else if hasVideoExt(src) {
					checkSrc = src
				} else {
					checkSrc = ""
				}
			}
			if checkSrc != "" && !baseSet[checkSrc] {
				if w > 0 && h > 0 {
					return att, nil
				}
				if checkSrc == stableSrc {
					stableN++
				} else {
					stableSrc, stableN = checkSrc, 0
				}
				if stableN >= 1 {
					return att, nil
				}
			}
		}

		elapsed := nowFloat() - tStart
		if onProgress != nil {
			prog := int(25 + elapsed*1.0)
			if prog > 92 {
				prog = 92
			}
			onProgress(prog)
		}

		var st map[string]any
		if p != nil {
			stRaw := p.JSStr(`(function(){
                var bs=[].slice.call(document.querySelectorAll('div[class*="hatch-chat-groupable-bubble"]'))
                    .filter(function(b){return /hatch-agent-bubble-bg/.test(b.className||'');});
                var lastTxt = bs.length ? (bs[bs.length-1].innerText||'').trim() : '';
                var hasStop = !!(document.querySelector('[data-testid="hatch-composer-stop-button"]')
                    || document.querySelector('button[aria-label*="Stop" i]')
                    || document.querySelector('button[aria-label*="停止"]'));
                var tail = document.body ? (document.body.innerText||'').slice(-700) : '';
                return JSON.stringify({cnt: bs.length, txt: lastTxt, stop: hasStop, tail: tail});
            })()`)
			if stRaw != "" {
				_ = json.Unmarshal([]byte(stRaw), &st)
			}
		}
		tail, _ := st["tail"].(string)
		if outOfCreditsRe.MatchString(tail) {
			return nil, &MuseGenerationError{Msg: "Account out of credits"}
		}
		curCnt64, _ := toInt64(st["cnt"])
		curCnt := int(curCnt64)
		if elapsed > 30.0 && curCnt == baseAgentCnt && stillSendingRe.MatchString(tail) {
			return nil, &MuseGenerationError{Msg: "Prompt delivery not confirmed (Still sending / Delivery not confirmed); aborting and retrying"}
		}
		curTxt, _ := st["txt"].(string)
		hasStop, _ := st["stop"].(bool)
		if curCnt > baseAgentCnt && curTxt != "" && !hasStop && len(atts) <= baseAttCnt {
			if !mediaReportRe.MatchString(curTxt) {
				if curTxt == lastTxt {
					txtStable++
				} else {
					lastTxt, txtStable = curTxt, 0
				}
				if txtStable >= 15 && elapsed > 8.0 {
					preview := curTxt
					if len(preview) > 120 {
						preview = preview[:120]
					}
					return nil, &MuseGenerationError{Msg: fmt.Sprintf("No media attachment produced; only text reply returned: %s", preview)}
				}
			} else {
				txtStable = 0
			}
		} else {
			txtStable = 0
		}
	}
	return nil, nil
}

const ExtractJSFmt = `(async function(src, expect){
  try{
    var u = src;
    if(!u) return JSON.stringify({ok:false,err:'no-media-src'});
    var r = await fetch(u);
    if(!r.ok) return JSON.stringify({ok:false,err:'media-http-'+r.status});
    var b = await r.blob();
    var ab = await b.arrayBuffer();
    var bytes = new Uint8Array(ab);
    var s = '';
    for(var i=0; i<bytes.length; i+=65536){
      s += String.fromCharCode.apply(null, bytes.subarray(i, i+65536));
    }
    return JSON.stringify({ok:true, mime:b.type||'', size:b.size, url:u, b64:btoa(s)});
  }catch(e){
    return JSON.stringify({ok:false, err:String(e)});
  }
})(%s, %s)`

const (
	AgentCountJS = `(function(){` +
		`var bs=[].slice.call(document.querySelectorAll('div[class*="hatch-chat-groupable-bubble"]'));` +
		`var n=0;for(var i=0;i<bs.length;i++){` +
		`if(/hatch-agent-bubble-bg/.test(bs[i].className||''))n++;}` +
		`return String(n);})()`

	PollChatJS = `(function(){` +
		`var els=[...document.querySelectorAll('*')].filter(function(e){` +
		`var s=getComputedStyle(e);` +
		`return (s.overflowY==='auto'||s.overflowY==='scroll')&&e.scrollHeight>e.clientHeight+100;});` +
		`els.sort(function(a,b){return b.scrollHeight-a.scrollHeight;});` +
		`if(els[0])els[0].scrollTop=els[0].scrollHeight;` +
		`var scope=document.querySelector('main,[class*="chat-scroll"],[class*="hatch-chat-scroll"]')||document.body;` +
		`var bs=[].slice.call(scope.querySelectorAll('div[class*="hatch-chat-groupable-bubble"]'))` +
		`.filter(function(b){return /hatch-agent-bubble-bg/.test(b.className||'');});` +
		`var nonEmpty=bs.filter(function(b){return ((b.innerText||'').trim().length)>0;});` +
		`var txt=nonEmpty.length?(nonEmpty[nonEmpty.length-1].innerText||'').trim():'';` +
		`var stop=!!(document.querySelector('[data-testid="hatch-composer-stop-button"]')` +
		`||document.querySelector('button[aria-label*="Stop" i]')` +
		`||document.querySelector('button[aria-label*="停止"]'));` +
		`return JSON.stringify({cnt:nonEmpty.length,total:bs.length,txt:txt,stop:stop});})()`
)

func (e *MuseEngine) agentCount(page *CDP) int {
	if page == nil {
		return 0
	}
	s := page.JSStr(AgentCountJS)
	n, _ := strconv.Atoi(s)
	return n
}

func (e *MuseEngine) pollChat(page *CDP) (int, string, bool) {
	if page == nil {
		return 0, "", false
	}
	raw := page.JSStr(PollChatJS)
	if raw == "" {
		return 0, "", false
	}
	var d map[string]any
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		return 0, "", false
	}
	cnt, _ := toInt64(d["cnt"])
	txt, _ := d["txt"].(string)
	stop, _ := d["stop"].(bool)
	return int(cnt), strings.TrimSpace(txt), stop
}

func (e *MuseEngine) ChatStream(cookies map[string]string, prompt string, expires map[string]int64, timeout int, accountID string, stopCh <-chan struct{}, onChunk func(string)) error {
	if timeout <= 0 {
		timeout = 300
		if e.cfg != nil && e.cfg.ChatTimeout > 0 {
			timeout = e.cfg.ChatTimeout
		}
	}
	page, err := e.EnsurePage(cookies, expires, accountID)
	if err != nil {
		return err
	}
	e.ResetThread(accountID, true)
	baseAgent, baseText, _ := e.pollChat(page)
	if _, err := e.sendPrompt(page, prompt); err != nil {
		return err
	}

	tSent := nowFloat()
	deadline := tSent + float64(timeout)
	firstTokenDeadline := deadline
	if tSent+300.0 < firstTokenDeadline {
		firstTokenDeadline = tSent + 300.0
	}
	sent, last := "", ""
	hasLast := false
	stable := 0
	gotFirst := false

	for nowFloat() < firstTokenDeadline {
		if stopCh != nil {
			select {
			case <-stopCh:
				return nil
			default:
			}
		}
		sleepSec(0.06)
		cnt, cur, _ := e.pollChat(page)
		if cur == "" || (cnt <= baseAgent && cur == baseText) {
			if nowFloat()-tSent > 240.0 {
				tail := page.JSStr("document.body.innerText.slice(-500)")
				if strings.Contains(tail, "Still sending") || strings.Contains(tail, "Connecting...") {
					return &MuseGenerationError{Msg: "Cloud VM connection timed out (Still sending)"}
				}
			}
			continue
		}
		delta := cur
		if strings.HasPrefix(cur, sent) {
			delta = cur[len(sent):]
		}
		if delta != "" {
			sent = cur
			if onChunk != nil {
				onChunk(delta)
			}
			last = cur
			hasLast = true
			gotFirst = true
			break
		}
	}

	if !gotFirst {
		return &MuseGenerationError{Msg: "Timed out waiting for first assistant token"}
	}

	for nowFloat() < deadline {
		if stopCh != nil {
			select {
			case <-stopCh:
				return nil
			default:
			}
		}
		sleepSec(0.06)
		_, cur, hasStop := e.pollChat(page)
		if cur == "" {
			continue
		}
		if !hasLast || cur != last {
			delta := cur
			if strings.HasPrefix(cur, sent) {
				delta = cur[len(sent):]
			}
			if delta != "" {
				sent = cur
				if onChunk != nil {
					onChunk(delta)
				}
			}
			last = cur
			hasLast = true
			stable = 0
		} else {
			stable++
			if !hasStop && stable >= 3 {
				return nil
			}
			if hasStop && stable >= 80 {
				return nil
			}
		}
	}
	return &MuseGenerationError{Msg: "Timed out waiting for assistant reply"}
}

func (e *MuseEngine) ExtractBytes(page *CDP, src, expect string, retries int) ([]byte, string, string, error) {
	if retries <= 0 {
		retries = 4
	}
	srcJSON, _ := json.Marshal(src)
	expJSON, _ := json.Marshal(expect)
	expr := fmt.Sprintf(ExtractJSFmt, string(srcJSON), string(expJSON))
	last := "unknown"
	for i := 0; i < retries; i++ {
		raw, err := page.JS(expr, true, 600*time.Second)
		var info map[string]any
		if err == nil {
			switch v := raw.(type) {
			case string:
				_ = json.Unmarshal([]byte(v), &info)
			case map[string]any:
				info = v
			}
		}
		if info != nil {
			if ok, _ := info["ok"].(bool); ok {
				b64Str, _ := info["b64"].(string)
				data, decErr := base64.StdEncoding.DecodeString(b64Str)
				if decErr == nil {
					mType, _ := info["mime"].(string)
					uStr, _ := info["url"].(string)
					return data, mType, uStr, nil
				}
			}
			if errMsg, ok := info["err"].(string); ok && errMsg != "" {
				last = errMsg
			}
		}
		sleepSec(2.0)
	}
	return nil, "", "", &MuseGenerationError{Msg: fmt.Sprintf("Failed to retrieve generated media: %s", last)}
}

func (e *MuseEngine) DownloadFallback(accountID, src string, timeoutSec float64) string {
	p := e.getPage(accountID)
	if p == nil {
		return ""
	}
	dlDir := ""
	if e.cfg != nil {
		dlDir = e.cfg.DownloadDir()
	}
	before := map[string]bool{}
	if entries, err := os.ReadDir(dlDir); err == nil {
		for _, ent := range entries {
			before[ent.Name()] = true
		}
	}
	srcJSON, _ := json.Marshal(src)
	clicked := p.JSStr(fmt.Sprintf(`(function(src){
        var media=[...document.querySelectorAll('img,video')]
            .find(m=>(m.currentSrc||m.src||'')===src);
        var node=media && media.closest('[data-testid^="hatch-chat-attachment-presentation-"]');
        if(!node) return 'none';
        node=node.closest('[class*="group/widget-presentation"]')||node;
        var b=[...node.querySelectorAll('button,[role=button]')]
            .find(x=>/下载|保存|download/i.test(x.getAttribute('aria-label')||''));
        if(!b) return 'none';
        b.click();return 'ok';
    })(%s)`, string(srcJSON)))
	if clicked == "none" || clicked == "" {
		return ""
	}
	deadline := nowFloat() + timeoutSec
	for nowFloat() < deadline {
		sleepSec(1.5)
		entries, err := os.ReadDir(dlDir)
		if err != nil {
			continue
		}
		var newestPath string
		var newestMod time.Time
		for _, ent := range entries {
			name := ent.Name()
			if before[name] || strings.HasSuffix(name, ".crdownload") {
				continue
			}
			fp := filepath.Join(dlDir, name)
			if st, err := os.Stat(fp); err == nil && st.Size() > 0 {
				if newestPath == "" || st.ModTime().After(newestMod) {
					newestPath = fp
					newestMod = st.ModTime()
				}
			}
		}
		if newestPath != "" {
			return newestPath
		}
	}
	return ""
}

func (e *MuseEngine) NormalizeImage(img string) (string, string) {
	if e.NormalizeImgFn != nil {
		return e.NormalizeImgFn(img)
	}
	img = strings.TrimSpace(img)
	if img == "" {
		return "", "image/png"
	}
	if strings.HasPrefix(img, "data:") {
		parts := strings.SplitN(img, ",", 2)
		mType := "image/png"
		if strings.Contains(parts[0], ";") {
			mType = strings.TrimSpace(strings.TrimPrefix(strings.SplitN(parts[0], ";", 2)[0], "data:"))
		}
		if len(parts) > 1 {
			return strings.TrimSpace(parts[1]), mType
		}
		return "", mType
	}
	if strings.HasPrefix(img, "http://") || strings.HasPrefix(img, "https://") {
		req, err := http.NewRequest(http.MethodGet, img, nil)
		if err != nil {
			return "", "image/png"
		}
		req.Header.Set("User-Agent", "Mozilla/5.0")
		resp, err := httpDo(req, 20*time.Second, true)
		if err != nil {
			log.Printf("[WARN] Failed to download remote reference image: %v", err)
			return "", "image/png"
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", "image/png"
		}
		mType := strings.SplitN(resp.Header.Get("Content-Type"), ";", 2)[0]
		if mType == "" {
			mType = "image/png"
		}
		return base64.StdEncoding.EncodeToString(data), mType
	}
	if st, err := os.Stat(img); err == nil && !st.IsDir() {
		data, err := os.ReadFile(img)
		if err != nil {
			return "", "image/png"
		}
		mType := mime.TypeByExtension(filepath.Ext(img))
		if mType == "" {
			mType = "image/png"
		}
		return base64.StdEncoding.EncodeToString(data), mType
	}
	return img, "image/png"
}

func (e *MuseEngine) ClearAttachments(accountID string) int {
	p := e.getPage(accountID)
	if p == nil {
		return 0
	}
	v, _ := p.JS(`(function(){`+
		`var btns=Array.from(document.querySelectorAll('button[aria-label]')).filter(function(b){`+
		`var al=b.getAttribute('aria-label')||'';`+
		`return /remove\s*attachment|移除附件|删除附件|移除|删除/i.test(al);`+
		`});`+
		`btns.forEach(function(b){b.click();});`+
		`Array.from(document.querySelectorAll('input[type="file"]')).forEach(function(inp){inp.value='';});`+
		`return btns.length;`+
		`})()`, false, 0)
	sleepSec(0.3)
	n, _ := toInt64(v)
	return int(n)
}

const InjectImageJSFmt = `(function(b64, mime) {
    try {
        var byteChars = atob(b64);
        var byteNumbers = new Array(byteChars.length);
        for (var i = 0; i < byteChars.length; i++) {
            byteNumbers[i] = byteChars.charCodeAt(i);
        }
        var byteArray = new Uint8Array(byteNumbers);
        var blob = new Blob([byteArray], {type: mime});
        var ext = mime.split('/')[1] || 'png';
        if (ext === 'jpeg') ext = 'jpg';
        var file = new File([blob], 'reference_image.' + ext, {type: mime});
        var inputs = Array.from(document.querySelectorAll('input[type="file"]'));
        if (!inputs.length) return JSON.stringify({ok: false, err: 'no-file-input'});
        var ov = document.querySelector('[data-testid="hatch-composer-placeholder-overlay"]');
        var composer = ov ? ov.closest('form') : null;
        var input = null;
        if (composer) {
            input = inputs.find(function(x){ return composer.contains(x); }) || null;
        }
        if (!input) {
            input = inputs.find(function(x){
                return !x.closest('[class*=chat-user-bubble], [class*="group/msg"]');
            }) || inputs[0];
        }
        if (!input.getAttribute('accept')) input.setAttribute('accept', 'image/*');
        var dt = new DataTransfer();
        dt.items.add(file);
        input.files = dt.files;
        input.dispatchEvent(new Event('change', {bubbles: true}));
        input.dispatchEvent(new Event('input', {bubbles: true}));
        return JSON.stringify({ok: true});
    } catch(e) {
        return JSON.stringify({ok: false, err: String(e)});
    }
})(%s, %s)`

func (e *MuseEngine) AttachImage(accountID, imageData string) error {
	if imageData == "" {
		return nil
	}
	b64, mType := e.NormalizeImage(imageData)
	if b64 == "" {
		return &MuseGenerationError{Msg: "Failed to read reference image; generation aborted"}
	}
	e.ClearAttachments(accountID)
	p := e.getPage(accountID)
	if p == nil {
		return &MuseGenerationError{Msg: "Failed to attach reference image; generation aborted"}
	}
	b64JSON, _ := json.Marshal(b64)
	mimeJSON, _ := json.Marshal(mType)
	rawRes := p.JSStr(fmt.Sprintf(InjectImageJSFmt, string(b64JSON), string(mimeJSON)))
	var resObj map[string]any
	if err := json.Unmarshal([]byte(rawRes), &resObj); err != nil || resObj["ok"] != true {
		return &MuseGenerationError{Msg: "Failed to attach reference image; generation aborted"}
	}

	deadline := nowFloat() + 15.0
	confirmed := false
	for nowFloat() < deadline {
		hasAttached := p.JSStr(`(function(){
            var btns = Array.from(document.querySelectorAll('button[aria-label]'));
            var hasRemove = btns.some(function(b){
                return /remove\s*attachment|移除附件|删除附件/i.test(b.getAttribute('aria-label') || '');
            });
            if (hasRemove) return 'remove';
            var ov = document.querySelector('[data-testid="hatch-composer-placeholder-overlay"]');
            var composer = ov ? (ov.closest('form') || ov.parentElement.parentElement.parentElement) : null;
            if (composer) {
                var media = composer.querySelectorAll('img, video');
                if (media.length > 0) return 'thumb';
            }
            return '';
        })()`)
		if hasAttached != "" {
			confirmed = true
			break
		}
		sleepSec(0.3)
	}
	if !confirmed {
		return &MuseGenerationError{Msg: "Reference image upload not confirmed; generation aborted"}
	}
	sleepSec(0.5)
	return nil
}

func pickExt(mType, rawURL, expect string) string {
	m := strings.ToLower(mType)
	for _, pair := range [][2]string{
		{"mp4", ".mp4"}, {"webm", ".webm"}, {"png", ".png"},
		{"jpeg", ".jpg"}, {"jpg", ".jpg"}, {"webp", ".webp"}, {"gif", ".gif"},
	} {
		if strings.Contains(m, pair[0]) {
			return pair[1]
		}
	}
	u := strings.ToLower(rawURL)
	for _, ext := range []string{".mp4", ".webm", ".png", ".jpg", ".webp"} {
		if strings.Contains(u, ext) {
			return ext
		}
	}
	if expect == "video" {
		return ".mp4"
	}
	return ".png"
}

func (e *MuseEngine) Generate(cookies map[string]string, prompt, expect string, timeout int, expires map[string]int64, accountID string, onProgress func(int), referenceImage string, stopCh <-chan struct{}) (map[string]any, error) {
	if e.GenerateFunc != nil {
		return e.GenerateFunc(cookies, prompt, expect, timeout, expires, accountID, onProgress, referenceImage, stopCh)
	}
	if expect == "" {
		expect = "image"
	}
	if timeout <= 0 {
		timeout = 240
	}
	deadline := nowFloat() + float64(timeout)
	page, err := e.EnsurePage(cookies, expires, accountID)
	if err != nil {
		return nil, err
	}
	e.ResetThread(accountID, false)
	e.scrollBottom(page)
	if referenceImage != "" {
		if err := e.AttachImage(accountID, referenceImage); err != nil {
			return nil, err
		}
	} else {
		e.ClearAttachments(accountID)
	}

	attsBefore := e.Attachments(accountID)
	baselineSources := map[string]bool{}
	for _, a := range attsBefore {
		for _, k := range []string{"src", "vSrc", "iSrc"} {
			if s, ok := a[k].(string); ok && s != "" {
				baselineSources[s] = true
			}
		}
	}
	baselineSrc := ""
	if len(attsBefore) > 0 {
		baselineSrc, _ = attsBefore[len(attsBefore)-1]["src"].(string)
	}
	baseAgentCnt := e.agentCount(page)
	if nowFloat() >= deadline {
		return nil, &MuseGenerationError{Msg: "Task preparation timed out; send aborted"}
	}
	clicked, err := e.sendPrompt(page, prompt)
	if err != nil {
		return nil, err
	}
	if clicked != "clicked" && clicked != "enter-sent" {
		return nil, &MuseGenerationError{Msg: "Prompt send not confirmed; generation aborted"}
	}

	rem := deadline - nowFloat()
	if rem < 0 {
		rem = 0
	}
	att, err := e.WaitAttachment(accountID, baselineSrc, rem, expect, onProgress, baseAgentCnt, len(attsBefore), stopCh, baselineSources)
	if err != nil {
		return nil, err
	}
	if att == nil {
		return nil, &MuseGenerationError{Msg: "Generation timed out; no new media attachment appeared"}
	}

	_ = os.MkdirAll(e.cfg.MediaDir(), 0o755)
	selectedSrc, _ := att["src"].(string)
	if expect == "video" {
		if vSrc, _ := att["vSrc"].(string); vSrc != "" {
			selectedSrc = vSrc
		}
	}

	data, mType, uStr, _ := e.ExtractBytes(page, selectedSrc, expect, 4)
	if len(data) > 0 {
		ext := pickExt(mType, uStr, expect)
		name := randomHex(32) + ext
		dst := filepath.Join(e.cfg.MediaDir(), name)
		if err := os.WriteFile(dst, data, 0o644); err != nil {
			return nil, &MuseGenerationError{Msg: fmt.Sprintf("Failed to write media file: %v", err)}
		}
		kind := "image"
		if hasVideoExt(ext) {
			kind = "video"
		}
		if expect == "video" && kind != "video" {
			return nil, &MuseGenerationError{Msg: fmt.Sprintf("Expected video output, but extracted attachment is an image (%s)", ext)}
		}
		return map[string]any{
			"path":       dst,
			"filename":   name,
			"size":       len(data),
			"ext":        ext,
			"mime":       mType,
			"kind":       kind,
			"via":        "blob",
			"attachment": att["tid"],
			"w":          att["w"],
			"h":          att["h"],
		}, nil
	}

	dlPath := e.DownloadFallback(accountID, selectedSrc, 180)
	if dlPath == "" {
		return nil, &MuseGenerationError{Msg: "Media generated but file download failed"}
	}
	ext := strings.ToLower(filepath.Ext(dlPath))
	if ext == "" {
		ext = ".bin"
	}
	name := randomHex(32) + ext
	dst := filepath.Join(e.cfg.MediaDir(), name)
	_ = os.Rename(dlPath, dst)
	kind := "image"
	if hasVideoExt(ext) {
		kind = "video"
	}
	if expect == "video" && kind != "video" {
		return nil, &MuseGenerationError{Msg: fmt.Sprintf("Expected video output, but downloaded attachment is an image (%s)", ext)}
	}
	st, _ := os.Stat(dst)
	sz := 0
	if st != nil {
		sz = int(st.Size())
	}
	return map[string]any{
		"path":       dst,
		"filename":   name,
		"size":       sz,
		"ext":        ext,
		"mime":       "",
		"kind":       kind,
		"via":        "download",
		"attachment": att["tid"],
		"w":          att["w"],
		"h":          att["h"],
	}, nil
}
