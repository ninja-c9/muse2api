//go:build ignore

// muse2api Cookie Helper — Launches an isolated browser window, waits for muse.ai login,
// extracts session cookies via CDP, and uploads them to your muse2api server.
//
// Usage:
//   go run get_muse_cookie.go --base http://your-server-ip:18610 --key m2a_xxx --label acc-01
package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	siteURL    = "https://muse.ai/"
	defaultCDP = 9333
	domainHint = "muse.ai"
)

var essentialCookies = []string{"hatch_sess", "hatch_gw", "hatch_vml", "hatch_native_auth_device"}

type wsClient struct {
	conn net.Conn
	br   *bufio.Reader
	id   int64
}

func dialWS(rawURL string, timeout time.Duration) (*wsClient, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	host := u.Host
	if !strings.Contains(host, ":") {
		host += ":80"
	}
	path := u.RequestURI()
	if path == "" {
		path = "/"
	}
	conn, err := net.DialTimeout("tcp", host, timeout)
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(timeout))
	var keyBytes [16]byte
	_, _ = rand.Read(keyBytes[:])
	key := base64.StdEncoding.EncodeToString(keyBytes[:])

	req := fmt.Sprintf("GET %s HTTP/1.1\r\n"+
		"Host: %s\r\n"+
		"Upgrade: websocket\r\n"+
		"Connection: Upgrade\r\n"+
		"Sec-WebSocket-Key: %s\r\n"+
		"Sec-WebSocket-Version: 13\r\n\r\n", path, host, key)
	if _, err := conn.Write([]byte(req)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	if err != nil || !strings.Contains(status, " 101") {
		_ = conn.Close()
		return nil, fmt.Errorf("WebSocket handshake failed: %s", strings.TrimSpace(status))
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		if line == "\r\n" || line == "\n" {
			break
		}
	}
	return &wsClient{conn: conn, br: br}, nil
}

func (w *wsClient) writeFrame(opcode byte, payload []byte) error {
	var head []byte
	head = append(head, 0x80|opcode)
	n := len(payload)
	if n < 126 {
		head = append(head, byte(0x80|n))
	} else if n < 65536 {
		head = append(head, 0x80|126, byte(n>>8), byte(n))
	} else {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(n))
		head = append(head, 0x80|127)
		head = append(head, b[:]...)
	}
	var mask [4]byte
	_, _ = rand.Read(mask[:])
	head = append(head, mask[:]...)
	buf := make([]byte, len(head)+n)
	copy(buf, head)
	for i := 0; i < n; i++ {
		buf[len(head)+i] = payload[i] ^ mask[i%4]
	}
	_, err := w.conn.Write(buf)
	return err
}

func (w *wsClient) recvText() (string, error) {
	var msg []byte
	for {
		var h [2]byte
		if _, err := io.ReadFull(w.br, h[:]); err != nil {
			return "", err
		}
		fin := (h[0] & 0x80) != 0
		opcode := h[0] & 0x0F
		masked := (h[1] & 0x80) != 0
		ln := int64(h[1] & 0x7F)
		if ln == 126 {
			var ext [2]byte
			if _, err := io.ReadFull(w.br, ext[:]); err != nil {
				return "", err
			}
			ln = int64(binary.BigEndian.Uint16(ext[:]))
		} else if ln == 127 {
			var ext [8]byte
			if _, err := io.ReadFull(w.br, ext[:]); err != nil {
				return "", err
			}
			ln = int64(binary.BigEndian.Uint64(ext[:]))
		}
		var mask [4]byte
		if masked {
			if _, err := io.ReadFull(w.br, mask[:]); err != nil {
				return "", err
			}
		}
		data := make([]byte, ln)
		if ln > 0 {
			if _, err := io.ReadFull(w.br, data); err != nil {
				return "", err
			}
		}
		if masked {
			for i := range data {
				data[i] ^= mask[i%4]
			}
		}
		switch opcode {
		case 0x8:
			return "", io.EOF
		case 0x9:
			_ = w.writeFrame(0xA, data)
		case 0x1, 0x0:
			msg = append(msg, data...)
			if fin {
				return string(msg), nil
			}
		}
	}
}

func (w *wsClient) call(method string, params map[string]any, timeout time.Duration) (map[string]any, error) {
	w.id++
	mid := w.id
	if params == nil {
		params = map[string]any{}
	}
	b, _ := json.Marshal(map[string]any{"id": mid, "method": method, "params": params})
	deadline := time.Now().Add(timeout)
	_ = w.conn.SetDeadline(deadline)
	if err := w.writeFrame(0x1, b); err != nil {
		return nil, err
	}
	for time.Now().Before(deadline) {
		raw, err := w.recvText()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return nil, err
		}
		var m map[string]any
		if json.Unmarshal([]byte(raw), &m) != nil {
			continue
		}
		if idF, ok := m["id"].(float64); ok && int64(idF) == mid {
			if errObj, hasErr := m["error"]; hasErr && errObj != nil {
				return nil, fmt.Errorf("%s: %v", method, errObj)
			}
			res, _ := m["result"].(map[string]any)
			return res, nil
		}
	}
	return nil, fmt.Errorf("CDP %s timed out", method)
}

func (w *wsClient) close() {
	if w.conn != nil {
		_ = w.conn.Close()
	}
}

func findChrome() string {
	var cands []string
	switch runtime.GOOS {
	case "windows":
		roots := []string{
			os.Getenv("ProgramFiles"),
			os.Getenv("ProgramFiles(x86)"),
			os.Getenv("LOCALAPPDATA"),
			`C:\Program Files`,
			`C:\Program Files (x86)`,
		}
		rels := []string{
			`Google\Chrome\Application\chrome.exe`,
			`Microsoft\Edge\Application\msedge.exe`,
			`BraveSoftware\Brave-Browser\Application\brave.exe`,
		}
		for _, r := range roots {
			if r == "" {
				continue
			}
			for _, rel := range rels {
				cands = append(cands, filepath.Join(r, rel))
			}
		}
	case "darwin":
		cands = []string{
			"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
			"/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
			"/Applications/Chromium.app/Contents/MacOS/Chromium",
			"/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
		}
	default:
		cands = []string{"google-chrome", "google-chrome-stable", "chromium", "chromium-browser", "microsoft-edge", "brave-browser"}
	}
	for _, c := range cands {
		if c == "" {
			continue
		}
		if st, err := os.Stat(c); err == nil && !st.IsDir() {
			return c
		}
		if !filepath.IsAbs(c) {
			if p, err := exec.LookPath(c); err == nil {
				return p
			}
		}
	}
	return ""
}

func waitCDP(port int, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	u := fmt.Sprintf("http://127.0.0.1:%d/json/version", port)
	client := &http.Client{Timeout: 2 * time.Second}
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := client.Get(u)
		if err == nil {
			var obj map[string]any
			_ = json.NewDecoder(resp.Body).Decode(&obj)
			_ = resp.Body.Close()
			if wsURL, ok := obj["webSocketDebuggerUrl"].(string); ok && wsURL != "" {
				return wsURL, nil
			}
		} else {
			lastErr = err
		}
		time.Sleep(500 * time.Millisecond)
	}
	return "", fmt.Errorf("browser debugging port (%d) did not start: %v", port, lastErr)
}

type cookieItem struct {
	Value    string
	Expires  int64
	HttpOnly bool
}

func readCookies(ws *wsClient) (map[string]cookieItem, error) {
	res, err := ws.call("Storage.getCookies", nil, 20*time.Second)
	if err != nil {
		return nil, err
	}
	out := map[string]cookieItem{}
	list, _ := res["cookies"].([]any)
	for _, itemRaw := range list {
		c, ok := itemRaw.(map[string]any)
		if !ok {
			continue
		}
		dom := strings.TrimLeft(fmt.Sprint(c["domain"]), ".")
		if !strings.Contains(dom, domainHint) {
			continue
		}
		name, _ := c["name"].(string)
		if name == "" {
			continue
		}
		exp := int64(-1)
		if expF, ok := c["expires"].(float64); ok {
			exp = int64(expF)
		}
		val, _ := c["value"].(string)
		ho, _ := c["httpOnly"].(bool)
		out[name] = cookieItem{Value: val, Expires: exp, HttpOnly: ho}
	}
	return out, nil
}

func upload(base, key, label string, cookies map[string]cookieItem) (map[string]any, error) {
	cMap := map[string]string{}
	eMap := map[string]int64{}
	for k, v := range cookies {
		cMap[k] = v.Value
		if v.Expires > 0 {
			eMap[k] = v.Expires
		}
	}
	bodyBytes, _ := json.Marshal(map[string]any{
		"label":   label,
		"cookies": cMap,
		"expires": eMap,
	})
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(base, "/")+"/admin/accounts", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %v", base, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		detail := string(raw)
		if len(detail) > 300 {
			detail = detail[:300]
		}
		return nil, fmt.Errorf("upload failed HTTP %d: %s", resp.StatusCode, detail)
	}
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return out, nil
}

func fmtTS(ts int64) string {
	if ts <= 0 {
		return "session"
	}
	return time.Unix(ts, 0).Format("2006-01-02 15:04")
}

func main() {
	baseFlag := flag.String("base", os.Getenv("MUSE2API_BASE"), "muse2api base URL, e.g. http://your-server-ip:18610")
	keyFlag := flag.String("key", os.Getenv("MUSE2API_KEY"), "API Key (prefixed with m2a_)")
	labelFlag := flag.String("label", "", "Account label, e.g. acc-01")
	timeoutFlag := flag.Int("timeout", 300, "Maximum seconds to wait for login (default 300)")
	keepOpenFlag := flag.Bool("keep-open", false, "Keep browser window open after upload")
	portFlag := flag.Int("port", defaultCDP, "CDP debugging port")
	chromeFlag := flag.String("chrome", "", "Path to Chrome/Chromium executable")
	flag.Parse()

	fmt.Println(strings.Repeat("=", 66))
	fmt.Println("  muse2api Cookie Helper (Go)")
	fmt.Println(strings.Repeat("=", 66))
	fmt.Println()

	if *baseFlag == "" || *keyFlag == "" {
		fmt.Println("✗ Missing arguments. Usage:")
		fmt.Println("    go run get_muse_cookie.go --base http://localhost:18610 --key m2a_xxx")
		os.Exit(2)
	}
	label := *labelFlag
	if label == "" {
		label = "muse-" + time.Now().Format("0102-1504")
	}

	chrome := *chromeFlag
	if chrome == "" {
		chrome = findChrome()
	}
	if chrome == "" {
		fmt.Println("✗ Chrome / Edge / Chromium not found. Specify the path with --chrome.")
		os.Exit(3)
	}
	fmt.Printf("Browser: %s\n", chrome)

	profile := filepath.Join(os.TempDir(), "muse2api-cookie-profile")
	_ = os.RemoveAll(profile)
	_ = os.MkdirAll(profile, 0o755)

	cmd := exec.Command(chrome,
		fmt.Sprintf("--remote-debugging-port=%d", *portFlag),
		fmt.Sprintf("--user-data-dir=%s", profile),
		"--no-first-run", "--no-default-browser-check",
		"--no-sandbox", "--disable-gpu",
		"--new-window", siteURL,
	)
	fmt.Println("Launching an isolated browser window...")
	if err := cmd.Start(); err != nil {
		fmt.Printf("✗ Failed to launch browser: %v\n", err)
		os.Exit(4)
	}

	var ws *wsClient
	cleanup := func() {
		if ws != nil {
			ws.close()
		}
		if !*keepOpenFlag && cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = os.RemoveAll(profile)
	}
	defer cleanup()

	wsURL, err := waitCDP(*portFlag, 30*time.Second)
	if err != nil {
		fmt.Printf("✗ %v\n", err)
		os.Exit(4)
	}
	ws, err = dialWS(wsURL, 15*time.Second)
	if err != nil {
		fmt.Printf("✗ Failed to connect to CDP port: %v\n", err)
		os.Exit(4)
	}

	fmt.Println()
	fmt.Println("┌" + strings.Repeat("─", 64) + "┐")
	fmt.Println("│  Please log in to muse.ai in the newly opened browser window.  │")
	fmt.Println("│  Cookies will be captured and uploaded automatically.          │")
	fmt.Println("└" + strings.Repeat("─", 64) + "┘")
	fmt.Println()

	deadline := time.Now().Add(time.Duration(*timeoutFlag) * time.Second)
	var got map[string]cookieItem
	var lastNote time.Time
	success := false

	for time.Now().Before(deadline) {
		got, err = readCookies(ws)
		if err != nil {
			fmt.Printf("  Error reading cookies, retrying... (%v)\n", err)
			time.Sleep(2 * time.Second)
			continue
		}
		var have []string
		for _, n := range essentialCookies {
			if _, ok := got[n]; ok {
				have = append(have, n)
			}
		}
		if len(have) == len(essentialCookies) {
			success = true
			break
		}
		if time.Since(lastNote) > 5*time.Second {
			lastNote = time.Now()
			left := int(time.Until(deadline).Seconds())
			haveStr := "none"
			if len(have) > 0 {
				haveStr = strings.Join(have, ", ")
			}
			fmt.Printf("  Waiting for login... %d/%d core cookies (%s), %ds remaining\n", len(have), len(essentialCookies), haveStr, left)
		}
		time.Sleep(2 * time.Second)
	}

	if !success {
		fmt.Println("\n✗ Timed out waiting for login.")
		os.Exit(5)
	}

	fmt.Println("\n✓ Captured complete session cookies:\n")
	fmt.Printf("  %-30s%-10s%s\n", "Cookie Name", "HttpOnly", "Expires")
	fmt.Println("  " + strings.Repeat("-", 62))
	for _, name := range essentialCookies {
		c := got[name]
		ho := "no"
		if c.HttpOnly {
			ho = "yes"
		}
		fmt.Printf("  %-30s%-10s%s\n", name, ho, fmtTS(c.Expires))
	}
	fmt.Println("\nUploading to muse2api...")

	r, err := upload(*baseFlag, *keyFlag, label, got)
	if err != nil {
		fmt.Printf("✗ %v\n\n  You can paste this cookie string into the Admin Console manually:\n\n  ", err)
		var pairs []string
		for k, v := range got {
			pairs = append(pairs, k+"="+v.Value)
		}
		fmt.Println(strings.Join(pairs, "; "))
		os.Exit(6)
	}

	addedList, _ := r["added"].([]any)
	firstAcc, _ := addedList[0].(map[string]any)
	fmt.Println()
	fmt.Println(strings.Repeat("=", 66))
	fmt.Printf("✓ Imported! Label: %v\n", firstAcc["label"])
	fmt.Printf("  Account ID: %v\n", firstAcc["id"])
	fmt.Printf("  Cookies: %v\n", firstAcc["cookie_count"])
	if wMsg, ok := r["warning"].(string); ok && wMsg != "" {
		fmt.Printf("  ⚠ %s\n", wMsg)
	}
	fmt.Printf("\n  Open %s/admin to view your account pool.\n", strings.TrimRight(*baseFlag, "/"))
	fmt.Println(strings.Repeat("=", 66))
}
