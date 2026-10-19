//go:build ignore

// Mail.tm temporary inbox helper: provisions a disposable address and polls for OTP codes.
//
// Usage:
//   go run tools/mailtm_otp.go
//   go run tools/mailtm_otp.go --addr a@b.c --pass xxx
//   go run tools/mailtm_otp.go --timeout 600
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
)

const mailAPI = "https://api.mail.tm"

func mailReq(path, method, token string, body any) any {
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, mailAPI+path, rdr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[!] %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[!] %s %s -> %v\n", method, path, err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		detail := string(raw)
		if len(detail) > 300 {
			detail = detail[:300]
		}
		fmt.Fprintf(os.Stderr, "[!] %s %s -> HTTP %d: %s\n", method, path, resp.StatusCode, detail)
		os.Exit(1)
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return map[string]any{}
	}
	var out any
	_ = json.Unmarshal(raw, &out)
	return out
}

func randChars(alphabet string, n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	out := make([]byte, n)
	for i := 0; i < n; i++ {
		out[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return string(out)
}

func extractMembers(v any) []map[string]any {
	var list []any
	switch val := v.(type) {
	case map[string]any:
		list, _ = val["hydra:member"].([]any)
	case []any:
		list = val
	}
	var out []map[string]any
	for _, it := range list {
		if m, ok := it.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

func newAccount() (string, string, string) {
	domains := extractMembers(mailReq("/domains", http.MethodGet, "", nil))
	domain := ""
	for _, d := range domains {
		active, hasActive := d["isActive"].(bool)
		if !hasActive || active {
			domain, _ = d["domain"].(string)
			if domain != "" {
				break
			}
		}
	}
	user := randChars("abcdefghijklmnopqrstuvwxyz0123456789", 12)
	pwd := randChars("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", 16)
	addr := user + "@" + domain
	mailReq("/accounts", http.MethodPost, "", map[string]string{"address": addr, "password": pwd})
	tokObj, _ := mailReq("/token", http.MethodPost, "", map[string]string{"address": addr, "password": pwd}).(map[string]any)
	tok, _ := tokObj["token"].(string)
	return addr, pwd, tok
}

func loginMail(addr, pwd string) string {
	tokObj, _ := mailReq("/token", http.MethodPost, "", map[string]string{"address": addr, "password": pwd}).(map[string]any)
	tok, _ := tokObj["token"].(string)
	return tok
}

var otpRe = regexp.MustCompile(`\b(\d{4,8})\b`)

func pollInbox(token string, timeoutSec int) string {
	seen := map[string]bool{}
	deadline := time.Now().Add(time.Duration(timeoutSec) * time.Second)
	fmt.Printf("[*] Polling inbox (up to %ds, Ctrl-C to stop)\n\n", timeoutSec)

	for time.Now().Before(deadline) {
		msgs := extractMembers(mailReq("/messages?page=1", http.MethodGet, token, nil))
		for _, m := range msgs {
			id, _ := m["id"].(string)
			if id == "" || seen[id] {
				continue
			}
			seen[id] = true
			full, _ := mailReq("/messages/"+id, http.MethodGet, token, nil).(map[string]any)
			frmMap, _ := full["from"].(map[string]any)
			frm, _ := frmMap["address"].(string)
			if frm == "" {
				frm = "?"
			}
			subj, _ := full["subject"].(string)
			body, _ := full["text"].(string)
			if body == "" {
				body, _ = full["intro"].(string)
			}
			fmt.Printf("[+] New message from %s\n    Subject: %s\n", frm, subj)
			codes := otpRe.FindAllString(subj+"\n"+body, -1)
			preview := strings.TrimSpace(body)
			if len(preview) > 500 {
				preview = preview[:500]
			}
			fmt.Printf("    Body: %s\n", preview)
			if len(codes) > 0 {
				fmt.Printf("\n>>> OTP Code: %s\n\n", codes[0])
				return codes[0]
			}
			fmt.Println("    (No OTP code detected, continuing to wait...)\n")
		}
		left := int(time.Until(deadline).Seconds())
		fmt.Printf("    Waiting... %ds remaining\r", left)
		time.Sleep(3 * time.Second)
	}
	fmt.Println("\n[!] Timed out waiting for OTP email.")
	return ""
}

func main() {
	addrFlag := flag.String("addr", "", "Reuse existing email address")
	pwdFlag := flag.String("pass", "", "Reuse existing email password")
	timeoutFlag := flag.Int("timeout", 900, "Polling timeout in seconds")
	flag.Parse()

	var addr, pwd, token string
	if *addrFlag != "" && *pwdFlag != "" {
		addr, pwd, token = *addrFlag, *pwdFlag, loginMail(*addrFlag, *pwdFlag)
		fmt.Printf("[*] Reusing address: %s\n", addr)
	} else {
		addr, pwd, token = newAccount()
		fmt.Printf("[*] Provisioned new address: %s\n    Password: %s\n", addr, pwd)
	}
	fmt.Printf("\n=== Enter this email on the muse.ai login page ===\n    %s\n\n", addr)
	if code := pollInbox(token, *timeoutFlag); code == "" {
		os.Exit(1)
	}
}
