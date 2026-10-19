package main

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

func defaultBaseDir() string {
	if envHome := strings.TrimSpace(os.Getenv("MUSE2API_HOME")); envHome != "" {
		return envHome
	}
	if wd, err := os.Getwd(); err == nil && wd != "" {
		return wd
	}
	if exe, err := os.Executable(); err == nil {
		return filepath.Dir(exe)
	}
	return "."
}

func loadDotEnv(baseDir string) {
	if baseDir == "" {
		baseDir = defaultBaseDir()
	}
	envPath := filepath.Join(baseDir, ".env")
	f, err := os.Open(envPath)
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		s := strings.TrimSpace(scanner.Text())
		if s == "" || strings.HasPrefix(s, "#") || !strings.Contains(s, "=") {
			continue
		}
		parts := strings.SplitN(s, "=", 2)
		k := strings.TrimSpace(parts[0])
		v := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
		if k != "" {
			if _, exists := os.LookupEnv(k); !exists {
				_ = os.Setenv(k, v)
			}
		}
	}
}

func envStr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	if n, err := strconv.Atoi(v); err == nil {
		return n
	}
	return def
}

func isFileOrOnPath(p string) bool {
	if p == "" {
		return false
	}
	if st, err := os.Stat(p); err == nil && !st.IsDir() {
		return true
	}
	if _, err := exec.LookPath(p); err == nil {
		return true
	}
	return false
}

func detectChromium() string {
	configured := strings.TrimSpace(os.Getenv("MUSE2API_CHROMIUM"))
	if isFileOrOnPath(configured) {
		return configured
	}
	candidates := []string{
		"/usr/bin/chromium",
		"/usr/bin/chromium-browser",
		"/snap/bin/chromium",
		"/usr/bin/google-chrome",
		"/usr/bin/google-chrome-stable",
		"chromium",
		"chromium-browser",
		"google-chrome",
		"chrome",
		`C:\Program Files\Google\Chrome\Application\chrome.exe`,
		`C:\Program Files (x86)\Google\Chrome\Application\chrome.exe`,
		`C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe`,
		"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
	}
	for _, c := range candidates {
		if isFileOrOnPath(c) {
			return c
		}
	}
	if configured != "" {
		return configured
	}
	return "chromium"
}

type Config struct {
	BaseDir                   string
	Host                      string
	Port                      int
	Chromium                  string
	CDPPort                   int
	HomeDir                   string
	ExtraPath                 string
	SiteURL                   string
	LoginWait                 int
	ProfileRoot               string
	APIKey                    string
	PublicBase                string
	CORSOrigins               string
	ImageTimeout              int
	VideoTimeout              int
	ChatTimeout               int
	MaxConcurrency            int
	ToolProtocol              bool
	KeepaliveDisabledAccounts bool
}

func NewConfig() *Config {
	base := defaultBaseDir()
	loadDotEnv(base)
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		home = "/root"
	}
	maxConc := envInt("MUSE2API_CONCURRENCY", 2)
	if maxConc < 1 {
		maxConc = 1
	}
	return &Config{
		BaseDir:                   envStr("MUSE2API_HOME", base),
		Host:                      envStr("MUSE2API_HOST", "0.0.0.0"),
		Port:                      envInt("MUSE2API_PORT", 18610),
		Chromium:                  detectChromium(),
		CDPPort:                   envInt("MUSE2API_CDP_PORT", 19210),
		HomeDir:                   envStr("MUSE2API_HOME_DIR", home),
		ExtraPath:                 envStr("MUSE2API_EXTRA_PATH", "/snap/bin"),
		SiteURL:                   envStr("MUSE2API_SITE", "https://muse.ai/"),
		LoginWait:                 envInt("MUSE2API_LOGIN_WAIT", 60),
		ProfileRoot:               envStr("MUSE2API_PROFILE_ROOT", filepath.Join(home, "muse2api-profiles")),
		APIKey:                    envStr("MUSE2API_KEY", ""),
		PublicBase:                envStr("MUSE2API_PUBLIC_BASE", ""),
		CORSOrigins:               envStr("MUSE2API_CORS_ORIGINS", "https://muse.ai,https://www.muse.ai"),
		ImageTimeout:              envInt("MUSE2API_IMAGE_TIMEOUT", 240),
		VideoTimeout:              envInt("MUSE2API_VIDEO_TIMEOUT", 600),
		ChatTimeout:               envInt("MUSE2API_CHAT_TIMEOUT", 300),
		MaxConcurrency:            maxConc,
		ToolProtocol:              strings.TrimSpace(envStr("MUSE2API_TOOL_PROTOCOL", "1")) != "0",
		KeepaliveDisabledAccounts: strings.TrimSpace(envStr("MUSE2API_KEEPALIVE_DISABLED_ACCOUNTS", "0")) != "0",
	}
}

func (c *Config) DataDir() string {
	return filepath.Join(c.BaseDir, "data")
}

func (c *Config) MediaDir() string {
	return filepath.Join(c.DataDir(), "media")
}

func (c *Config) DownloadDir() string {
	return filepath.Join(c.DataDir(), "downloads")
}

func (c *Config) ProfileDir() string {
	if c.ProfileRoot == "" {
		return filepath.Join(c.BaseDir, "profile")
	}
	return filepath.Join(c.ProfileRoot, "generate")
}

func (c *Config) AccountsFile() string {
	return filepath.Join(c.DataDir(), "accounts.json")
}

func (c *Config) TasksFile() string {
	return filepath.Join(c.DataDir(), "tasks.json")
}

func (c *Config) EnsureDirs() {
	for _, d := range []string{c.BaseDir, c.DataDir(), c.MediaDir(), c.DownloadDir(), c.ProfileDir()} {
		if d != "" {
			_ = os.MkdirAll(d, 0o755)
		}
	}
}

func PersistEnv(baseDir, key, value string) {
	path := filepath.Join(baseDir, ".env")
	var lines []string
	if b, err := os.ReadFile(path); err == nil {
		lines = strings.Split(strings.ReplaceAll(string(b), "\r\n", "\n"), "\n")
	}
	found := false
	prefix := key + "="
	for i, ln := range lines {
		if strings.HasPrefix(strings.TrimSpace(ln), prefix) {
			lines[i] = key + "=" + value
			found = true
		}
	}
	if !found {
		lines = append(lines, key+"="+value)
	}
	content := strings.TrimSpace(strings.Join(lines, "\n")) + "\n"
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(content), 0o600); err == nil {
		_ = os.Rename(tmp, path)
	}
}

var CFG = NewConfig()
