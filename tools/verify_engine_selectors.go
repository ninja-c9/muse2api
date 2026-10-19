//go:build ignore

// Connects to the local CDP debugging port to verify active muse.ai page targets.
//
// Usage:
//   go run tools/verify_engine_selectors.go
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	verifyCDPPort = 19210
	pngB64        = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
)

func main() {
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/json", verifyCDPPort))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to CDP port: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	var pages []map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&pages)

	var wsURL, pageURL string
	for _, p := range pages {
		u, _ := p["url"].(string)
		if p["type"] == "page" && strings.Contains(u, "muse.ai") {
			wsURL, _ = p["webSocketDebuggerUrl"].(string)
			pageURL = u
			break
		}
	}
	if wsURL == "" {
		fmt.Fprintln(os.Stderr, "No muse.ai page target found")
		os.Exit(1)
	}
	fmt.Printf("[verify] page = %s (ws = %s)\n", pageURL, wsURL)
	_ = time.Now()
}
