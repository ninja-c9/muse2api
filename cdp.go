package main

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type CDPError struct {
	Msg string
}

func (e *CDPError) Error() string {
	return e.Msg
}

// CDP implements a minimal Chrome DevTools Protocol client over RFC6455 WebSocket using pure Go stdlib.
type CDP struct {
	conn    net.Conn
	br      *bufio.Reader
	timeout time.Duration
	maxSize int
	id      int64
	mu      sync.Mutex

	// Optional hooks for offline unit testing without a browser connection.
	MockJS   func(expr string) (any, error)
	MockSend func(method string, params map[string]any) (map[string]any, error)
}

func NewCDP(wsURL string, timeout time.Duration) (*CDP, error) {
	if timeout <= 0 {
		timeout = 90 * time.Second
	}
	u, err := url.Parse(wsURL)
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
	wsKey := base64.StdEncoding.EncodeToString(keyBytes[:])

	req := fmt.Sprintf("GET %s HTTP/1.1\r\n"+
		"Host: %s\r\n"+
		"Upgrade: websocket\r\n"+
		"Connection: Upgrade\r\n"+
		"Sec-WebSocket-Key: %s\r\n"+
		"Sec-WebSocket-Version: 13\r\n\r\n", path, host, wsKey)
	if _, err := conn.Write([]byte(req)); err != nil {
		_ = conn.Close()
		return nil, err
	}

	br := bufio.NewReaderSize(conn, 64*1024)
	statusLine, err := br.ReadString('\n')
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if !strings.Contains(statusLine, " 101") {
		_ = conn.Close()
		return nil, fmt.Errorf("websocket handshake failed: %s", strings.TrimSpace(statusLine))
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
	_ = conn.SetDeadline(time.Time{})

	return &CDP{
		conn:    conn,
		br:      br,
		timeout: timeout,
		maxSize: 256 << 20, // 256MB
	}, nil
}

func (c *CDP) writeFrame(opcode byte, payload []byte) error {
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
	_, err := c.conn.Write(buf)
	return err
}

func (c *CDP) readMessage() ([]byte, error) {
	var message []byte
	for {
		var h [2]byte
		if _, err := io.ReadFull(c.br, h[:]); err != nil {
			return nil, err
		}
		fin := (h[0] & 0x80) != 0
		opcode := h[0] & 0x0F
		masked := (h[1] & 0x80) != 0
		length := int64(h[1] & 0x7F)

		if length == 126 {
			var ext [2]byte
			if _, err := io.ReadFull(c.br, ext[:]); err != nil {
				return nil, err
			}
			length = int64(binary.BigEndian.Uint16(ext[:]))
		} else if length == 127 {
			var ext [8]byte
			if _, err := io.ReadFull(c.br, ext[:]); err != nil {
				return nil, err
			}
			length = int64(binary.BigEndian.Uint64(ext[:]))
		}

		if c.maxSize > 0 && int64(len(message))+length > int64(c.maxSize) {
			return nil, errors.New("websocket message exceeds max size")
		}

		var mask [4]byte
		if masked {
			if _, err := io.ReadFull(c.br, mask[:]); err != nil {
				return nil, err
			}
		}

		data := make([]byte, length)
		if length > 0 {
			if _, err := io.ReadFull(c.br, data); err != nil {
				return nil, err
			}
		}
		if masked {
			for i := range data {
				data[i] ^= mask[i%4]
			}
		}

		switch opcode {
		case 0x8: // close
			return nil, io.EOF
		case 0x9: // ping -> pong
			_ = c.writeFrame(0xA, data)
			continue
		case 0xA: // pong
			continue
		case 0x1, 0x2, 0x0:
			message = append(message, data...)
			if fin {
				return message, nil
			}
		}
	}
}

func (c *CDP) Send(method string, params map[string]any, timeout time.Duration) (map[string]any, error) {
	if c.MockSend != nil {
		return c.MockSend(method, params)
	}
	if c.conn == nil {
		return map[string]any{}, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.id++
	mid := c.id
	if params == nil {
		params = map[string]any{}
	}
	reqBytes, err := json.Marshal(map[string]any{
		"id":     mid,
		"method": method,
		"params": params,
	})
	if err != nil {
		return nil, err
	}
	to := timeout
	if to <= 0 {
		to = c.timeout
	}
	deadline := time.Now().Add(to)
	_ = c.conn.SetWriteDeadline(deadline)
	if err := c.writeFrame(0x1, reqBytes); err != nil {
		return nil, err
	}

	for time.Now().Before(deadline) {
		_ = c.conn.SetReadDeadline(deadline)
		raw, err := c.readMessage()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return nil, err
		}
		var msg map[string]any
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue
		}
		idVal, ok := toInt64(msg["id"])
		if ok && idVal == mid {
			if errObj, hasErr := msg["error"]; hasErr && errObj != nil {
				b, _ := json.Marshal(errObj)
				return nil, &CDPError{Msg: fmt.Sprintf("%s: %s", method, string(b))}
			}
			return msg, nil
		}
	}
	return nil, fmt.Errorf("CDP %s timed out", method)
}

func (c *CDP) JS(expr string, awaitPromise bool, timeout time.Duration) (any, error) {
	if c.MockJS != nil {
		return c.MockJS(expr)
	}
	r, err := c.Send("Runtime.evaluate", map[string]any{
		"expression":    expr,
		"returnByValue": true,
		"awaitPromise":  awaitPromise,
	}, timeout)
	if err != nil {
		return nil, err
	}
	resOuter, _ := r["result"].(map[string]any)
	if resOuter != nil {
		if resInner, ok := resOuter["result"].(map[string]any); ok {
			if val, exists := resInner["value"]; exists {
				return val, nil
			}
		}
		return resOuter, nil
	}
	return nil, nil
}

func (c *CDP) JSStr(expr string) string {
	v, err := c.JS(expr, false, 0)
	if err != nil || v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	b, _ := json.Marshal(v)
	return string(b)
}

func (c *CDP) JSBool(expr string) bool {
	v, err := c.JS(expr, false, 0)
	if err != nil || v == nil {
		return false
	}
	switch val := v.(type) {
	case bool:
		return val
	case string:
		return val != "" && val != "false" && val != "0"
	case float64:
		return val != 0
	default:
		return false
	}
}

func (c *CDP) Pump(seconds time.Duration, onEvent func(map[string]any)) {
	if c.conn == nil {
		return
	}
	end := time.Now().Add(seconds)
	for time.Now().Before(end) {
		_ = c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		raw, err := c.readMessage()
		if err != nil {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal(raw, &ev); err != nil {
			continue
		}
		if onEvent != nil {
			if _, ok := ev["method"]; ok {
				onEvent(ev)
			}
		}
	}
}

func (c *CDP) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

func httpJSON(rawURL string, timeout time.Duration) (map[string]any, error) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(rawURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}
