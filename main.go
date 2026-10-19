package main

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	store           *Store
	engine          *MuseEngine
	GEN_LOCK        *ConcurrencyLock
	ACCOUNT_POOL    *AccountPoolManager
	IMAGE_TASK_LOCK sync.Mutex
)

func InitGlobals(cfg *Config) {
	if cfg == nil {
		cfg = NewConfig()
	}
	CFG = cfg
	CFG.EnsureDirs()
	store = NewStore(CFG)
	engine = NewMuseEngine(CFG)
	GEN_LOCK = NewConcurrencyLock(CFG.MaxConcurrency)
	ACCOUNT_POOL = NewAccountPoolManager(store)
}

func init() {
	InitGlobals(CFG)
}

func publicBase() string {
	return strings.TrimRight(CFG.PublicBase, "/")
}

func mediaURL(name string) string {
	base := publicBase()
	if base != "" {
		return fmt.Sprintf("%s/v1/media/%s", base, name)
	}
	return fmt.Sprintf("/v1/media/%s", name)
}

func errType(status int) string {
	switch {
	case status == 404:
		return "not_found_error"
	case status == 429:
		return "rate_limit_error"
	case status >= 500:
		return "server_error"
	default:
		return "invalid_request_error"
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, r *http.Request, status int, detail string) {
	if strings.HasPrefix(r.URL.Path, "/v1/") {
		writeJSON(w, status, map[string]any{
			"error": map[string]any{
				"message": detail,
				"type":    errType(status),
				"param":   nil,
				"code":    status,
			},
		})
		return
	}
	writeJSON(w, status, map[string]any{"detail": detail})
}

func checkAuth(w http.ResponseWriter, r *http.Request) bool {
	if CFG.APIKey == "" {
		return true
	}
	token := strings.TrimSpace(r.Header.Get("x-api-key"))
	if token == "" {
		authHdr := strings.TrimSpace(r.Header.Get("Authorization"))
		if len(authHdr) >= 7 && strings.EqualFold(authHdr[:7], "bearer ") {
			token = strings.TrimSpace(authHdr[7:])
		}
	}
	if token == "" || token != CFG.APIKey {
		writeErr(w, r, http.StatusUnauthorized, "Invalid API key or missing Authorization: Bearer <key> / x-api-key")
		return false
	}
	return true
}

func setSSEHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Connection", "keep-alive")
}

func sseChunk(obj any) string {
	b, _ := json.Marshal(obj)
	return "data: " + string(b) + "\n\n"
}

func sseEvent(event string, data any) string {
	b, _ := json.Marshal(data)
	return fmt.Sprintf("event: %s\ndata: %s\n\n", event, string(b))
}

func chatChunk(cid string, created int64, model string, delta map[string]any, finish any) map[string]any {
	return map[string]any{
		"id":      cid,
		"object":  "chat.completion.chunk",
		"created": created,
		"model":   model,
		"choices": []map[string]any{
			{"index": 0, "delta": delta, "finish_reason": finish},
		},
	}
}

func wantUsage(streamOpts any) bool {
	if m, ok := streamOpts.(map[string]any); ok {
		if b, ok := m["include_usage"].(bool); ok {
			return b
		}
	}
	return false
}

func paceText(text string, chunkSize int, delay time.Duration, emit func(string)) {
	if text == "" {
		return
	}
	runes := []rune(text)
	if len(runes) <= 3 || delay <= 0 || chunkSize <= 0 {
		emit(text)
		return
	}
	for i := 0; i < len(runes); i += chunkSize {
		end := i + chunkSize
		if end > len(runes) {
			end = len(runes)
		}
		emit(string(runes[i:end]))
		if end < len(runes) {
			time.Sleep(delay)
		}
	}
}

// ---------- Image Generation & Async Queue ----------

func imageResponse(req ImageRequest, res map[string]any) map[string]any {
	fname, _ := res["filename"].(string)
	sizeVal := req.Size
	if sizeVal == "" {
		sizeVal = "auto"
	}
	item := map[string]any{
		"revised_prompt": req.Prompt,
		"url":            mediaURL(fname),
		"size":           sizeVal,
		"kind":           res["kind"],
		"bytes":          res["size"],
	}
	if req.ResponseFormat == "b64_json" {
		fpath, _ := res["path"].(string)
		if fpath == "" {
			fpath = filepath.Join(CFG.MediaDir(), fname)
		}
		if b, err := os.ReadFile(fpath); err == nil {
			item["b64_json"] = base64.StdEncoding.EncodeToString(b)
		}
		delete(item, "url")
	}
	return map[string]any{
		"created": nowUnix(),
		"data":    []map[string]any{item},
	}
}

func queueImage(w http.ResponseWriter, r *http.Request, req ImageRequest, prompt, refImg, idempotencyKey string) {
	req.ApplyDefaults()
	if len(idempotencyKey) > 256 {
		writeErr(w, r, http.StatusBadRequest, "Idempotency-Key too long")
		return
	}
	var keyHash string
	if idempotencyKey != "" {
		h := sha256.Sum256([]byte(idempotencyKey))
		keyHash = hex.EncodeToString(h[:])
	}
	reqBytes, _ := json.Marshal(map[string]any{
		"request":   req,
		"reference": refImg,
	})
	rh := sha256.Sum256(reqBytes)
	requestHash := hex.EncodeToString(rh[:])

	IMAGE_TASK_LOCK.Lock()
	tasks := store.ListTasks(1000)
	if keyHash != "" {
		for _, existing := range tasks {
			if existing["image_request_key"] == keyHash {
				if existing["image_request_hash"] != requestHash {
					IMAGE_TASK_LOCK.Unlock()
					writeErr(w, r, http.StatusConflict, "Idempotency-Key reused with different request")
					return
				}
				prog, _ := toInt64(existing["progress"])
				IMAGE_TASK_LOCK.Unlock()
				writeJSON(w, http.StatusAccepted, map[string]any{
					"id":         existing["id"],
					"task_id":    existing["id"],
					"object":     "image.task",
					"status":     existing["status"],
					"progress":   prog,
					"created_at": existing["created_at"],
				})
				return
			}
		}
	}
	activeCount := 0
	for _, t := range tasks {
		st, _ := t["status"].(string)
		if t["kind"] == "image" && (st == "queued" || st == "processing") {
			activeCount++
		}
	}
	if activeCount >= 8 {
		IMAGE_TASK_LOCK.Unlock()
		writeErr(w, r, http.StatusTooManyRequests, "Image queue is full; retry later")
		return
	}

	task := store.CreateTask("image", req.Prompt)
	tid, _ := task["id"].(string)
	var keyHashVal any
	if keyHash != "" {
		keyHashVal = keyHash
	}
	store.UpdateTask(tid, map[string]any{
		"api_prompt":         prompt,
		"size":               req.Size,
		"response_format":    req.ResponseFormat,
		"progress":           0,
		"image_request_key":  keyHashVal,
		"image_request_hash": requestHash,
	})
	IMAGE_TASK_LOCK.Unlock()

	timeout := CFG.ImageTimeout
	if req.Timeout != nil && *req.Timeout > 0 {
		timeout = *req.Timeout
	}

	go func() {
		t0 := time.Now()
		onAcquired := func() {
			t0 = time.Now()
			store.UpdateTask(tid, map[string]any{"status": "processing", "progress": 5})
		}
		res, accID, err := RunGeneration(prompt, "image", timeout, "", func(p int) {
			store.UpdateTask(tid, map[string]any{"progress": p})
		}, refImg, onAcquired)
		elapsed := float64(int(time.Since(t0).Seconds()*10)) / 10.0
		if err != nil {
			store.UpdateTask(tid, map[string]any{
				"status":  "failed",
				"error":   err.Error(),
				"elapsed": elapsed,
			})
			return
		}
		fname, _ := res["filename"].(string)
		mURL := mediaURL(fname)
		store.UpdateTask(tid, map[string]any{
			"status":   "completed",
			"progress": 100,
			"account":  accID,
			"elapsed":  elapsed,
			"url":      mURL,
			"result": map[string]any{
				"filename": fname,
				"size":     res["size"],
				"kind":     res["kind"],
				"url":      mURL,
			},
		})
	}()

	writeJSON(w, http.StatusAccepted, map[string]any{
		"id":         tid,
		"task_id":    tid,
		"object":     "image.task",
		"status":     "queued",
		"progress":   0,
		"created_at": task["created_at"],
	})
}

// ---------- Keepalive & Warmup ----------

var (
	keepaliveMu    sync.Mutex
	KeepaliveState = map[string]any{
		"running":     false,
		"last_run":    nil,
		"last_result": nil,
		"next_run":    nil,
	}
	probeAccountSyncHook func(aid string, checkQuota bool) map[string]any
)

func ProbeAccountSync(aid string, checkQuota bool) map[string]any {
	if probeAccountSyncHook != nil {
		return probeAccountSyncHook(aid, checkQuota)
	}
	acc := store.GetAccount(aid)
	if acc == nil || len(extractCookiesMap(acc["cookies"])) == 0 {
		lbl := aid
		if acc != nil {
			if l, ok := acc["label"].(string); ok && l != "" {
				lbl = l
			}
		}
		return map[string]any{"ok": false, "id": aid, "label": lbl, "error": "Account has no valid cookies"}
	}
	label, _ := acc["label"].(string)
	if label == "" {
		label = aid
	}
	res, err := RenewSessionHTTP(extractCookiesMap(acc["cookies"]), extractExpMap(acc["cookies_exp"]), true)
	if err != nil {
		errMsg := err.Error()
		if len(errMsg) > 200 {
			errMsg = errMsg[:200]
		}
		if isAuthErr(err) {
			fVal := false
			store.TouchKeepalive(aid, &fVal, "Keepalive auth failed: "+errMsg)
		} else {
			store.TouchKeepalive(aid, nil, "Keepalive unconfirmed (status preserved): "+errMsg)
		}
		return map[string]any{"ok": false, "id": aid, "label": label, "error": err.Error()}
	}

	okRes := res["ok"] == true
	store.UpdateAccount(aid, map[string]any{
		"cookies":     res["cookies"],
		"cookies_exp": res["cookies_exp"],
		"ok":          okRes,
		"synced_at":   nowUnix(),
	})
	vmState, _ := res["vm_state"].(string)
	if vmState == "" {
		vmState = "RUNNING"
	}
	tVal := true
	store.TouchKeepalive(aid, &tVal, fmt.Sprintf("Session valid · Auto keepalive (VM: %s)", vmState))

	quota := acc["quota"]
	if checkQuota && GEN_LOCK.Acquire(false, 0) {
		func() {
			defer GEN_LOCK.Release()
			if err := engine.Start(); err != nil {
				return
			}
			q, qErr := engine.Quota(
				extractCookiesMap(res["cookies"]),
				extractExpMap(res["cookies_exp"]),
				aid,
			)
			if qErr == nil && q["found"] == true {
				q["checked_at"] = nowUnix()
				store.UpdateAccount(aid, map[string]any{"quota": q})
				quota = q
			} else if qErr != nil {
				log.Printf("[WARN] Failed to read quota for account %s: %v", aid, qErr)
			}
		}()
	}

	updated := store.GetAccount(aid)
	var expAt any
	if updated != nil {
		expAt = updated["expires_at"]
	}
	return map[string]any{
		"ok":         true,
		"id":         aid,
		"label":      label,
		"vm_id":      res["vm_id"],
		"vm_state":   vmState,
		"wake_ok":    res["wake_ok"],
		"expires_at": expAt,
		"quota":      quota,
	}
}

func RunKeepaliveAll(force bool) map[string]any {
	keepaliveMu.Lock()
	defer keepaliveMu.Unlock()

	now := nowUnix()
	KeepaliveState["running"] = true
	KeepaliveState["last_run"] = now
	defer func() {
		KeepaliveState["running"] = false
	}()

	var results []map[string]any
	var skipped []map[string]any

	includeDisabled := CFG != nil && CFG.KeepaliveDisabledAccounts
	for _, a := range store.ListAccounts() {
		if !includeDisabled && !isAccountEnabled(a) {
			continue
		}
		aid, _ := a["id"].(string)
		label, _ := a["label"].(string)
		if label == "" {
			label = aid
		}
		expAt, hasExp := toInt64(a["expires_at"])
		lastKA, _ := toInt64(a["last_keepalive"])
		okVal, hasOK := a["ok"].(bool)

		needsRun := force || lastKA == 0 || ((now-lastKA) > 6*3600 && (!hasExp || expAt == 0 || (expAt-now) < 36*3600 || !hasOK || !okVal))
		if !needsRun {
			skipped = append(skipped, map[string]any{
				"id":     aid,
				"label":  label,
				"reason": "Session sufficient and recently refreshed",
			})
			continue
		}

		log.Printf("[INFO] [Keepalive] Renewing session and waking VM for account %s (%s)...", label, aid)
		res := ProbeAccountSync(aid, false)
		results = append(results, res)
		log.Printf("[INFO] [Keepalive] Account %s result: ok=%v expires_at=%v", label, res["ok"], res["expires_at"])
		sleepSec(0.5)
	}

	if results == nil {
		results = []map[string]any{}
	}
	if skipped == nil {
		skipped = []map[string]any{}
	}
	summary := map[string]any{
		"checked_at":      now,
		"refreshed_count": len(results),
		"refreshed":       results,
		"skipped_count":   len(skipped),
		"skipped":         skipped,
	}
	KeepaliveState["last_result"] = summary
	KeepaliveState["next_run"] = now + 900
	return summary
}

func WarmupBrowserSync() {
	if engine.CurrentAccID != "" && engine.Page != nil {
		return
	}
	if !GEN_LOCK.Acquire(false, 0) {
		return
	}
	defer GEN_LOCK.Release()

	acc, err := ACCOUNT_POOL.Acquire(AcquireAccountOptions{
		Rotate:     false,
		TimeoutSec: 0.1,
		HasRecord:  true,
		RecordUse:  false,
	})
	if err != nil || acc == nil {
		return
	}
	accID, _ := acc["id"].(string)
	defer func() {
		engine.CloseCurrentPage(accID)
		ACCOUNT_POOL.Release(accID)
	}()

	log.Printf("[INFO] [Browser Warmup] Warming up standby tab for account %v (%s)...", acc["label"], accID)
	refreshed, _ := RenewAndPersist(accID, true, false)
	if refreshed == nil {
		refreshed = acc
	}
	if err := engine.Start(); err != nil {
		log.Printf("[WARN] [Browser Warmup] Warmup failed: %v", err)
		return
	}
	if _, err := engine.EnsurePage(extractCookiesMap(refreshed["cookies"]), extractExpMap(refreshed["cookies_exp"]), accID); err != nil {
		log.Printf("[WARN] [Browser Warmup] Warmup failed: %v", err)
		return
	}
	log.Printf("[INFO] [Browser Warmup] Standby tab and WebSocket ready for account %v (%s)", acc["label"], accID)
}

func StartupInit() {
	for _, task := range store.ListTasks(10000) {
		kind, _ := task["kind"].(string)
		st, _ := task["status"].(string)
		tid, _ := task["id"].(string)
		if (kind == "image" || kind == "video") && (st == "queued" || st == "processing") {
			store.UpdateTask(tid, map[string]any{
				"status": "failed",
				"error":  "Task interrupted by server restart; please resubmit",
			})
		}
	}
	if CFG.APIKey == "" {
		newKey := "m2a_" + randomHex(48)
		CFG.APIKey = newKey
		PersistEnv(CFG.BaseDir, "MUSE2API_KEY", newKey)
		log.Printf("[INFO] No MUSE2API_KEY configured; generated initial key: %s", newKey)
	}
}

func ProbeAccountForTest(aid string) map[string]any {
	if !GEN_LOCK.Acquire(true, 60.0) {
		return map[string]any{"ok": false, "message": "Timed out waiting for browser queue"}
	}
	defer GEN_LOCK.Release()

	acc := store.GetAccount(aid)
	if acc == nil {
		return map[string]any{"ok": false, "message": "Account not found"}
	}
	if err := engine.Start(); err != nil {
		msg := err.Error()
		if len(msg) > 200 {
			msg = msg[:200]
		}
		store.TouchKeepalive(aid, nil, "Test unconfirmed (status preserved): "+msg)
		return map[string]any{"ok": false, "message": msg}
	}
	if _, err := engine.Refresh(extractCookiesMap(acc["cookies"]), extractExpMap(acc["cookies_exp"]), aid); err != nil {
		msg := err.Error()
		if len(msg) > 200 {
			msg = msg[:200]
		}
		if isAuthErr(err) {
			store.Mark(aid, false, msg)
		} else {
			store.TouchKeepalive(aid, nil, "Test unconfirmed (status preserved): "+msg)
		}
		return map[string]any{"ok": false, "message": msg}
	}
	synced := SyncCookies(aid)
	var quota any
	var quotaErr any
	q, err := engine.Quota(extractCookiesMap(acc["cookies"]), extractExpMap(acc["cookies_exp"]), aid)
	if err != nil {
		quotaErr = "MuseGenerationError; quota cache not updated"
	} else if q["found"] == true {
		q["checked_at"] = nowUnix()
		store.UpdateAccount(aid, map[string]any{"quota": q})
		quota = q
	} else {
		quotaErr = "Quota panel returned no valid data; existing cache preserved"
	}
	note := "Session valid"
	if quotaErr != nil {
		note += "; quota read failed"
	}
	store.Mark(aid, true, note)
	return map[string]any{
		"ok":          true,
		"message":     "Session is valid and ready for generation",
		"synced":      synced,
		"quota":       quota,
		"quota_error": quotaErr,
	}
}

// ---------- HTTP Router ----------

var ompDescRe = regexp.MustCompile(`Description:\s*(.+)`)

func NewHandler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "time": nowUnix()})
	})

	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		accs := 0
		for _, a := range store.ListAccounts() {
			if isAccountEnabled(a) {
				accs++
			}
		}
		st := "no_account"
		if accs > 0 {
			st = "ready"
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":          st,
			"accounts":        accs,
			"browser_running": engine.BrowserRunning(),
		})
	})

	mux.HandleFunc("GET /v1/models", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": Models})
	})

	mux.HandleFunc("POST /v1/images/tasks", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		var req ImageRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.Prompt) == "" {
			writeErr(w, r, http.StatusUnprocessableEntity, "Invalid request parameters")
			return
		}
		req.ApplyDefaults()
		refImg := req.ExtractReferenceImage()
		idemKey := r.Header.Get("Idempotency-Key")
		queueImage(w, r, req, BuildImagePrompt(req), refImg, idemKey)
	})

	mux.HandleFunc("GET /v1/images/tasks/{task_id}", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		taskID := r.PathValue("task_id")
		task := store.GetTask(taskID)
		if task == nil || task["kind"] != "image" {
			writeErr(w, r, http.StatusNotFound, "Image task not found")
			return
		}
		out := cloneMap(task)
		if out["status"] == "completed" {
			resMap, _ := out["result"].(map[string]any)
			promptStr, _ := out["prompt"].(string)
			sizeStr, _ := out["size"].(string)
			respFmt, _ := out["response_format"].(string)
			if respFmt == "" {
				respFmt = "url"
			}
			imgReq := ImageRequest{
				Prompt:         promptStr,
				Size:           sizeStr,
				ResponseFormat: respFmt,
			}
			for k, v := range imageResponse(imgReq, resMap) {
				out[k] = v
			}
		}
		writeJSON(w, http.StatusOK, out)
	})

	mux.HandleFunc("POST /v1/images/generations", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		var req ImageRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.Prompt) == "" {
			writeErr(w, r, http.StatusUnprocessableEntity, "Invalid request parameters")
			return
		}
		req.ApplyDefaults()
		refImg := req.ExtractReferenceImage()
		prompt := BuildImagePrompt(req)
		timeout := CFG.ImageTimeout
		if req.Timeout != nil && *req.Timeout > 0 {
			timeout = *req.Timeout
		}
		if req.Async {
			queueImage(w, r, req, prompt, refImg, "")
			return
		}
		res, _, err := RunGeneration(prompt, "image", timeout, "", nil, refImg, nil)
		if err != nil {
			if isAuthErr(err) {
				writeErr(w, r, http.StatusUnauthorized, err.Error())
			} else {
				writeErr(w, r, http.StatusBadGateway, err.Error())
			}
			return
		}
		writeJSON(w, http.StatusOK, imageResponse(req, res))
	})

	mux.HandleFunc("POST /v1/images/edits", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		ct := strings.ToLower(r.Header.Get("Content-Type"))
		var req ImageRequest
		req.ApplyDefaults()

		if strings.Contains(ct, "multipart/form-data") {
			if err := r.ParseMultipartForm(64 << 20); err != nil {
				writeErr(w, r, http.StatusBadRequest, "Failed to parse multipart form")
				return
			}
			req.Prompt = r.FormValue("prompt")
			if m := r.FormValue("model"); m != "" {
				req.Model = m
			}
			req.Size = r.FormValue("size")
			req.AspectRatio = r.FormValue("aspect_ratio")
			if rf := r.FormValue("response_format"); rf != "" {
				req.ResponseFormat = rf
			}
			if as := strings.ToLower(strings.TrimSpace(r.FormValue("async"))); as == "true" || as == "1" {
				req.Async = true
			}
			if tv := r.FormValue("timeout"); tv != "" {
				if n, err := strconv.Atoi(tv); err == nil {
					req.Timeout = &n
				}
			}
			file, hdr, err := r.FormFile("image")
			if err == nil {
				defer file.Close()
				content, _ := io.ReadAll(file)
				refMime := hdr.Header.Get("Content-Type")
				if refMime == "" {
					refMime = "image/png"
				}
				req.ReferenceImage = fmt.Sprintf("data:%s;base64,%s", refMime, base64.StdEncoding.EncodeToString(content))
			} else if imgStr := r.FormValue("image"); imgStr != "" {
				req.ReferenceImage = imgStr
			}
		} else {
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				writeErr(w, r, http.StatusUnprocessableEntity, "Invalid request parameters")
				return
			}
			req.Prompt, _ = body["prompt"].(string)
			if m, ok := body["model"].(string); ok && m != "" {
				req.Model = m
			}
			req.Size, _ = body["size"].(string)
			req.AspectRatio, _ = body["aspect_ratio"].(string)
			if rf, ok := body["response_format"].(string); ok && rf != "" {
				req.ResponseFormat = rf
			}
			if as, ok := body["async"].(bool); ok {
				req.Async = as
			} else if asStr, ok := body["async"].(string); ok && (asStr == "true" || asStr == "1") {
				req.Async = true
			}
			if tv, ok := toInt64(body["timeout"]); ok && tv > 0 {
				tInt := int(tv)
				req.Timeout = &tInt
			}
			req.Image = body["image"]
			if imgs, ok := body["images"].([]any); ok {
				req.Images = imgs
			}
			if refStr, ok := body["reference_image"].(string); ok && refStr != "" {
				req.ReferenceImage = refStr
			} else if iuStr, ok := body["image_url"].(string); ok && iuStr != "" {
				req.ReferenceImage = iuStr
			}
			if req.ReferenceImage == "" {
				req.ReferenceImage = req.ExtractReferenceImage()
			}
		}

		if strings.TrimSpace(req.Prompt) == "" {
			req.Prompt = "Generate an image based on this reference image"
		}
		refImg := req.ExtractReferenceImage()
		fullPrompt := BuildImagePrompt(req)
		genTimeout := CFG.ImageTimeout
		if req.Timeout != nil && *req.Timeout > 0 {
			genTimeout = *req.Timeout
		}
		if req.Async {
			queueImage(w, r, req, fullPrompt, refImg, "")
			return
		}
		res, _, err := RunGeneration(fullPrompt, "image", genTimeout, "", nil, refImg, nil)
		if err != nil {
			if isAuthErr(err) {
				writeErr(w, r, http.StatusUnauthorized, err.Error())
			} else {
				writeErr(w, r, http.StatusBadGateway, err.Error())
			}
			return
		}
		writeJSON(w, http.StatusOK, imageResponse(req, res))
	})

	createVideoHandler := func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		var req VideoRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.Prompt) == "" {
			writeErr(w, r, http.StatusUnprocessableEntity, "Invalid request parameters")
			return
		}
		refImg := req.ExtractReferenceImage()
		prompt := BuildVideoPrompt(req)
		timeout := CFG.VideoTimeout
		if req.Timeout != nil && *req.Timeout > 0 {
			timeout = *req.Timeout
		}
		task := store.CreateTask("video", req.Prompt)
		tid, _ := task["id"].(string)
		store.UpdateTask(tid, map[string]any{"api_prompt": prompt, "progress": 10})

		go func() {
			t0 := time.Now()
			onAcquired := func() {
				t0 = time.Now()
				store.UpdateTask(tid, map[string]any{"status": "processing", "progress": 15})
			}
			res, accID, err := RunGeneration(prompt, "video", timeout, "", func(p int) {
				store.UpdateTask(tid, map[string]any{"progress": p})
			}, refImg, onAcquired)
			elapsed := float64(int(time.Since(t0).Seconds()*10)) / 10.0
			if err != nil {
				store.UpdateTask(tid, map[string]any{
					"status":  "failed",
					"elapsed": elapsed,
					"error":   err.Error(),
				})
				return
			}
			fname, _ := res["filename"].(string)
			vurl := mediaURL(fname)
			store.UpdateTask(tid, map[string]any{
				"status":   "completed",
				"progress": 100,
				"account":  accID,
				"elapsed":  elapsed,
				"url":      vurl,
				"video":    map[string]any{"url": vurl},
				"result": map[string]any{
					"url":      vurl,
					"filename": fname,
					"bytes":    res["size"],
					"kind":     res["kind"],
				},
			})
		}()

		writeJSON(w, http.StatusOK, map[string]any{
			"id":         tid,
			"task_id":    tid,
			"object":     "video.task",
			"status":     "queued",
			"progress":   10,
			"created_at": task["created_at"],
		})
	}
	mux.HandleFunc("POST /v1/videos", createVideoHandler)
	mux.HandleFunc("POST /v1/videos/generations", createVideoHandler)

	getVideoHandler := func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		taskID := r.PathValue("task_id")
		t := store.GetTask(taskID)
		if t == nil {
			writeErr(w, r, http.StatusNotFound, "Task not found")
			return
		}
		out := cloneMap(t)
		status, _ := out["status"].(string)
		if status == "succeeded" || status == "success" || status == "done" {
			out["status"] = "completed"
			status = "completed"
		}
		if status == "completed" {
			out["progress"] = 100
		} else if status == "processing" {
			createdAt, _ := toFloat64(out["created_at"])
			elapsed := nowFloat() - createdAt
			calcProg := int(20 + elapsed*1.1)
			if calcProg > 92 {
				calcProg = 92
			}
			curProg, _ := toInt64(out["progress"])
			if int(curProg) > calcProg {
				calcProg = int(curProg)
			}
			out["progress"] = calcProg
		}
		vurl, _ := out["url"].(string)
		if vurl == "" {
			if resMap, ok := out["result"].(map[string]any); ok {
				vurl, _ = resMap["url"].(string)
			}
		}
		if vurl != "" {
			out["url"] = vurl
			if _, hasVid := out["video"]; !hasVid {
				out["video"] = map[string]any{"url": vurl}
			}
		}
		writeJSON(w, http.StatusOK, out)
	}
	mux.HandleFunc("GET /v1/videos/{task_id}", getVideoHandler)
	mux.HandleFunc("GET /v1/videos/generations/{task_id}", getVideoHandler)

	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		var req ChatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, r, http.StatusUnprocessableEntity, "Invalid request parameters")
			return
		}
		prompt := BuildChatPrompt(req.Messages)
		if prompt == "" {
			prompt = strings.TrimSpace(req.Prompt)
		}
		if prompt == "" {
			writeErr(w, r, http.StatusBadRequest, "messages cannot be empty")
			return
		}

		cid := "chatcmpl-" + randomHex(24)
		created := nowUnix()
		model := ResolveModel(req.Model, "muse-spark")

		if len(req.Tools) == 0 && (strings.Contains(prompt, "Compress into one routing hint") || (req.MaxTokens != nil && *req.MaxTokens <= 5)) {
			shortHint := "OK"
			if m := ompDescRe.FindStringSubmatch(prompt); len(m) > 1 {
				fields := strings.Fields(m[1])
				if len(fields) > 12 {
					fields = fields[:12]
				}
				shortHint = strings.Join(fields, " ")
			}
			if req.Stream {
				setSSEHeaders(w)
				flusher, _ := w.(http.Flusher)
				_, _ = io.WriteString(w, sseChunk(chatChunk(cid, created, model, map[string]any{"role": "assistant"}, nil)))
				_, _ = io.WriteString(w, sseChunk(chatChunk(cid, created, model, map[string]any{"content": shortHint}, nil)))
				_, _ = io.WriteString(w, sseChunk(chatChunk(cid, created, model, map[string]any{}, "stop")))
				_, _ = io.WriteString(w, "data: [DONE]\n\n")
				if flusher != nil {
					flusher.Flush()
				}
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{
				"id":      cid,
				"object":  "chat.completion",
				"created": created,
				"model":   model,
				"choices": []map[string]any{
					{
						"index":         0,
						"finish_reason": "stop",
						"message":       map[string]any{"role": "assistant", "content": shortHint},
					},
				},
				"usage": map[string]int{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
			})
			return
		}

		toolNote := ""
		if CFG.ToolProtocol {
			toolNote = BuildToolsPrompt(req.Tools)
		}
		if toolNote != "" {
			prompt = InjectTools(prompt, toolNote)
		}

		timeout := CFG.ChatTimeout
		if req.Timeout != nil && *req.Timeout > 0 {
			timeout = *req.Timeout
		}
		acc := store.PickAccount(PickOptions{
			PreferredID: engine.CurrentAccID,
			Rotate:      true,
			RecordUse:   true,
		})
		if acc == nil {
			writeErr(w, r, http.StatusBadRequest, "No available accounts; please import cookies in the admin dashboard first")
			return
		}
		accID, _ := acc["id"].(string)
		cookies := extractCookiesMap(acc["cookies"])
		expires := extractExpMap(acc["cookies_exp"])

		if req.Stream {
			setSSEHeaders(w)
			flusher, _ := w.(http.Flusher)
			flush := func(s string) {
				_, _ = io.WriteString(w, s)
				if flusher != nil {
					flusher.Flush()
				}
			}

			flush(sseChunk(chatChunk(cid, created, model, map[string]any{"role": "assistant"}, nil)))
			ch, cancel := SafeChatStream(cookies, prompt, expires, timeout, accID)
			defer cancel()

			go func() {
				<-r.Context().Done()
				cancel()
			}()

			var streamErr error
			if toolNote == "" {
				for item := range ch {
					if item.Err != nil {
						streamErr = item.Err
						break
					}
					paceText(item.Chunk, 2, 12*time.Millisecond, func(piece string) {
						flush(sseChunk(chatChunk(cid, created, model, map[string]any{"content": piece}, nil)))
					})
				}
				if streamErr == nil {
					flush(sseChunk(chatChunk(cid, created, model, map[string]any{}, "stop")))
				}
			} else {
				var buf strings.Builder
				for item := range ch {
					if item.Err != nil {
						streamErr = item.Err
						break
					}
					buf.WriteString(item.Chunk)
				}
				if streamErr == nil {
					full := buf.String()
					calls, rest := ParseToolCalls(full)
					if len(calls) > 0 {
						if rest != "" {
							paceText(rest, 2, 12*time.Millisecond, func(piece string) {
								flush(sseChunk(chatChunk(cid, created, model, map[string]any{"content": piece}, nil)))
							})
						}
						var head, body []map[string]any
						for i, c := range calls {
							fn, _ := c["function"].(map[string]any)
							head = append(head, map[string]any{
								"index": i,
								"id":    c["id"],
								"type":  "function",
								"function": map[string]any{
									"name":      fn["name"],
									"arguments": "",
								},
							})
							body = append(body, map[string]any{
								"index": i,
								"function": map[string]any{
									"arguments": fn["arguments"],
								},
							})
						}
						flush(sseChunk(chatChunk(cid, created, model, map[string]any{"tool_calls": head}, nil)))
						flush(sseChunk(chatChunk(cid, created, model, map[string]any{"tool_calls": body}, nil)))
						flush(sseChunk(chatChunk(cid, created, model, map[string]any{}, "tool_calls")))
					} else {
						if full != "" {
							paceText(full, 2, 12*time.Millisecond, func(piece string) {
								flush(sseChunk(chatChunk(cid, created, model, map[string]any{"content": piece}, nil)))
							})
						}
						flush(sseChunk(chatChunk(cid, created, model, map[string]any{}, "stop")))
					}
				}
			}

			if streamErr != nil {
				if isAuthErr(streamErr) {
					store.Mark(accID, false, streamErr.Error())
					flush(sseChunk(map[string]any{"error": map[string]any{"message": streamErr.Error(), "type": "auth_error", "code": 401}}))
				} else if isGenErr(streamErr) {
					note := streamErr.Error()
					if len(note) > 60 {
						note = note[:60]
					}
					store.Mark(accID, true, "Assistant timeout: "+note)
					engine.ResetThread(accID, true)
					flush(sseChunk(map[string]any{"error": map[string]any{"message": streamErr.Error(), "type": "server_error", "code": 502}}))
				} else {
					flush(sseChunk(map[string]any{"error": map[string]any{"message": "Internal error: " + streamErr.Error(), "type": "server_error", "code": 500}}))
				}
				return
			}

			if wantUsage(req.StreamOptions) {
				flush(sseChunk(map[string]any{
					"id":      cid,
					"object":  "chat.completion.chunk",
					"created": created,
					"model":   model,
					"choices": []any{},
					"usage":   map[string]int{"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
				}))
			}
			flush("data: [DONE]\n\n")
			store.Mark(accID, true, "")
			return
		}

		ch, cancel := SafeChatStream(cookies, prompt, expires, timeout, accID)
		defer cancel()
		var buf strings.Builder
		for item := range ch {
			if item.Err != nil {
				if isAuthErr(item.Err) {
					writeErr(w, r, http.StatusUnauthorized, item.Err.Error())
				} else {
					writeErr(w, r, http.StatusBadGateway, item.Err.Error())
				}
				return
			}
			buf.WriteString(item.Chunk)
		}
		text := buf.String()
		usage := map[string]int{"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
		if toolNote != "" {
			calls, rest := ParseToolCalls(text)
			if len(calls) > 0 {
				var restVal any
				if rest != "" {
					restVal = rest
				}
				writeJSON(w, http.StatusOK, map[string]any{
					"id":      cid,
					"object":  "chat.completion",
					"created": created,
					"model":   model,
					"choices": []map[string]any{
						{
							"index":         0,
							"finish_reason": "tool_calls",
							"message": map[string]any{
								"role":       "assistant",
								"content":    restVal,
								"tool_calls": calls,
							},
						},
					},
					"usage": usage,
				})
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"id":      cid,
			"object":  "chat.completion",
			"created": created,
			"model":   model,
			"choices": []map[string]any{
				{
					"index":         0,
					"finish_reason": "stop",
					"message": map[string]any{
						"role":    "assistant",
						"content": text,
					},
				},
			},
			"usage": usage,
		})
	})

	mux.HandleFunc("POST /v1/responses", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		var req ResponsesRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, r, http.StatusUnprocessableEntity, "Invalid request parameters")
			return
		}
		prompt := BuildChatPrompt(ResponsesToChatMessages(req))
		if prompt == "" {
			writeErr(w, r, http.StatusBadRequest, "input cannot be empty")
			return
		}
		toolNote := ""
		if CFG.ToolProtocol {
			toolNote = BuildToolsPrompt(req.Tools)
		}
		if toolNote != "" {
			prompt = InjectTools(prompt, toolNote)
		}
		model := ResolveModel(req.Model, "muse-spark")
		timeout := CFG.ChatTimeout
		if req.Timeout != nil && *req.Timeout > 0 {
			timeout = *req.Timeout
		}
		acc := store.PickAccount(PickOptions{
			PreferredID: engine.CurrentAccID,
			Rotate:      true,
			RecordUse:   true,
		})
		if acc == nil {
			writeErr(w, r, http.StatusBadRequest, "No available accounts; please import cookies in the admin dashboard first")
			return
		}
		rid := "resp_" + randomHex(24)
		mid := "msg_" + randomHex(24)
		created := nowUnix()
		accID, _ := acc["id"].(string)
		cookies := extractCookiesMap(acc["cookies"])
		expires := extractExpMap(acc["cookies_exp"])

		envelope := func(status, text string, calls []map[string]any) map[string]any {
			done := status == "completed"
			var output []map[string]any
			if text != "" || len(calls) == 0 {
				msgStatus := "in_progress"
				if done {
					msgStatus = "completed"
				}
				var content []map[string]any
				if text != "" {
					content = []map[string]any{{"type": "output_text", "text": text, "annotations": []any{}}}
				} else {
					content = []map[string]any{}
				}
				output = append(output, map[string]any{
					"type":    "message",
					"id":      mid,
					"role":    "assistant",
					"status":  msgStatus,
					"content": content,
				})
			}
			for _, c := range calls {
				fn, _ := c["function"].(map[string]any)
				output = append(output, map[string]any{
					"type":      "function_call",
					"id":        "fc_" + randomHex(20),
					"call_id":   c["id"],
					"name":      fn["name"],
					"arguments": fn["arguments"],
					"status":    "completed",
				})
			}
			return map[string]any{
				"id":         rid,
				"object":     "response",
				"created_at": created,
				"status":     status,
				"model":      model,
				"output":     output,
				"usage":      map[string]int{"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
			}
		}

		if req.Stream {
			setSSEHeaders(w)
			flusher, _ := w.(http.Flusher)
			flush := func(s string) {
				_, _ = io.WriteString(w, s)
				if flusher != nil {
					flusher.Flush()
				}
			}
			ch, cancel := SafeChatStream(cookies, prompt, expires, timeout, accID)
			defer cancel()
			go func() {
				<-r.Context().Done()
				cancel()
			}()

			flush(sseEvent("response.created", map[string]any{
				"type":     "response.created",
				"response": envelope("in_progress", "", nil),
			}))

			if toolNote == "" {
				flush(sseEvent("response.output_item.added", map[string]any{
					"type":         "response.output_item.added",
					"output_index": 0,
					"item":         map[string]any{"id": mid, "type": "message", "role": "assistant", "status": "in_progress", "content": []any{}},
				}))
				flush(sseEvent("response.content_part.added", map[string]any{
					"type":          "response.content_part.added",
					"item_id":       mid,
					"output_index":  0,
					"content_index": 0,
					"part":          map[string]any{"type": "output_text", "text": "", "annotations": []any{}},
				}))
				var full strings.Builder
				for item := range ch {
					if item.Err != nil {
						flush(sseEvent("response.failed", map[string]any{"type": "response.failed", "response": envelope("failed", "", nil)}))
						return
					}
					full.WriteString(item.Chunk)
					flush(sseEvent("response.output_text.delta", map[string]any{
						"type":          "response.output_text.delta",
						"item_id":       mid,
						"output_index":  0,
						"content_index": 0,
						"delta":         item.Chunk,
					}))
				}
				fullStr := full.String()
				flush(sseEvent("response.output_text.done", map[string]any{
					"type":          "response.output_text.done",
					"item_id":       mid,
					"output_index":  0,
					"content_index": 0,
					"text":          fullStr,
				}))
				flush(sseEvent("response.output_item.done", map[string]any{
					"type":         "response.output_item.done",
					"output_index": 0,
					"item": map[string]any{
						"id":      mid,
						"type":    "message",
						"role":    "assistant",
						"status":  "completed",
						"content": []map[string]any{{"type": "output_text", "text": fullStr, "annotations": []any{}}},
					},
				}))
				flush(sseEvent("response.completed", map[string]any{
					"type":     "response.completed",
					"response": envelope("completed", fullStr, nil),
				}))
				return
			}

			var full strings.Builder
			for item := range ch {
				if item.Err != nil {
					flush(sseEvent("response.failed", map[string]any{"type": "response.failed", "response": envelope("failed", "", nil)}))
					return
				}
				full.WriteString(item.Chunk)
			}
			fullStr := full.String()
			calls, rest := ParseToolCalls(fullStr)
			outIdx := 0
			if rest != "" || len(calls) == 0 {
				txt := fullStr
				if len(calls) > 0 {
					txt = rest
				}
				flush(sseEvent("response.output_item.added", map[string]any{
					"type": "response.output_item.added", "output_index": outIdx,
					"item": map[string]any{"id": mid, "type": "message", "role": "assistant", "status": "in_progress", "content": []any{}},
				}))
				flush(sseEvent("response.content_part.added", map[string]any{
					"type": "response.content_part.added", "item_id": mid, "output_index": outIdx, "content_index": 0,
					"part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}},
				}))
				flush(sseEvent("response.output_text.delta", map[string]any{
					"type": "response.output_text.delta", "item_id": mid, "output_index": outIdx, "content_index": 0, "delta": txt,
				}))
				flush(sseEvent("response.output_text.done", map[string]any{
					"type": "response.output_text.done", "item_id": mid, "output_index": outIdx, "content_index": 0, "text": txt,
				}))
				flush(sseEvent("response.output_item.done", map[string]any{
					"type": "response.output_item.done", "output_index": outIdx,
					"item": map[string]any{
						"id": mid, "type": "message", "role": "assistant", "status": "completed",
						"content": []map[string]any{{"type": "output_text", "text": txt, "annotations": []any{}}},
					},
				}))
				outIdx++
			}
			for _, c := range calls {
				fn, _ := c["function"].(map[string]any)
				fcID := "fc_" + randomHex(20)
				fcItem := map[string]any{
					"id":        fcID,
					"type":      "function_call",
					"call_id":   c["id"],
					"name":      fn["name"],
					"arguments": fn["arguments"],
					"status":    "completed",
				}
				flush(sseEvent("response.output_item.added", map[string]any{
					"type": "response.output_item.added", "output_index": outIdx,
					"item": map[string]any{
						"id": fcID, "type": "function_call", "call_id": c["id"],
						"name": fn["name"], "arguments": "", "status": "in_progress",
					},
				}))
				flush(sseEvent("response.function_call_arguments.delta", map[string]any{
					"type": "response.function_call_arguments.delta", "item_id": fcID, "output_index": outIdx,
					"delta": fn["arguments"],
				}))
				flush(sseEvent("response.function_call_arguments.done", map[string]any{
					"type": "response.function_call_arguments.done", "item_id": fcID, "output_index": outIdx,
					"arguments": fn["arguments"],
				}))
				flush(sseEvent("response.output_item.done", map[string]any{
					"type": "response.output_item.done", "output_index": outIdx, "item": fcItem,
				}))
				outIdx++
			}
			finalTxt := fullStr
			if len(calls) > 0 {
				finalTxt = rest
			}
			flush(sseEvent("response.completed", map[string]any{
				"type":     "response.completed",
				"response": envelope("completed", finalTxt, calls),
			}))
			return
		}

		ch, cancel := SafeChatStream(cookies, prompt, expires, timeout, accID)
		defer cancel()
		var buf strings.Builder
		for item := range ch {
			if item.Err != nil {
				if isAuthErr(item.Err) {
					writeErr(w, r, http.StatusUnauthorized, item.Err.Error())
				} else {
					writeErr(w, r, http.StatusBadGateway, item.Err.Error())
				}
				return
			}
			buf.WriteString(item.Chunk)
		}
		text := buf.String()
		if toolNote != "" {
			calls, rest := ParseToolCalls(text)
			if len(calls) > 0 {
				writeJSON(w, http.StatusOK, envelope("completed", rest, calls))
				return
			}
		}
		writeJSON(w, http.StatusOK, envelope("completed", text, nil))
	})

	// Anthropic Messages API
	mux.HandleFunc("GET /api/hello", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
	mux.HandleFunc("HEAD /api/hello", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("POST /v1/messages/count_tokens", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		var req AnthropicMessagesRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		prompt := BuildChatPrompt(AnthropicToChatMessages(req))
		toolNote := ""
		if CFG.ToolProtocol {
			toolNote = BuildToolsPrompt(req.Tools)
		}
		inTokens := (len(prompt) + len(toolNote)) / 4
		if inTokens < 1 {
			inTokens = 1
		}
		writeJSON(w, http.StatusOK, map[string]any{"input_tokens": inTokens})
	})

	mux.HandleFunc("POST /v1/messages", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		var req AnthropicMessagesRequest
		req.Model = "muse-spark"
		req.MaxTokens = 4096
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, r, http.StatusUnprocessableEntity, "Invalid request parameters")
			return
		}
		mid := "msg_" + randomHex(24)

		if req.MaxTokens <= 5 && len(req.Tools) == 0 {
			if req.Stream {
				setSSEHeaders(w)
				flusher, _ := w.(http.Flusher)
				_, _ = io.WriteString(w, sseEvent("message_start", map[string]any{
					"type": "message_start",
					"message": map[string]any{
						"id": mid, "type": "message", "role": "assistant",
						"model": req.Model, "content": []any{},
						"stop_reason": nil, "stop_sequence": nil,
						"usage": map[string]int{"input_tokens": 1, "output_tokens": 1},
					},
				}))
				_, _ = io.WriteString(w, sseEvent("content_block_start", map[string]any{
					"type": "content_block_start", "index": 0,
					"content_block": map[string]any{"type": "text", "text": ""},
				}))
				_, _ = io.WriteString(w, sseEvent("content_block_delta", map[string]any{
					"type": "content_block_delta", "index": 0,
					"delta": map[string]any{"type": "text_delta", "text": "OK"},
				}))
				_, _ = io.WriteString(w, sseEvent("content_block_stop", map[string]any{"type": "content_block_stop", "index": 0}))
				_, _ = io.WriteString(w, sseEvent("message_delta", map[string]any{
					"type":  "message_delta",
					"delta": map[string]any{"stop_reason": "end_turn", "stop_sequence": nil},
					"usage": map[string]int{"output_tokens": 1},
				}))
				_, _ = io.WriteString(w, sseEvent("message_stop", map[string]any{"type": "message_stop"}))
				if flusher != nil {
					flusher.Flush()
				}
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{
				"id": mid, "type": "message", "role": "assistant", "model": req.Model,
				"content":       []map[string]any{{"type": "text", "text": "OK"}},
				"stop_reason":   "end_turn",
				"stop_sequence": nil,
				"usage":         map[string]int{"input_tokens": 1, "output_tokens": 1},
			})
			return
		}

		prompt := BuildChatPrompt(AnthropicToChatMessages(req))
		if prompt == "" {
			writeErr(w, r, http.StatusBadRequest, "messages cannot be empty")
			return
		}
		toolNote := ""
		if CFG.ToolProtocol {
			toolNote = BuildToolsPrompt(req.Tools)
		}
		if toolNote != "" {
			prompt = InjectTools(prompt, toolNote)
		}
		timeout := CFG.ChatTimeout
		if req.Timeout != nil && *req.Timeout > 0 {
			timeout = *req.Timeout
		}
		acc := store.PickAccount(PickOptions{
			PreferredID: engine.CurrentAccID,
			Rotate:      true,
			RecordUse:   true,
		})
		if acc == nil {
			writeErr(w, r, http.StatusBadRequest, "No available accounts; please import cookies in the admin dashboard first")
			return
		}
		accID, _ := acc["id"].(string)
		cookies := extractCookiesMap(acc["cookies"])
		expires := extractExpMap(acc["cookies_exp"])
		inTokens := len(prompt) / 4
		if inTokens < 1 {
			inTokens = 1
		}

		if req.Stream {
			setSSEHeaders(w)
			flusher, _ := w.(http.Flusher)
			flush := func(s string) {
				_, _ = io.WriteString(w, s)
				if flusher != nil {
					flusher.Flush()
				}
			}
			ch, cancel := SafeChatStream(cookies, prompt, expires, timeout, accID)
			defer cancel()
			go func() {
				<-r.Context().Done()
				cancel()
			}()

			flush(sseEvent("message_start", map[string]any{
				"type": "message_start",
				"message": map[string]any{
					"id": mid, "type": "message", "role": "assistant",
					"model": req.Model, "content": []any{},
					"stop_reason": nil, "stop_sequence": nil,
					"usage": map[string]int{"input_tokens": inTokens, "output_tokens": 1},
				},
			}))

			if toolNote == "" {
				flush(sseEvent("content_block_start", map[string]any{
					"type": "content_block_start", "index": 0,
					"content_block": map[string]any{"type": "text", "text": ""},
				}))
				outLen := 0
				for item := range ch {
					if item.Err != nil {
						flush(sseEvent("error", map[string]any{"type": "error", "error": map[string]any{"type": "api_error", "message": item.Err.Error()}}))
						return
					}
					outLen += len(item.Chunk)
					paceText(item.Chunk, 2, 12*time.Millisecond, func(piece string) {
						flush(sseEvent("content_block_delta", map[string]any{
							"type": "content_block_delta", "index": 0,
							"delta": map[string]any{"type": "text_delta", "text": piece},
						}))
					})
				}
				flush(sseEvent("content_block_stop", map[string]any{"type": "content_block_stop", "index": 0}))
				outTok := outLen / 4
				if outTok < 1 {
					outTok = 1
				}
				flush(sseEvent("message_delta", map[string]any{
					"type":  "message_delta",
					"delta": map[string]any{"stop_reason": "end_turn", "stop_sequence": nil},
					"usage": map[string]int{"output_tokens": outTok},
				}))
				flush(sseEvent("message_stop", map[string]any{"type": "message_stop"}))
				store.Mark(accID, true, "")
				return
			}

			var buf strings.Builder
			for item := range ch {
				if item.Err != nil {
					flush(sseEvent("error", map[string]any{"type": "error", "error": map[string]any{"type": "api_error", "message": item.Err.Error()}}))
					return
				}
				buf.WriteString(item.Chunk)
			}
			fullStr := buf.String()
			calls, rest := ParseToolCalls(fullStr)
			outTok := len(fullStr) / 4
			if outTok < 1 {
				outTok = 1
			}
			idx := 0
			if len(calls) > 0 {
				if rest != "" {
					flush(sseEvent("content_block_start", map[string]any{
						"type": "content_block_start", "index": idx,
						"content_block": map[string]any{"type": "text", "text": ""},
					}))
					paceText(rest, 2, 12*time.Millisecond, func(piece string) {
						flush(sseEvent("content_block_delta", map[string]any{
							"type": "content_block_delta", "index": idx,
							"delta": map[string]any{"type": "text_delta", "text": piece},
						}))
					})
					flush(sseEvent("content_block_stop", map[string]any{"type": "content_block_stop", "index": idx}))
					idx++
				}
				for _, c := range calls {
					fn, _ := c["function"].(map[string]any)
					tid := "toolu_" + randomHex(20)
					argsStr, _ := fn["arguments"].(string)
					if argsStr == "" {
						argsStr = "{}"
					}
					flush(sseEvent("content_block_start", map[string]any{
						"type": "content_block_start", "index": idx,
						"content_block": map[string]any{
							"type": "tool_use", "id": tid, "name": fn["name"], "input": map[string]any{},
						},
					}))
					flush(sseEvent("content_block_delta", map[string]any{
						"type": "content_block_delta", "index": idx,
						"delta": map[string]any{"type": "input_json_delta", "partial_json": argsStr},
					}))
					flush(sseEvent("content_block_stop", map[string]any{"type": "content_block_stop", "index": idx}))
					idx++
				}
				flush(sseEvent("message_delta", map[string]any{
					"type":  "message_delta",
					"delta": map[string]any{"stop_reason": "tool_use", "stop_sequence": nil},
					"usage": map[string]int{"output_tokens": outTok},
				}))
			} else {
				flush(sseEvent("content_block_start", map[string]any{
					"type": "content_block_start", "index": 0,
					"content_block": map[string]any{"type": "text", "text": ""},
				}))
				if fullStr != "" {
					paceText(fullStr, 2, 12*time.Millisecond, func(piece string) {
						flush(sseEvent("content_block_delta", map[string]any{
							"type": "content_block_delta", "index": 0,
							"delta": map[string]any{"type": "text_delta", "text": piece},
						}))
					})
				}
				flush(sseEvent("content_block_stop", map[string]any{"type": "content_block_stop", "index": 0}))
				flush(sseEvent("message_delta", map[string]any{
					"type":  "message_delta",
					"delta": map[string]any{"stop_reason": "end_turn", "stop_sequence": nil},
					"usage": map[string]int{"output_tokens": outTok},
				}))
			}
			flush(sseEvent("message_stop", map[string]any{"type": "message_stop"}))
			store.Mark(accID, true, "")
			return
		}

		ch, cancel := SafeChatStream(cookies, prompt, expires, timeout, accID)
		defer cancel()
		var buf strings.Builder
		for item := range ch {
			if item.Err != nil {
				if isAuthErr(item.Err) {
					writeErr(w, r, http.StatusUnauthorized, item.Err.Error())
				} else {
					writeErr(w, r, http.StatusBadGateway, item.Err.Error())
				}
				return
			}
			buf.WriteString(item.Chunk)
		}
		text := buf.String()
		outTokens := len(text) / 4
		if outTokens < 1 {
			outTokens = 1
		}
		if toolNote != "" {
			calls, rest := ParseToolCalls(text)
			if len(calls) > 0 {
				writeJSON(w, http.StatusOK, map[string]any{
					"id": mid, "type": "message", "role": "assistant", "model": req.Model,
					"content":       CallsToAnthropicBlocks(calls, rest),
					"stop_reason":   "tool_use",
					"stop_sequence": nil,
					"usage":         map[string]int{"input_tokens": inTokens, "output_tokens": outTokens},
				})
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"id": mid, "type": "message", "role": "assistant", "model": req.Model,
			"content":       []map[string]any{{"type": "text", "text": text}},
			"stop_reason":   "end_turn",
			"stop_sequence": nil,
			"usage":         map[string]int{"input_tokens": inTokens, "output_tokens": outTokens},
		})
	})

	mux.HandleFunc("GET /v1/media/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if strings.Contains(name, "/") || strings.Contains(name, `\`) || strings.Contains(name, "..") {
			writeErr(w, r, http.StatusBadRequest, "Invalid filename")
			return
		}
		p := filepath.Join(CFG.MediaDir(), name)
		st, err := os.Stat(p)
		if err != nil || st.IsDir() {
			writeErr(w, r, http.StatusNotFound, "File not found")
			return
		}
		http.ServeFile(w, r, p)
	})

	// ---------- Admin Endpoints ----------

	mux.HandleFunc("GET /admin/status", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		base := publicBase()
		baseURL := ""
		if base != "" {
			baseURL = base + "/v1"
		}
		mediaCnt := 0
		if entries, err := os.ReadDir(CFG.MediaDir()); err == nil {
			mediaCnt = len(entries)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"accounts":          store.ListAccounts(),
			"stats":             store.Stats(),
			"tasks":             store.ListTasks(20),
			"media_count":       mediaCnt,
			"browser_running":   engine.BrowserRunning(),
			"essential_cookies": EssentialCookies,
			"base_url":          baseURL,
			"config": map[string]any{
				"site":          CFG.SiteURL,
				"cdp_port":      CFG.CDPPort,
				"image_timeout": CFG.ImageTimeout,
				"video_timeout": CFG.VideoTimeout,
				"host":          CFG.Host,
				"port":          CFG.Port,
				"public_base":   base,
			},
		})
	})

	mux.HandleFunc("GET /admin/accounts", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"accounts": store.ListAccounts(),
			"stats":    store.Stats(),
		})
	})

	mux.HandleFunc("POST /admin/accounts", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		var req AccountRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, r, http.StatusBadRequest, "No cookies parsed; please check input format")
			return
		}
		var added []map[string]any
		var seen []map[string]string

		if req.Batch != "" {
			for _, b := range ParseBatch(req.Batch) {
				acc := store.AddAccount(b.Cookies, b.Label, nil)
				seen = append(seen, b.Cookies)
				added = append(added, map[string]any{
					"id":           acc["id"],
					"label":        acc["label"],
					"cookie_count": len(b.Cookies),
					"expires_at":   acc["expires_at"],
				})
			}
		}

		cookies := map[string]string{}
		for k, v := range req.Cookies {
			cookies[k] = v
		}
		if req.CookieHeader != "" {
			for k, v := range ParseCookieText(req.CookieHeader) {
				cookies[k] = v
			}
		}
		if len(cookies) > 0 {
			acc := store.AddAccount(cookies, req.Label, req.Expires)
			seen = append(seen, cookies)
			added = append(added, map[string]any{
				"id":           acc["id"],
				"label":        acc["label"],
				"cookie_count": len(cookies),
				"expires_at":   acc["expires_at"],
			})
		}

		if len(added) == 0 {
			writeErr(w, r, http.StatusBadRequest, "No cookies parsed; please check input format")
			return
		}

		var missing []string
		for _, n := range EssentialCookies {
			found := false
			for _, c := range seen {
				if c[n] != "" {
					found = true
					break
				}
			}
			if !found {
				missing = append(missing, n)
			}
		}
		if missing == nil {
			missing = []string{}
		}
		warning := ""
		if len(missing) > 0 {
			warning = fmt.Sprintf("Missing essential cookies: %s; this account may fail to generate", strings.Join(missing, ", "))
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"added":             added,
			"count":             len(added),
			"essential_missing": missing,
			"warning":           warning,
		})
	})

	mux.HandleFunc("PATCH /admin/accounts/{aid}", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		aid := r.PathValue("aid")
		var req AccountPatch
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, r, http.StatusBadRequest, "Invalid request parameters")
			return
		}
		fields := map[string]any{}
		if req.Label != nil {
			fields["label"] = *req.Label
		}
		if req.Enabled != nil {
			fields["enabled"] = *req.Enabled
		}
		acc := store.UpdateAccount(aid, fields)
		if acc == nil {
			writeErr(w, r, http.StatusNotFound, "Account not found")
			return
		}
		out := map[string]any{}
		for k, v := range acc {
			if k != "cookies" {
				out[k] = v
			}
		}
		out["cookie_count"] = len(extractCookiesMap(acc["cookies"]))
		writeJSON(w, http.StatusOK, out)
	})

	mux.HandleFunc("DELETE /admin/accounts/{aid}", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		aid := r.PathValue("aid")
		if !store.DeleteAccount(aid) {
			writeErr(w, r, http.StatusNotFound, "Account not found")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"deleted": true, "id": aid})
	})

	testAccountHandler := func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		aid := r.PathValue("aid")
		acc := store.GetAccount(aid)
		if acc == nil {
			writeErr(w, r, http.StatusNotFound, "Account not found")
			return
		}
		if len(extractCookiesMap(acc["cookies"])) == 0 {
			writeErr(w, r, http.StatusBadRequest, "Account has no cookies")
			return
		}
		res := ProbeAccountForTest(aid)
		writeJSON(w, http.StatusOK, res)
	}
	mux.HandleFunc("POST /admin/accounts/{aid}/test", testAccountHandler)
	mux.HandleFunc("POST /admin/accounts/{aid}/relogin", testAccountHandler)

	mux.HandleFunc("POST /admin/accounts/{aid}/cookies", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		aid := r.PathValue("aid")
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeErr(w, r, http.StatusBadRequest, "No cookies parsed")
			return
		}
		cookies := extractCookiesMap(payload["cookies"])
		if ch, ok := payload["cookie_header"].(string); ok && ch != "" {
			for k, v := range ParseCookieText(ch) {
				cookies[k] = v
			}
		}
		if len(cookies) == 0 {
			writeErr(w, r, http.StatusBadRequest, "No cookies parsed")
			return
		}
		exp := extractExpMap(payload["expires"])
		fields := map[string]any{
			"cookies":       cookies,
			"ok_nil":        true,
			"note":          "Cookies updated",
			"expiry_anchor": nowUnix(),
		}
		if len(exp) > 0 {
			fields["cookies_exp"] = exp
		}
		acc := store.UpdateAccount(aid, fields)
		if acc == nil {
			writeErr(w, r, http.StatusNotFound, "Account not found")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":           true,
			"id":           aid,
			"cookie_count": len(cookies),
			"expires_at":   acc["expires_at"],
		})
	})

	mux.HandleFunc("POST /admin/relogin", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		acc := store.PickAccount(PickOptions{Rotate: true, RecordUse: true})
		if acc == nil {
			writeErr(w, r, http.StatusBadRequest, "No available accounts")
			return
		}
		aid, _ := acc["id"].(string)
		if err := engine.Start(); err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		if _, err := engine.Refresh(extractCookiesMap(acc["cookies"]), extractExpMap(acc["cookies_exp"]), aid); err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "account": aid})
	})

	queryAccountQuota := func(w http.ResponseWriter, r *http.Request, aid string) {
		acc := store.GetAccount(aid)
		if acc == nil {
			writeErr(w, r, http.StatusNotFound, "Account not found")
			return
		}
		if len(extractCookiesMap(acc["cookies"])) == 0 {
			writeErr(w, r, http.StatusBadRequest, "Account has no cookies")
			return
		}
		if !GEN_LOCK.Acquire(true, 60.0) {
			writeErr(w, r, http.StatusBadGateway, "Timed out waiting for browser queue")
			return
		}
		defer GEN_LOCK.Release()

		if err := engine.Start(); err != nil {
			writeErr(w, r, http.StatusBadGateway, err.Error())
			return
		}
		q, err := engine.Quota(extractCookiesMap(acc["cookies"]), extractExpMap(acc["cookies_exp"]), aid)
		if err != nil {
			if isAuthErr(err) {
				msg := err.Error()
				if len(msg) > 200 {
					msg = msg[:200]
				}
				store.Mark(aid, false, msg)
				writeErr(w, r, http.StatusUnauthorized, err.Error())
			} else {
				writeErr(w, r, http.StatusBadGateway, err.Error())
			}
			return
		}
		if q["found"] != true {
			writeErr(w, r, http.StatusBadGateway, "Quota panel returned no valid data; existing cache preserved")
			return
		}
		q["checked_at"] = nowUnix()
		store.UpdateAccount(aid, map[string]any{"quota": q})
		writeJSON(w, http.StatusOK, q)
	}

	mux.HandleFunc("POST /admin/accounts/{aid}/quota", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		queryAccountQuota(w, r, r.PathValue("aid"))
	})

	mux.HandleFunc("POST /admin/quota", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		acc := store.PickAccount(PickOptions{Rotate: true, RecordUse: true})
		if acc == nil {
			writeErr(w, r, http.StatusBadRequest, "No available accounts")
			return
		}
		aid, _ := acc["id"].(string)
		queryAccountQuota(w, r, aid)
	})

	mux.HandleFunc("GET /admin/apikey", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		base := publicBase()
		baseURL, modelsURL, mURL := "", "", "/v1/media/{name}"
		if base != "" {
			baseURL = base + "/v1"
			modelsURL = base + "/v1/models"
			mURL = base + "/v1/media/{name}"
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"api_key":    CFG.APIKey,
			"base_url":   baseURL,
			"models_url": modelsURL,
			"media_url":  mURL,
		})
	})

	mux.HandleFunc("POST /admin/apikey/rotate", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		newKey := "m2a_" + randomHex(48)
		old := CFG.APIKey
		CFG.APIKey = newKey
		PersistEnv(CFG.BaseDir, "MUSE2API_KEY", newKey)
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":       true,
			"api_key":  newKey,
			"previous": old,
			"message":  "New API key generated and active immediately; previous key invalidated",
		})
	})

	mux.HandleFunc("GET /admin/cookie-helper", func(w http.ResponseWriter, r *http.Request) {
		p := filepath.Join(CFG.BaseDir, "tools", "get_muse_cookie.go")
		if _, err := os.Stat(p); err != nil {
			writeErr(w, r, http.StatusNotFound, "Helper script not found")
			return
		}
		if r.URL.Query().Get("download") == "1" {
			w.Header().Set("Content-Disposition", `attachment; filename="get_muse_cookie.go"`)
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		http.ServeFile(w, r, p)
	})

	mux.HandleFunc("GET /admin/extension", func(w http.ResponseWriter, r *http.Request) {
		src := filepath.Join(CFG.BaseDir, "extension")
		entries, err := os.ReadDir(src)
		if err != nil {
			writeErr(w, r, http.StatusNotFound, "Extension directory not found")
			return
		}
		var buf bytes.Buffer
		zw := zip.NewWriter(&buf)
		for _, ent := range entries {
			if ent.IsDir() {
				continue
			}
			fp := filepath.Join(src, ent.Name())
			b, err := os.ReadFile(fp)
			if err != nil {
				continue
			}
			fw, err := zw.Create(filepath.Join("muse2api-extension", ent.Name()))
			if err == nil {
				_, _ = fw.Write(b)
			}
		}
		_ = zw.Close()
		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Disposition", `attachment; filename="muse2api-extension.zip"`)
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(buf.Bytes())
	})

	mux.HandleFunc("GET /admin/extension/files", func(w http.ResponseWriter, r *http.Request) {
		src := filepath.Join(CFG.BaseDir, "extension")
		entries, err := os.ReadDir(src)
		if err != nil {
			writeErr(w, r, http.StatusNotFound, "Extension directory not found")
			return
		}
		var files []string
		for _, ent := range entries {
			if !ent.IsDir() {
				files = append(files, ent.Name())
			}
		}
		sort.Strings(files)
		writeJSON(w, http.StatusOK, map[string]any{"files": files})
	})

	mux.HandleFunc("GET /admin/tasks", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		limit := 50
		if lStr := r.URL.Query().Get("limit"); lStr != "" {
			if n, err := strconv.Atoi(lStr); err == nil && n > 0 {
				limit = n
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"tasks": store.ListTasks(limit)})
	})

	mux.HandleFunc("DELETE /admin/tasks/{tid}", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		tid := r.PathValue("tid")
		if !store.DeleteTask(tid) {
			writeErr(w, r, http.StatusNotFound, "Task not found")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"deleted": true})
	})

	mux.HandleFunc("POST /admin/tasks/clear", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		var payload map[string]any
		_ = json.NewDecoder(r.Body).Decode(&payload)
		keep, _ := toInt64(payload["keep"])
		writeJSON(w, http.StatusOK, map[string]any{"removed": store.ClearTasks(int(keep))})
	})

	mux.HandleFunc("GET /admin/media", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		d := CFG.MediaDir()
		var items []map[string]any
		if entries, err := os.ReadDir(d); err == nil {
			for _, ent := range entries {
				if ent.IsDir() {
					continue
				}
				name := ent.Name()
				p := filepath.Join(d, name)
				st, err := os.Stat(p)
				if err != nil || st.IsDir() {
					continue
				}
				ext := strings.ToLower(filepath.Ext(name))
				kind := "image"
				if hasVideoExt(ext) {
					kind = "video"
				}
				items = append(items, map[string]any{
					"name":  name,
					"url":   mediaURL(name),
					"bytes": st.Size(),
					"mtime": st.ModTime().Unix(),
					"kind":  kind,
				})
			}
		}
		sort.SliceStable(items, func(i, j int) bool {
			mi, _ := toInt64(items[i]["mtime"])
			mj, _ := toInt64(items[j]["mtime"])
			return mi > mj
		})
		if items == nil {
			items = []map[string]any{}
		}
		writeJSON(w, http.StatusOK, map[string]any{"media": items, "count": len(items)})
	})

	mux.HandleFunc("POST /admin/media/delete", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		var payload map[string]any
		_ = json.NewDecoder(r.Body).Decode(&payload)
		var names []any
		switch v := payload["names"].(type) {
		case string:
			names = []any{v}
		case []any:
			names = v
		case nil:
			names = []any{}
		default:
			writeErr(w, r, http.StatusBadRequest, "names must be an array of filenames")
			return
		}
		d := CFG.MediaDir()
		removed := 0
		var errs []string
		for _, nRaw := range names {
			name, ok := nRaw.(string)
			if !ok || name == "" || strings.Contains(name, "/") || strings.Contains(name, `\`) || strings.Contains(name, "..") {
				errs = append(errs, fmt.Sprintf("Invalid filename: %v", nRaw))
				continue
			}
			p := filepath.Join(d, name)
			if st, err := os.Stat(p); err == nil && !st.IsDir() {
				if err := os.Remove(p); err != nil {
					errs = append(errs, fmt.Sprintf("%s: %v", name, err))
				} else {
					removed++
				}
			}
		}
		if errs == nil {
			errs = []string{}
		}
		writeJSON(w, http.StatusOK, map[string]any{"removed": removed, "errors": errs})
	})

	mux.HandleFunc("DELETE /admin/media/{name}", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		name := r.PathValue("name")
		if strings.Contains(name, "/") || strings.Contains(name, `\`) || strings.Contains(name, "..") {
			writeErr(w, r, http.StatusBadRequest, "Invalid filename")
			return
		}
		p := filepath.Join(CFG.MediaDir(), name)
		st, err := os.Stat(p)
		if err != nil || st.IsDir() {
			writeErr(w, r, http.StatusNotFound, "File not found")
			return
		}
		if err := os.Remove(p); err != nil {
			writeErr(w, r, http.StatusInternalServerError, fmt.Sprintf("Failed to delete file: %v", err))
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "deleted": name})
	})

	serveAdminHTML := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		p := filepath.Join(CFG.BaseDir, "admin.html")
		if b, err := os.ReadFile(p); err == nil {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(b)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "<!doctype html><meta charset=utf-8><body style='background:#0b0f19;color:#e6e8ee;font-family:system-ui;padding:40px'><h2>muse2api</h2><p>Admin dashboard file admin.html is missing.</p><p>Available endpoints: <code>/v1/images/generations</code>, <code>/v1/videos</code></p></body>")
	}
	mux.HandleFunc("GET /{$}", serveAdminHTML)
	mux.HandleFunc("GET /admin", serveAdminHTML)

	mux.HandleFunc("POST /admin/accounts/keepalive", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		if KeepaliveState["running"] == true {
			writeJSON(w, http.StatusOK, map[string]any{"status": "busy", "message": "Keepalive task is already running; please wait"})
			return
		}
		force := true
		if fStr := r.URL.Query().Get("force"); fStr == "false" || fStr == "0" {
			force = false
		}
		writeJSON(w, http.StatusOK, RunKeepaliveAll(force))
	})

	mux.HandleFunc("POST /admin/accounts/{aid}/keepalive", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		writeJSON(w, http.StatusOK, ProbeAccountSync(r.PathValue("aid"), false))
	})

	mux.HandleFunc("GET /admin/keepalive/status", func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(w, r) {
			return
		}
		writeJSON(w, http.StatusOK, KeepaliveState)
	})

	// Static version status stub for admin UI compatibility
	versionStub := func(w http.ResponseWriter, r *http.Request) {
		ver := "1.0.0"
		var highlights []any
		if b, err := os.ReadFile(filepath.Join(CFG.BaseDir, "version.json")); err == nil {
			var vj map[string]any
			if json.Unmarshal(b, &vj) == nil {
				if vStr, ok := vj["version"].(string); ok && vStr != "" {
					ver = vStr
				}
				highlights, _ = vj["highlights"].([]any)
			}
		}
		if highlights == nil {
			highlights = []any{}
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"local_version":  ver,
			"remote_version": ver,
			"has_update":     false,
			"up_to_date":     true,
			"highlights":     highlights,
			"recent_commits": []any{},
			"checked_at":     nowUnix(),
		})
	}
	mux.HandleFunc("GET /admin/update/check", versionStub)
	mux.HandleFunc("GET /admin/repo/status", versionStub)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		w.Header().Set("Access-Control-Expose-Headers", "*")
		w.Header().Set("Access-Control-Max-Age", "600")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func main() {
	StartupInit()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("[INFO] [Keepalive Daemon] Started; check interval: 15 minutes")
		time.Sleep(1 * time.Second)
		WarmupBrowserSync()
		ticker := time.NewTicker(15 * time.Minute)
		defer ticker.Stop()
		for {
			RunKeepaliveAll(false)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	addr := fmt.Sprintf("%s:%d", CFG.Host, CFG.Port)
	srv := &http.Server{
		Addr:    addr,
		Handler: NewHandler(),
	}

	go func() {
		log.Printf("[INFO] muse2api (Go) listening on http://%s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[FATAL] HTTP server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("[INFO] Shutting down muse2api...")
	engine.Stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}
