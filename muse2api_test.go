package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func setupTestEnv(t *testing.T, publicBase string) string {
	t.Helper()
	tmp := t.TempDir()
	t.Setenv("MUSE2API_HOME", tmp)
	t.Setenv("MUSE2API_PROFILE_ROOT", tmp)
	t.Setenv("MUSE2API_KEY", "test-only")
	t.Setenv("MUSE2API_PUBLIC_BASE", publicBase)
	InitGlobals(NewConfig())
	return tmp
}

func TestConcurrency(t *testing.T) {
	// 1. ConcurrencyLock
	lock := NewConcurrencyLock(2)
	if lock.Locked() {
		t.Fatal("expected unlocked initially")
	}
	if !lock.Acquire(true, 0.1) || !lock.Locked() {
		t.Fatal("slot 1 acquire failed")
	}
	if !lock.Acquire(true, 0.1) || !lock.Locked() {
		t.Fatal("slot 2 acquire failed")
	}
	if lock.Acquire(true, 0.05) {
		t.Fatal("slot 3 should time out")
	}
	lock.Release()
	if !lock.Locked() {
		t.Fatal("should still be locked with 1 active")
	}
	if !lock.Acquire(true, 0.1) {
		t.Fatal("slot 3 should succeed after release")
	}
	lock.Release()
	lock.Release()
	if lock.Locked() {
		t.Fatal("expected unlocked after all released")
	}

	// 2. AccountPoolManager
	setupTestEnv(t, "")
	a1 := store.AddAccount(map[string]string{"k": "v1"}, "A1", nil)
	a2 := store.AddAccount(map[string]string{"k": "v2"}, "A2", nil)
	id1, _ := a1["id"].(string)
	id2, _ := a2["id"].(string)

	got1, err := ACCOUNT_POOL.Acquire(AcquireAccountOptions{TimeoutSec: 0.5})
	if err != nil || got1["id"] != id1 || !ACCOUNT_POOL.BusyAccounts[id1] {
		t.Fatalf("unexpected first acquire: %v, err=%v", got1, err)
	}
	got2, err := ACCOUNT_POOL.Acquire(AcquireAccountOptions{TimeoutSec: 0.5})
	if err != nil || got2["id"] != id2 || !ACCOUNT_POOL.BusyAccounts[id2] {
		t.Fatalf("unexpected second acquire: %v, err=%v", got2, err)
	}
	t0 := time.Now()
	if _, err := ACCOUNT_POOL.Acquire(AcquireAccountOptions{TimeoutSec: 0.1}); err == nil {
		t.Fatal("expected timeout when both accounts are busy")
	}
	if time.Since(t0) < 80*time.Millisecond {
		t.Fatalf("expected wait >= 80ms, got %v", time.Since(t0))
	}
	ACCOUNT_POOL.Release(id1)
	if ACCOUNT_POOL.BusyAccounts[id1] {
		t.Fatal("id1 should no longer be busy")
	}
	got3, err := ACCOUNT_POOL.Acquire(AcquireAccountOptions{TimeoutSec: 0.5})
	if err != nil || got3["id"] != id1 {
		t.Fatalf("expected id1 after release, got %v (err=%v)", got3, err)
	}
	ACCOUNT_POOL.Release(id2)
	ACCOUNT_POOL.Release(id1)

	// 3. Parallel generation simulation
	var mu sync.Mutex
	activeWorkers := 0
	concurrencyObserved := false
	seenAccounts := map[string]bool{}

	renewAndPersistHook = func(accID string, wakeVM, force bool) (map[string]any, error) {
		return nil, nil
	}
	syncCookiesHook = func(accID string) map[string]any {
		return nil
	}
	engine.StartFunc = func() error { return nil }
	engine.GenerateFunc = func(cookies map[string]string, prompt, expect string, timeout int, expires map[string]int64, accountID string, onProgress func(int), referenceImage string, stopCh <-chan struct{}) (map[string]any, error) {
		mu.Lock()
		activeWorkers++
		seenAccounts[accountID] = true
		if activeWorkers >= 2 {
			concurrencyObserved = true
		}
		mu.Unlock()
		time.Sleep(120 * time.Millisecond)
		mu.Lock()
		activeWorkers--
		mu.Unlock()
		return map[string]any{"filename": accountID + ".png", "size": 1234, "kind": expect}, nil
	}
	defer func() {
		renewAndPersistHook = nil
		syncCookiesHook = nil
		engine.StartFunc = nil
		engine.GenerateFunc = nil
	}()

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, _, err := RunGeneration("task", "image", 5, "", nil, "", nil)
			if err != nil {
				t.Errorf("worker %d failed: %v", idx, err)
			}
		}(i)
	}
	wg.Wait()

	if !concurrencyObserved {
		t.Fatal("expected 2 workers to run concurrently")
	}
	if !seenAccounts[id1] || !seenAccounts[id2] {
		t.Fatalf("expected both accounts used, got %v", seenAccounts)
	}
	if len(ACCOUNT_POOL.BusyAccounts) != 0 {
		t.Fatalf("expected 0 busy accounts after completion, got %v", ACCOUNT_POOL.BusyAccounts)
	}
}

func TestKeepaliveScheduling(t *testing.T) {
	setupTestEnv(t, "")
	origNow := nowUnix
	origSleep := sleepSec
	defer func() {
		nowUnix = origNow
		sleepSec = origSleep
		probeAccountSyncHook = nil
	}()
	sleepSec = func(float64) {}

	// 1. Maintenance selection does not count as use
	acc := store.AddAccount(map[string]string{"hatch_sess": "fixture"}, "fixture", nil)
	acc["last_used"] = float64(1234)
	acc["use_count"] = 7
	selected := store.PickAccount(PickOptions{RecordUse: false})
	if selected["id"] != acc["id"] {
		t.Fatal("selected wrong account")
	}
	if lu, _ := toInt64(acc["last_used"]); lu != 1234 {
		t.Fatalf("last_used changed on maintenance pick: %v", acc["last_used"])
	}
	if uc, _ := toInt64(acc["use_count"]); uc != 7 {
		t.Fatalf("use_count changed on maintenance pick: %v", acc["use_count"])
	}
	store.PickAccount(PickOptions{RecordUse: true})
	if uc, _ := toInt64(acc["use_count"]); uc != 8 {
		t.Fatalf("expected use_count=8, got %v", acc["use_count"])
	}

	// 2. Keepalive only runs when due
	now := int64(1_900_000_000)
	nowUnix = func() int64 { return now }
	store.Accounts = []map[string]any{
		{
			"id": "fresh", "label": "fresh", "enabled": true,
			"cookies":    map[string]string{"hatch_sess": "1"},
			"expires_at": now + 72*3600, "last_keepalive": now - 900, "ok": true,
		},
		{
			"id": "due", "label": "due", "enabled": true,
			"cookies":    map[string]string{"hatch_sess": "2"},
			"expires_at": now + 24*3600, "last_keepalive": now - 7*3600, "ok": true,
		},
	}
	var called []string
	probeAccountSyncHook = func(aid string, checkQuota bool) map[string]any {
		called = append(called, aid)
		return map[string]any{"id": aid, "ok": true}
	}
	res := RunKeepaliveAll(false)
	if len(called) != 1 || called[0] != "due" {
		t.Fatalf("expected only 'due' to be probed, got %v", called)
	}
	if res["refreshed_count"] != 1 || res["skipped_count"] != 1 {
		t.Fatalf("unexpected summary counts: %v", res)
	}
}

func TestAdminBaseURL(t *testing.T) {
	for _, pub := range []string{"", "https://api.example.com"} {
		setupTestEnv(t, pub)
		handler := NewHandler()

		req1 := httptest.NewRequest(http.MethodGet, "/admin/status", nil)
		req1.Header.Set("Authorization", "Bearer test-only")
		w1 := httptest.NewRecorder()
		handler.ServeHTTP(w1, req1)
		if w1.Code != http.StatusOK {
			t.Fatalf("GET /admin/status failed: %d", w1.Code)
		}
		var statusObj map[string]any
		_ = json.Unmarshal(w1.Body.Bytes(), &statusObj)

		req2 := httptest.NewRequest(http.MethodGet, "/admin/apikey", nil)
		req2.Header.Set("Authorization", "Bearer test-only")
		w2 := httptest.NewRecorder()
		handler.ServeHTTP(w2, req2)
		if w2.Code != http.StatusOK {
			t.Fatalf("GET /admin/apikey failed: %d", w2.Code)
		}
		var keyObj map[string]any
		_ = json.Unmarshal(w2.Body.Bytes(), &keyObj)

		baseURL, _ := statusObj["base_url"].(string)
		if (baseURL != "") != (pub != "") {
			t.Fatalf("base_url=%q for pub=%q", baseURL, pub)
		}
		if baseURL != keyObj["base_url"] {
			t.Fatalf("status base_url %q != apikey base_url %q", baseURL, keyObj["base_url"])
		}
		cfgMap, _ := statusObj["config"].(map[string]any)
		if cfgMap["public_base"] != strings.TrimRight(pub, "/") {
			t.Fatalf("config.public_base mismatch: %v", cfgMap["public_base"])
		}
	}
}

func TestMediaDelete(t *testing.T) {
	setupTestEnv(t, "")
	handler := NewHandler()

	f1 := filepath.Join(CFG.MediaDir(), "sample1.webp")
	f2 := filepath.Join(CFG.MediaDir(), "sample2.mp4")
	_ = os.WriteFile(f1, []byte("content1"), 0o644)
	_ = os.WriteFile(f2, []byte("content2"), 0o644)

	doReq := func(method, path string, body any) (*httptest.ResponseRecorder, map[string]any) {
		var rdr io.Reader
		if body != nil {
			b, _ := json.Marshal(body)
			rdr = bytes.NewReader(b)
		}
		req := httptest.NewRequest(method, path, rdr)
		req.Header.Set("Authorization", "Bearer test-only")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		var out map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &out)
		return w, out
	}

	w, data := doReq(http.MethodGet, "/admin/media", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /admin/media status=%d", w.Code)
	}
	mediaList, _ := data["media"].([]any)
	if len(mediaList) != 2 {
		t.Fatalf("expected 2 media files, got %d", len(mediaList))
	}

	w, data = doReq(http.MethodPost, "/admin/media/delete", map[string]any{"names": []string{"sample1.webp"}})
	if w.Code != http.StatusOK || data["removed"] != float64(1) {
		t.Fatalf("delete sample1 failed: %v", data)
	}
	if _, err := os.Stat(f1); !os.IsNotExist(err) {
		t.Fatal("sample1.webp still exists")
	}

	// Path traversal blocked
	w, data = doReq(http.MethodPost, "/admin/media/delete", map[string]any{"names": []string{"../main.go", `..\main.go`}})
	if w.Code != http.StatusOK || data["removed"] != float64(0) {
		t.Fatalf("traversal check failed: %v", data)
	}

	// Single delete
	w, data = doReq(http.MethodDelete, "/admin/media/sample2.mp4", nil)
	if w.Code != http.StatusOK || data["status"] != "ok" {
		t.Fatalf("DELETE /admin/media/sample2.mp4 failed: %v", data)
	}
	if _, err := os.Stat(f2); !os.IsNotExist(err) {
		t.Fatal("sample2.mp4 still exists")
	}

	w, _ = doReq(http.MethodDelete, "/admin/media/nonexistent.webp", nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for nonexistent file, got %d", w.Code)
	}
}

func TestAsyncImages(t *testing.T) {
	setupTestEnv(t, "")
	handler := NewHandler()

	mediaPath := filepath.Join(CFG.MediaDir(), "fixture.png")
	fixtureBytes := []byte("image fixture bytes")
	_ = os.WriteFile(mediaPath, fixtureBytes, 0o644)

	// 1. Verify GEN_LOCK covers retry/cleanup and bounded queue wait
	runGenerationLockedHook = func(prompt, kind string, timeout int, accountID string, onProgress func(int), referenceImage string, deadline time.Time) (map[string]any, string, error) {
		if !GEN_LOCK.Locked() {
			t.Fatal("browser lock must cover retry/cleanup")
		}
		if deadline.IsZero() {
			t.Fatal("deadline must be set")
		}
		return map[string]any{"fixture": true}, "", nil
	}
	_, _, err := RunGeneration("lock fixture", "image", 1, "", nil, "", nil)
	runGenerationLockedHook = nil
	if err != nil || GEN_LOCK.Locked() {
		t.Fatalf("lock check failed: err=%v locked=%v", err, GEN_LOCK.Locked())
	}

	GEN_LOCK.AcquireHook = func(blocking bool, timeoutSec float64) (bool, bool) {
		if timeoutSec != 1 {
			t.Fatalf("expected timeout=1, got %v", timeoutSec)
		}
		return true, false
	}
	_, _, err = RunGeneration("queue fixture", "image", 1, "", nil, "", nil)
	GEN_LOCK.AcquireHook = nil
	if !isGenErr(err) {
		t.Fatalf("expected MuseGenerationError on busy lock, got %v", err)
	}

	type genCall struct {
		Prompt string
		Kind   string
		RefImg string
	}
	var callsMu sync.Mutex
	var calls []genCall
	releaseCh := make(chan struct{})

	runGenerationHook = func(prompt, kind string, timeout int, accountID string, onProgress func(int), referenceImage string, onAcquired func()) (map[string]any, string, error) {
		if onAcquired != nil {
			onAcquired()
		}
		callsMu.Lock()
		calls = append(calls, genCall{Prompt: prompt, Kind: kind, RefImg: referenceImage})
		callsMu.Unlock()
		<-releaseCh
		if strings.Contains(prompt, "FAIL_FIXTURE") {
			return nil, "", &MuseGenerationError{Msg: "fixture generation failed"}
		}
		return map[string]any{
			"filename": "fixture.png",
			"size":     len(fixtureBytes),
			"kind":     "image",
			"path":     mediaPath,
		}, "fixture-account", nil
	}
	defer func() {
		runGenerationHook = nil
	}()

	doJSON := func(method, path string, body any, extraHeaders map[string]string) (*httptest.ResponseRecorder, map[string]any) {
		var rdr io.Reader
		if body != nil {
			b, _ := json.Marshal(body)
			rdr = bytes.NewReader(b)
		}
		req := httptest.NewRequest(method, path, rdr)
		req.Header.Set("Authorization", "Bearer test-only")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		for k, v := range extraHeaders {
			req.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		var out map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &out)
		return w, out
	}

	go func() {
		time.Sleep(150 * time.Millisecond)
		close(releaseCh)
	}()
	t0 := time.Now()
	w, respData := doJSON(http.MethodPost, "/v1/images/generations", map[string]any{
		"prompt": "change background", "async": true, "response_format": "b64_json",
	}, nil)
	elapsed := time.Since(t0)
	if w.Code != http.StatusAccepted || elapsed >= 120*time.Millisecond {
		t.Fatalf("expected 202 before completion, got code=%d elapsed=%v", w.Code, elapsed)
	}
	tid, _ := respData["task_id"].(string)

	finishTask := func(taskID string) map[string]any {
		for i := 0; i < 100; i++ {
			rw, data := doJSON(http.MethodGet, "/v1/images/tasks/"+taskID, nil, nil)
			if rw.Code != http.StatusOK {
				t.Fatalf("GET task status=%d", rw.Code)
			}
			st, _ := data["status"].(string)
			if st == "completed" || st == "failed" {
				return data
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("task never finished")
		return nil
	}

	done := finishTask(tid)
	if done["status"] != "completed" || done["progress"] != float64(100) {
		t.Fatalf("unexpected task completion: %v", done)
	}
	dataArr, _ := done["data"].([]any)
	firstItem, _ := dataArr[0].(map[string]any)
	b64Dec, _ := base64.StdEncoding.DecodeString(firstItem["b64_json"].(string))
	if !bytes.Equal(b64Dec, fixtureBytes) {
		t.Fatal("b64_json roundtrip mismatch")
	}
	callsMu.Lock()
	if len(calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(calls))
	}
	callsMu.Unlock()
	doJSON(http.MethodGet, "/v1/images/tasks/"+tid, nil, nil)
	callsMu.Lock()
	if len(calls) != 1 {
		t.Fatal("polling must not rerun generation")
	}
	callsMu.Unlock()

	// Multipart /v1/images/edits
	var mpBuf bytes.Buffer
	mw := multipart.NewWriter(&mpBuf)
	_ = mw.WriteField("prompt", "edit reference")
	_ = mw.WriteField("async", "true")
	fw, _ := mw.CreateFormFile("image", "reference.png")
	_, _ = fw.Write([]byte("fixture reference"))
	_ = mw.Close()

	mpReq := httptest.NewRequest(http.MethodPost, "/v1/images/edits", &mpBuf)
	mpReq.Header.Set("Authorization", "Bearer test-only")
	mpReq.Header.Set("Content-Type", mw.FormDataContentType())
	mpW := httptest.NewRecorder()
	handler.ServeHTTP(mpW, mpReq)
	if mpW.Code != http.StatusAccepted {
		t.Fatalf("multipart edits status=%d", mpW.Code)
	}
	var mpData map[string]any
	_ = json.Unmarshal(mpW.Body.Bytes(), &mpData)
	done = finishTask(mpData["task_id"].(string))
	if done["status"] != "completed" {
		t.Fatalf("multipart task failed: %v", done)
	}
	callsMu.Lock()
	lastRef := calls[len(calls)-1].RefImg
	callsMu.Unlock()
	wantRef := "data:application/octet-stream;base64," + base64.StdEncoding.EncodeToString([]byte("fixture reference"))
	if !strings.HasSuffix(lastRef, base64.StdEncoding.EncodeToString([]byte("fixture reference"))) {
		t.Fatalf("unexpected multipart ref: got %q, want suffix of %q", lastRef, wantRef)
	}

	// JSON /v1/images/edits
	w, respData = doJSON(http.MethodPost, "/v1/images/edits", map[string]any{
		"prompt": "edit JSON reference", "async": true, "reference_image": "data:image/png;base64,eA==",
	}, nil)
	if w.Code != http.StatusAccepted {
		t.Fatalf("json edits status=%d", w.Code)
	}
	done = finishTask(respData["task_id"].(string))
	if done["status"] != "completed" {
		t.Fatal("json edits task not completed")
	}
	callsMu.Lock()
	if calls[len(calls)-1].RefImg != "data:image/png;base64,eA==" {
		t.Fatalf("unexpected json ref: %q", calls[len(calls)-1].RefImg)
	}
	callsMu.Unlock()

	// Dedicated task endpoint + Idempotency-Key
	body := map[string]any{"prompt": "dedicated async request", "reference_image": "data:image/png;base64,eA=="}
	w, respData = doJSON(http.MethodPost, "/v1/images/tasks", body, map[string]string{"Idempotency-Key": "fixture-key"})
	if w.Code != http.StatusAccepted {
		t.Fatalf("dedicated task status=%d", w.Code)
	}
	dedicatedID, _ := respData["id"].(string)
	done = finishTask(dedicatedID)
	if done["status"] != "completed" {
		t.Fatal("dedicated task failed")
	}
	callsMu.Lock()
	countBeforeDup := len(calls)
	callsMu.Unlock()

	dupW, dupData := doJSON(http.MethodPost, "/v1/images/tasks", body, map[string]string{"Idempotency-Key": "fixture-key"})
	if dupW.Code != http.StatusAccepted || dupData["id"] != dedicatedID {
		t.Fatalf("idempotency duplicate mismatch: code=%d data=%v", dupW.Code, dupData)
	}
	callsMu.Lock()
	if len(calls) != countBeforeDup {
		t.Fatal("duplicate idempotency request must not re-run generation")
	}
	callsMu.Unlock()

	confW, _ := doJSON(http.MethodPost, "/v1/images/tasks", map[string]any{"prompt": "different"}, map[string]string{"Idempotency-Key": "fixture-key"})
	if confW.Code != http.StatusConflict {
		t.Fatalf("expected 409 conflict on reused Idempotency-Key, got %d", confW.Code)
	}

	// Sync compatibility
	syncW, syncData := doJSON(http.MethodPost, "/v1/images/generations", map[string]any{"prompt": "sync compatible"}, nil)
	if syncW.Code != http.StatusOK || syncData["data"] == nil {
		t.Fatalf("sync generations failed: %d %v", syncW.Code, syncData)
	}

	// Failure terminal, 404, 401
	failW, failResp := doJSON(http.MethodPost, "/v1/images/generations", map[string]any{"prompt": "FAIL_FIXTURE", "async": true}, nil)
	if failW.Code != http.StatusAccepted {
		t.Fatal("fail fixture not accepted")
	}
	done = finishTask(failResp["task_id"].(string))
	if done["status"] != "failed" || done["error"] != "fixture generation failed" {
		t.Fatalf("expected failed status, got %v", done)
	}
	missW, _ := doJSON(http.MethodGet, "/v1/images/tasks/missing", nil, nil)
	if missW.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", missW.Code)
	}
	unauthW, _ := doJSON(http.MethodGet, "/v1/images/tasks/"+tid, nil, map[string]string{"Authorization": "Bearer wrong"})
	if unauthW.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", unauthW.Code)
	}

	// Bounded queue (max 8 active image tasks)
	for i := 0; i < 8; i++ {
		store.CreateTask("image", "queued fixture")
	}
	fullW, _ := doJSON(http.MethodPost, "/v1/images/tasks", map[string]any{"prompt": "queue full"}, nil)
	if fullW.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 when queue full, got %d", fullW.Code)
	}

	// Startup marks interrupted tasks failed
	pending := store.CreateTask("image", "interrupted fixture")
	StartupInit()
	if store.GetTask(pending["id"].(string))["status"] != "failed" {
		t.Fatal("StartupInit must mark interrupted tasks failed")
	}
}

func makeMockHTTPResp(status int, bodyStr string, cookies []*http.Cookie) *http.Response {
	h := http.Header{}
	for _, c := range cookies {
		h.Add("Set-Cookie", c.String())
	}
	return &http.Response{
		StatusCode: status,
		Header:     h,
		Body:       io.NopCloser(strings.NewReader(bodyStr)),
	}
}

func TestSessionHealth(t *testing.T) {
	setupTestEnv(t, "")
	origNow := nowUnix
	origNowF := nowFloat
	origHTTP := httpDo
	origSleep := sleepSec
	defer func() {
		nowUnix = origNow
		nowFloat = origNowF
		httpDo = origHTTP
		sleepSec = origSleep
	}()
	nowUnix = func() int64 { return 1_900_000_000 }
	nowFloat = func() float64 { return 1_900_000_000 }
	sleepSec = func(float64) {}

	makeAcc := func(prior any, withExpiry bool) map[string]any {
		exp := map[string]int64{}
		if withExpiry {
			exp["hatch_vml"] = 1_900_012_345
		}
		a := store.AddAccount(map[string]string{"hatch_sess": "fixture"}, "", exp)
		a["ok"] = prior
		a["last_keepalive"] = int64(1_899_999_900)
		a["synced_at"] = int64(1_899_999_900)
		return a
	}

	type testCase struct {
		name string
		fn   func() (*http.Response, error)
	}
	renewedCookie := []*http.Cookie{
		{Name: "hatch_sess", Value: "renewed-fixture", Expires: time.Unix(1_900_020_000, 0)},
	}
	cases := []testCase{
		{"401", func() (*http.Response, error) { return makeMockHTTPResp(401, `{}`, renewedCookie), nil }},
		{"403", func() (*http.Response, error) { return makeMockHTTPResp(403, `{}`, renewedCookie), nil }},
		{"429", func() (*http.Response, error) { return makeMockHTTPResp(429, `{}`, renewedCookie), nil }},
		{"503", func() (*http.Response, error) { return makeMockHTTPResp(503, `{}`, renewedCookie), nil }},
		{"redirect", func() (*http.Response, error) { return makeMockHTTPResp(302, `{}`, renewedCookie), nil }},
		{"non_json", func() (*http.Response, error) { return makeMockHTTPResp(200, `not-json`, renewedCookie), nil }},
		{"non_object", func() (*http.Response, error) { return makeMockHTTPResp(200, `[]`, renewedCookie), nil }},
		{"unassigned", func() (*http.Response, error) {
			return makeMockHTTPResp(200, `{"status":"unassigned"}`, renewedCookie), nil
		}},
		{"timeout", func() (*http.Response, error) { return nil, errors.New("timeout") }},
		{"connection", func() (*http.Response, error) { return nil, errors.New("connection refused") }},
		{"assigned", func() (*http.Response, error) {
			return makeMockHTTPResp(200, `{"status":"assigned","vm_state":"RUNNING"}`, renewedCookie), nil
		}},
	}

	protectedKeys := []string{"cookies", "cookies_exp", "expires_at", "synced_at", "last_keepalive"}
	priors := []any{true, nil, false}

	for _, prior := range priors {
		for _, tc := range cases {
			a := makeAcc(prior, true)
			aid, _ := a["id"].(string)
			before := cloneMap(a)
			redirectChecked := false
			httpDo = func(req *http.Request, timeout time.Duration, followRedirects bool) (*http.Response, error) {
				if followRedirects {
					t.Fatal("/api/session must not follow redirects")
				}
				redirectChecked = true
				return tc.fn()
			}
			res := ProbeAccountSync(aid, false)
			if !redirectChecked {
				t.Fatal("httpDo was not called")
			}
			var expectedOK any = prior
			if tc.name == "assigned" {
				expectedOK = true
			} else if tc.name == "401" {
				expectedOK = false
			}
			if a["ok"] != expectedOK {
				t.Fatalf("case=%s prior=%v: got ok=%v, want %v", tc.name, prior, a["ok"], expectedOK)
			}
			if (res["ok"] == true) != (tc.name == "assigned") {
				t.Fatalf("case=%s: wrong probe result %v", tc.name, res["ok"])
			}
			if tc.name != "assigned" {
				for _, k := range protectedKeys {
					b1, _ := json.Marshal(a[k])
					b2, _ := json.Marshal(before[k])
					if !bytes.Equal(b1, b2) {
						t.Fatalf("case=%s modified protected field %s: got %s, want %s", tc.name, k, b1, b2)
					}
				}
			} else {
				cMap := extractCookiesMap(a["cookies"])
				if cMap["hatch_sess"] != "renewed-fixture" {
					t.Fatalf("renewed cookie not saved: %v", cMap)
				}
				if ka, _ := toInt64(a["last_keepalive"]); ka != 1_900_000_000 {
					t.Fatalf("last_keepalive not updated: %v", a["last_keepalive"])
				}
				if expAt, _ := toInt64(a["expires_at"]); expAt != 1_900_012_345 {
					t.Fatalf("expires_at fabricated on success: %v", a["expires_at"])
				}
			}
			if tc.name == "401" || tc.name == "403" || tc.name == "429" || tc.name == "503" {
				if !strings.Contains(fmt.Sprint(a["note"]), "HTTP "+tc.name) {
					t.Fatalf("case=%s missing HTTP diagnostic in note: %v", tc.name, a["note"])
				}
			}
		}
	}

	// TouchKeepalive with nil and without renewed cookie
	aNoExp := makeAcc(true, false)
	beforeExp := aNoExp["expires_at"]
	nowUnix = func() int64 { return 1_900_003_600 }
	tVal := true
	store.TouchKeepalive(aNoExp["id"].(string), &tVal, "")
	if aNoExp["expires_at"] != beforeExp {
		t.Fatalf("estimated expiry advanced without renewed cookie: got %v, want %v", aNoExp["expires_at"], beforeExp)
	}
	nowUnix = func() int64 { return 1_900_000_000 }

	// Manual test classification (MuseAuthError vs MuseGenerationError)
	for _, simErr := range []error{&MuseAuthError{Msg: "fixture auth"}, &MuseGenerationError{Msg: "fixture loading"}} {
		for _, prior := range priors {
			a := makeAcc(prior, true)
			aid, _ := a["id"].(string)
			engine.StartFunc = func() error { return nil }
			engine.RefreshFunc = func(cookies map[string]string, expires map[string]int64) (*CDP, error) {
				return nil, simErr
			}
			ProbeAccountForTest(aid)
			var want any = prior
			if isAuthErr(simErr) {
				want = false
			}
			if a["ok"] != want {
				t.Fatalf("manual test misclassified account for err=%T prior=%v: got %v, want %v", simErr, prior, a["ok"], want)
			}
		}
	}
	engine.StartFunc = nil
	engine.RefreshFunc = nil

	// EnsurePage failure classification ("loading workspace" vs "Sign in")
	for _, tc := range []struct {
		body    string
		wantAuth bool
	}{
		{"loading workspace", false},
		{"Sign in", true},
	} {
		e := NewMuseEngine(&Config{BaseDir: t.TempDir(), LoginWait: 0})
		e.OpenPageFunc = func() (*CDP, string, error) {
			return &CDP{
				MockJS: func(expr string) (any, error) {
					if strings.Contains(expr, "innerText") {
						return tc.body, nil
					}
					return false, nil
				},
			}, "", nil
		}
		e.ApplyCookiesFn = func(page *CDP, cookies map[string]string, expires map[string]int64) {}
		httpDo = func(req *http.Request, timeout time.Duration, followRedirects bool) (*http.Response, error) {
			return makeMockHTTPResp(200, `{"status":"assigned","vm_state":"RUNNING"}`, nil), nil
		}
		_, err := e.EnsurePage(map[string]string{"hatch_sess": "fixture"}, nil, "")
		if err == nil || isAuthErr(err) != tc.wantAuth {
			t.Fatalf("EnsurePage body=%q: got err=%T (%v), wantAuth=%v", tc.body, err, err, tc.wantAuth)
		}
	}
}

func TestWakeConfirmation(t *testing.T) {
	origHTTP := httpDo
	defer func() { httpDo = origHTTP }()

	cases := []struct {
		verifyResp *http.Response
		wantState  string
		wantOK     bool
	}{
		{makeMockHTTPResp(200, `{"status":"assigned","vm_state":"SUSPENDED"}`, nil), "SUSPENDED", false},
		{makeMockHTTPResp(200, `{"status":"assigned","vm_state":"RUNNING"}`, nil), "RUNNING", true},
		{makeMockHTTPResp(503, `{}`, nil), "SUSPENDED", false},
	}

	for _, tc := range cases {
		getStep := 0
		httpDo = func(req *http.Request, timeout time.Duration, followRedirects bool) (*http.Response, error) {
			if req.Method == http.MethodPost {
				return makeMockHTTPResp(200, `{"accepted":true}`, nil), nil
			}
			getStep++
			if getStep == 1 {
				return makeMockHTTPResp(200, `{"status":"assigned","vm_id":"vm-fixture","vm_state":"SUSPENDED"}`, nil), nil
			}
			return tc.verifyResp, nil
		}
		res, err := RenewSessionHTTP(map[string]string{"hatch_sess": "fixture"}, nil, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res["wake_requested"] != true {
			t.Fatal("expected wake_requested=true")
		}
		if res["wake_ok"] != tc.wantOK {
			t.Fatalf("got wake_ok=%v, want %v", res["wake_ok"], tc.wantOK)
		}
		if res["vm_state"] != tc.wantState {
			t.Fatalf("got vm_state=%v, want %s", res["vm_state"], tc.wantState)
		}
	}
}

func TestQuotaAccountBinding(t *testing.T) {
	origSleep := sleepSec
	defer func() { sleepSec = origSleep }()
	sleepSec = func(float64) {}

	responses := []string{
		`{"x": 1, "y": 2}`, `{"x": 1, "y": 2}`,
		"Usage\nFree plan\nWeekly limit resets on Oct 17\n2% used\nAdditional tokens\nNever expires\n0% used (8B tokens left)",
		`{"x": 1, "y": 2}`, `{"x": 1, "y": 2}`,
		"Usage\nFree plan\nWeekly limit resets on Oct 17\n24% used\nAdditional tokens\nNever expires\n0% used (1B tokens left)",
	}
	idx := 0
	var seen []string

	e := NewMuseEngine(nil)
	e.ClickPointFn = func(x, y int) {}
	e.EnsurePageFunc = func(cookies map[string]string, expires map[string]int64, accountID string) (*CDP, error) {
		seen = append(seen, accountID)
		p := &CDP{
			MockJS: func(expr string) (any, error) {
				r := responses[idx]
				idx++
				return r, nil
			},
		}
		e.setSession(accountID, p, "")
		return p, nil
	}

	qa, err := e.Quota(map[string]string{"fixture": "a"}, nil, "account-a")
	if err != nil {
		t.Fatalf("quota a failed: %v", err)
	}
	qb, err := e.Quota(map[string]string{"fixture": "b"}, nil, "account-b")
	if err != nil {
		t.Fatalf("quota b failed: %v", err)
	}
	if len(seen) != 2 || seen[0] != "account-a" || seen[1] != "account-b" {
		t.Fatalf("unexpected account binding sequence: %v", seen)
	}
	if qa["weekly_used_pct"] != 2 || qa["extra_left"] != "8B tokens left" {
		t.Fatalf("unexpected qa: %v", qa)
	}
	if qb["weekly_used_pct"] != 24 || qb["extra_left"] != "1B tokens left" {
		t.Fatalf("unexpected qb: %v", qb)
	}
}

func TestMuseVideoDeliveryAndVMWait(t *testing.T) {
	origNowF := nowFloat
	origSleep := sleepSec
	defer func() {
		nowFloat = origNowF
		sleepSec = origSleep
	}()

	// 1. Undelivered video fast-fails around 30s when "Still sending"
	clock := 0.0
	nowFloat = func() float64 { return clock }
	sleepSec = func(s float64) { clock += s }

	e := NewMuseEngine(nil)
	e.ScrollBottomFn = func() {}
	e.Page = &CDP{
		MockJS: func(expr string) (any, error) {
			return `{"cnt":0,"txt":"","stop":true,"tail":"Still sending"}`, nil
		},
	}
	e.AttachmentsFunc = func() []map[string]any { return nil }

	_, err := e.WaitAttachment("", "", 60, "video", nil, 0, 0, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "Prompt delivery not confirmed") {
		t.Fatalf("expected undelivered fast-fail error, got %v", err)
	}
	if clock >= 32.0 {
		t.Fatalf("expected fast-fail before 32s, got %v", clock)
	}

	// 2. VM wait ("Connecting..." does not abort before attachment arrives at t=20s)
	clock = 0.0
	e.Page = &CDP{
		MockJS: func(expr string) (any, error) {
			return `{"tail":"Connecting...","cnt":0,"txt":"","stop":true}`, nil
		},
	}
	e.AttachmentsFunc = func() []map[string]any {
		if clock < 20.0 {
			return nil
		}
		return []map[string]any{{"src": "generated", "tid": "image", "w": 100, "h": 100}}
	}
	att, err := e.WaitAttachment("", "", 30, "image", nil, 0, 0, nil, nil)
	if err != nil || att == nil || att["src"] != "generated" {
		t.Fatalf("expected generated attachment at t=20s, got att=%v err=%v", att, err)
	}
}

func TestMediaSelection(t *testing.T) {
	origNowF := nowFloat
	origSleep := sleepSec
	defer func() {
		nowFloat = origNowF
		sleepSec = origSleep
	}()
	clock := 0.0
	nowFloat = func() float64 { return clock }
	sleepSec = func(s float64) { clock += s }

	e := NewMuseEngine(nil)
	e.ScrollBottomFn = func() {}

	// 1. Ignore baseline sources and pick new image
	calls := 0
	e.AttachmentsFunc = func() []map[string]any {
		calls++
		if calls == 1 {
			return []map[string]any{{"src": "old-a", "tid": "image", "w": 5, "h": 5}}
		}
		return []map[string]any{{"src": "new-image", "tid": "image", "w": 5, "h": 5}}
	}
	selected, err := e.WaitAttachment("", "old-b", 5, "image", nil, 0, 2, nil, map[string]bool{"old-a": true, "old-b": true})
	if err != nil || selected["src"] != "new-image" {
		t.Fatalf("expected new-image, got %v (err=%v)", selected, err)
	}

	// 2. Video attachment with vSrc is selected
	e.AttachmentsFunc = func() []map[string]any {
		return []map[string]any{{"src": "poster.png", "vSrc": "clip.mp4", "tid": "video", "hasVideo": true, "w": 32, "h": 32}}
	}
	vid, err := e.WaitAttachment("", "", 3, "video", nil, 0, 0, nil, nil)
	if err != nil || vid["vSrc"] != "clip.mp4" {
		t.Fatalf("expected clip.mp4, got %v (err=%v)", vid, err)
	}

	// 3. Cross-modal image rejected when expecting video
	e.AttachmentsFunc = func() []map[string]any {
		return []map[string]any{{"src": "poster_only.png", "tid": "image", "hasVideo": false, "w": 32, "h": 32}}
	}
	if att, _ := e.WaitAttachment("", "", 1, "video", nil, 0, 0, nil, nil); att != nil {
		t.Fatalf("expected cross-modal fallback rejected, got %v", att)
	}

	// 4. tid=hatch-chat-attachment-presentation-video with only cover jpg (no <video>/vSrc) rejected
	e.AttachmentsFunc = func() []map[string]any {
		return []map[string]any{{"src": "cover.jpg", "vSrc": "", "tid": "hatch-chat-attachment-presentation-video", "hasVideo": false, "w": 1280, "h": 720}}
	}
	if att, _ := e.WaitAttachment("", "", 1, "video", nil, 0, 0, nil, nil); att != nil {
		t.Fatalf("expected video cover frame rejected, got %v", att)
	}

	// 5. Invalid reference image fails closed
	e.NormalizeImgFn = func(img string) (string, string) { return "", "image/png" }
	if err := e.AttachImage("", "bad-reference"); !isGenErr(err) {
		t.Fatalf("expected AttachImage to fail closed with MuseGenerationError, got %v", err)
	}
}

func TestToolProtocolAndPrompts(t *testing.T) {
	// ParseToolCalls with <local_action>
	out := "I will read the file.\n```xml\n<local_action>\n{\"name\": \"Read\", \"arguments\": {\"file_path\": \"/tmp/a.txt\"}}\n</local_action>\n```"
	calls, rest := ParseToolCalls(out)
	if len(calls) != 1 {
		t.Fatalf("expected 1 tool call, got %d", len(calls))
	}
	fn, _ := calls[0]["function"].(map[string]any)
	if fn["name"] != "Read" {
		t.Fatalf("expected Read, got %v", fn["name"])
	}
	if rest != "I will read the file." {
		t.Fatalf("unexpected rest text: %q", rest)
	}
}
