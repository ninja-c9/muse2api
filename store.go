package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

var EssentialCookies = []string{
	"hatch_sess",
	"hatch_gw",
	"hatch_vml",
	"hatch_native_auth_device",
}

const VMLTTL int64 = 2 * 86400

// nowUnix and nowFloat allow deterministic clock injection in offline tests.
var nowUnix = func() int64 {
	return time.Now().Unix()
}

var nowFloat = func() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

func randomHex(nChars int) string {
	b := make([]byte, (nChars+1)/2)
	_, _ = rand.Read(b)
	s := hex.EncodeToString(b)
	if len(s) > nChars {
		return s[:nChars]
	}
	return s
}

func toInt64(v any) (int64, bool) {
	if v == nil {
		return 0, false
	}
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int64:
		return val, true
	case float64:
		return int64(val), true
	case json.Number:
		if n, err := val.Int64(); err == nil {
			return n, true
		}
		if f, err := val.Float64(); err == nil {
			return int64(f), true
		}
	case string:
		if n, err := strconv.ParseInt(val, 10, 64); err == nil {
			return n, true
		}
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return int64(f), true
		}
	}
	return 0, false
}

func toFloat64(v any) (float64, bool) {
	if v == nil {
		return 0, false
	}
	switch val := v.(type) {
	case float64:
		return val, true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case json.Number:
		if f, err := val.Float64(); err == nil {
			return f, true
		}
	}
	return 0, false
}

func isPos(v any) bool {
	n, ok := toInt64(v)
	return ok && n > 0
}

func MinExpiry(cookiesExp map[string]int64) *int64 {
	if len(cookiesExp) == 0 {
		return nil
	}
	var vals []int64
	for _, name := range EssentialCookies {
		if v, ok := cookiesExp[name]; ok && v > 0 {
			vals = append(vals, v)
		}
	}
	if len(vals) == 0 {
		for _, v := range cookiesExp {
			if v > 0 {
				vals = append(vals, v)
			}
		}
	}
	if len(vals) == 0 {
		return nil
	}
	m := vals[0]
	for _, v := range vals[1:] {
		if v < m {
			m = v
		}
	}
	return &m
}

func AccountExpiry(cookiesExp map[string]int64, baseTS int64) *int64 {
	if cookiesExp != nil {
		if vml, ok := cookiesExp["hatch_vml"]; ok && vml > 0 {
			val := vml
			return &val
		}
	}
	if baseTS > 0 {
		val := baseTS + VMLTTL
		return &val
	}
	return MinExpiry(cookiesExp)
}

func readJSONFile(path string, target any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, target)
}

// writeJSONHook can be overridden in tests to skip disk writes if desired.
var writeJSONHook func(path string, obj any) error

func writeJSONFile(path string, obj any) error {
	if writeJSONHook != nil {
		return writeJSONHook(path, obj)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := f.Name()
	defer func() {
		if _, err := os.Stat(tmpName); err == nil {
			_ = os.Remove(tmpName)
		}
	}()
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", " ")
	if err := enc.Encode(obj); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func cloneMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	b, _ := json.Marshal(m)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	return out
}

func extractCookiesMap(v any) map[string]string {
	out := map[string]string{}
	switch m := v.(type) {
	case map[string]string:
		for k, val := range m {
			out[k] = val
		}
	case map[string]any:
		for k, val := range m {
			if s, ok := val.(string); ok {
				out[k] = s
			}
		}
	}
	return out
}

func extractExpMap(v any) map[string]int64 {
	out := map[string]int64{}
	switch m := v.(type) {
	case map[string]int64:
		for k, val := range m {
			if val > 0 {
				out[k] = val
			}
		}
	case map[string]int:
		for k, val := range m {
			if val > 0 {
				out[k] = int64(val)
			}
		}
	case map[string]any:
		for k, val := range m {
			if n, ok := toInt64(val); ok && n > 0 {
				out[k] = n
			}
		}
	}
	return out
}

func isAccountEnabled(a map[string]any) bool {
	if v, exists := a["enabled"]; exists {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return true
}

type Store struct {
	cfg         *Config
	mu          sync.Mutex
	Accounts    []map[string]any
	Tasks       map[string]map[string]any
	stickyID    string
	stickyCount int
}

func NewStore(cfg *Config) *Store {
	s := &Store{
		cfg:      cfg,
		Accounts: []map[string]any{},
		Tasks:    map[string]map[string]any{},
	}
	if cfg != nil {
		_ = readJSONFile(cfg.AccountsFile(), &s.Accounts)
		for _, a := range s.Accounts {
			ce := extractExpMap(a["cookies_exp"])
			if vml, ok := ce["hatch_vml"]; ok && vml > 0 {
				a["expires_at"] = vml
			}
		}
		_ = readJSONFile(cfg.TasksFile(), &s.Tasks)
	}
	if s.Accounts == nil {
		s.Accounts = []map[string]any{}
	}
	if s.Tasks == nil {
		s.Tasks = map[string]map[string]any{}
	}
	return s
}

func (s *Store) saveAccountsLocked() {
	if s.cfg != nil {
		_ = writeJSONFile(s.cfg.AccountsFile(), s.Accounts)
	}
}

func (s *Store) saveTasksLocked() {
	if s.cfg != nil {
		_ = writeJSONFile(s.cfg.TasksFile(), s.Tasks)
	}
}

func (s *Store) AddAccount(cookies map[string]string, label string, cookiesExp map[string]int64) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()

	aid := randomHex(12)
	if label == "" {
		label = aid
	}
	exp := map[string]int64{}
	for k, v := range cookiesExp {
		if v > 0 {
			exp[k] = v
		}
	}
	now := nowUnix()
	var expVal any
	if p := AccountExpiry(exp, now); p != nil {
		expVal = *p
	}
	cCopy := map[string]string{}
	for k, v := range cookies {
		cCopy[k] = v
	}
	acc := map[string]any{
		"id":             aid,
		"label":          label,
		"cookies":        cCopy,
		"cookies_exp":    exp,
		"expires_at":     expVal,
		"expiry_anchor":  nil,
		"enabled":        true,
		"created_at":     now,
		"last_used":      nil,
		"last_keepalive": nil,
		"use_count":      0,
		"ok":             nil,
		"note":           "",
		"synced_at":      nil,
	}
	s.Accounts = append(s.Accounts, acc)
	s.saveAccountsLocked()
	return acc
}

func (s *Store) ListAccounts() []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]map[string]any, 0, len(s.Accounts))
	for _, a := range s.Accounts {
		item := map[string]any{}
		for k, v := range a {
			if k != "cookies" {
				item[k] = v
			}
		}
		ce := extractExpMap(a["cookies_exp"])
		if vml, ok := ce["hatch_vml"]; ok && vml > 0 {
			item["expires_at"] = vml
		}
		cookies := extractCookiesMap(a["cookies"])
		item["cookie_count"] = len(cookies)
		essOK := true
		for _, n := range EssentialCookies {
			if cookies[n] == "" {
				essOK = false
				break
			}
		}
		item["essential_ok"] = essOK
		item["expires_estimated"] = !(ce["hatch_vml"] > 0)
		out = append(out, item)
	}
	return out
}

func (s *Store) DeleteAccount(aid string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := len(s.Accounts)
	filtered := make([]map[string]any, 0, n)
	for _, a := range s.Accounts {
		if id, _ := a["id"].(string); id != aid {
			filtered = append(filtered, a)
		}
	}
	s.Accounts = filtered
	s.saveAccountsLocked()
	return len(s.Accounts) != n
}

type PickOptions struct {
	PreferredID   string
	Rotate        bool
	ForceRotate   bool
	ExcludeID     string
	ExcludeIDs    map[string]bool
	HasExcludeIDs bool
	RecordUse     bool
}

func (s *Store) PickAccount(opts PickOptions) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()

	var live []map[string]any
	for _, a := range s.Accounts {
		if isAccountEnabled(a) && len(extractCookiesMap(a["cookies"])) > 0 {
			live = append(live, a)
		}
	}
	if len(live) == 0 {
		return nil
	}

	blocked := map[string]bool{}
	for k, v := range opts.ExcludeIDs {
		if v {
			blocked[k] = true
		}
	}
	if opts.ExcludeID != "" {
		blocked[opts.ExcludeID] = true
	}
	if len(blocked) > 0 {
		var available []map[string]any
		for _, a := range live {
			id, _ := a["id"].(string)
			if !blocked[id] {
				available = append(available, a)
			}
		}
		if len(available) > 0 {
			live = available
		} else if opts.HasExcludeIDs {
			return nil
		} else if len(live) > 1 && opts.ExcludeID != "" {
			var alt []map[string]any
			for _, a := range live {
				if id, _ := a["id"].(string); id != opts.ExcludeID {
					alt = append(alt, a)
				}
			}
			if len(alt) > 0 {
				live = alt
			}
		}
	}

	if opts.PreferredID != "" && !opts.ForceRotate {
		for _, a := range live {
			id, _ := a["id"].(string)
			okVal, hasOK := a["ok"].(bool)
			isNotFalse := !hasOK || okVal
			if id == opts.PreferredID && isNotFalse {
				stickyN := 0
				if s.stickyID == opts.PreferredID {
					stickyN = s.stickyCount
				}
				if !opts.Rotate || stickyN < 15 {
					s.stickyID = opts.PreferredID
					s.stickyCount = stickyN + 1
					if opts.RecordUse {
						a["last_used"] = nowFloat()
						uc, _ := toInt64(a["use_count"])
						a["use_count"] = int(uc) + 1
						s.saveAccountsLocked()
					}
					return a
				}
			}
		}
	}

	var healthy []map[string]any
	for _, a := range live {
		okVal, hasOK := a["ok"].(bool)
		if !hasOK || okVal {
			healthy = append(healthy, a)
		}
	}
	candidates := healthy
	if len(candidates) == 0 {
		candidates = append([]map[string]any(nil), live...)
	} else {
		candidates = append([]map[string]any(nil), healthy...)
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		luI, _ := toFloat64(candidates[i]["last_used"])
		luJ, _ := toFloat64(candidates[j]["last_used"])
		if luI != luJ {
			return luI < luJ
		}
		ucI, _ := toInt64(candidates[i]["use_count"])
		ucJ, _ := toInt64(candidates[j]["use_count"])
		return ucI < ucJ
	})

	acc := candidates[0]
	id, _ := acc["id"].(string)
	s.stickyID = id
	s.stickyCount = 1
	if opts.RecordUse {
		acc["last_used"] = nowFloat()
		uc, _ := toInt64(acc["use_count"])
		acc["use_count"] = int(uc) + 1
		s.saveAccountsLocked()
	}
	return acc
}

// TouchKeepalive updates account status after keepalive probe.
// ok == nil records an unconfirmed probe without altering ok, last_keepalive, or expires_at.
func (s *Store) TouchKeepalive(aid string, ok *bool, note string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	nowTS := nowUnix()
	for _, a := range s.Accounts {
		if id, _ := a["id"].(string); id == aid {
			if ok != nil {
				a["ok"] = *ok
				if *ok {
					a["last_keepalive"] = nowTS
					// ponytail: trust only actual cookie expiry or original anchor, never fabricate +48h on probe success.
					ce := extractExpMap(a["cookies_exp"])
					anchor, hasAnchor := toInt64(a["expiry_anchor"])
					if !hasAnchor || anchor <= 0 {
						anchor, _ = toInt64(a["created_at"])
					}
					if p := AccountExpiry(ce, anchor); p != nil {
						a["expires_at"] = *p
					} else {
						a["expires_at"] = nil
					}
				}
			}
			if note != "" {
				if len(note) > 300 {
					note = note[:300]
				}
				a["note"] = note
			}
			a["checked_at"] = nowTS
		}
	}
	s.saveAccountsLocked()
}

func (s *Store) Mark(aid string, ok bool, note string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(note) > 300 {
		note = note[:300]
	}
	nowTS := nowUnix()
	for _, a := range s.Accounts {
		if id, _ := a["id"].(string); id == aid {
			a["ok"] = ok
			a["note"] = note
			a["checked_at"] = nowTS
		}
	}
	s.saveAccountsLocked()
}

func (s *Store) GetAccount(aid string) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, a := range s.Accounts {
		if id, _ := a["id"].(string); id == aid {
			return a
		}
	}
	return nil
}

func (s *Store) UpdateAccount(aid string, fields map[string]any) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, a := range s.Accounts {
		if id, _ := a["id"].(string); id == aid {
			for k, v := range fields {
				if k == "ok_nil" && v == true {
					a["ok"] = nil
					continue
				}
				if v != nil {
					a[k] = v
				}
			}
			if ceRaw, hasCE := fields["cookies_exp"]; hasCE && ceRaw != nil {
				ce := extractExpMap(a["cookies_exp"])
				a["cookies_exp"] = ce
				if ce["hatch_vml"] > 0 {
					a["expiry_anchor"] = nil
				} else if anchor, ok := toInt64(a["expiry_anchor"]); !ok || anchor <= 0 {
					a["expiry_anchor"] = nowUnix()
				}
				baseTS, ok := toInt64(a["expiry_anchor"])
				if !ok || baseTS <= 0 {
					baseTS, _ = toInt64(a["created_at"])
				}
				if p := AccountExpiry(ce, baseTS); p != nil {
					a["expires_at"] = *p
				} else {
					a["expires_at"] = nil
				}
			}
			s.saveAccountsLocked()
			return a
		}
	}
	return nil
}

func (s *Store) Stats() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := nowUnix()
	total := len(s.Accounts)
	enabled, bad, healthy, expiring, expired := 0, 0, 0, 0, 0
	for _, a := range s.Accounts {
		if !isAccountEnabled(a) {
			continue
		}
		enabled++
		if okVal, hasOK := a["ok"].(bool); hasOK {
			if okVal {
				healthy++
			} else {
				bad++
			}
		}
		if expAt, ok := toInt64(a["expires_at"]); ok && expAt > 0 {
			if expAt-now < 3*86400 {
				expiring++
			}
			if expAt <= now {
				expired++
			}
		}
	}
	return map[string]int{
		"total":    total,
		"enabled":  enabled,
		"disabled": total - enabled,
		"healthy":  healthy,
		"error":    bad,
		"expiring": expiring,
		"expired":  expired,
	}
}

// ---------- Tasks ----------

func (s *Store) CreateTask(kind, prompt string) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()

	tid := "task_" + randomHex(20)
	now := nowUnix()
	t := map[string]any{
		"id":         tid,
		"object":     "task",
		"kind":       kind,
		"prompt":     prompt,
		"status":     "queued",
		"created_at": now,
		"updated_at": now,
		"result":     nil,
		"error":      nil,
	}
	s.Tasks[tid] = t
	s.saveTasksLocked()
	return cloneMap(t)
}

func (s *Store) UpdateTask(tid string, fields map[string]any) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.Tasks[tid]
	if !ok {
		return nil
	}
	for k, v := range fields {
		t[k] = v
	}
	t["updated_at"] = nowUnix()
	s.saveTasksLocked()
	return cloneMap(t)
}

func (s *Store) GetTask(tid string) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.Tasks[tid]
	if !ok {
		return nil
	}
	return cloneMap(t)
}

func (s *Store) ListTasks(limit int) []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()

	if limit <= 0 {
		limit = 50
	}
	items := make([]map[string]any, 0, len(s.Tasks))
	for _, t := range s.Tasks {
		items = append(items, cloneMap(t))
	}
	sort.SliceStable(items, func(i, j int) bool {
		ci, _ := toInt64(items[i]["created_at"])
		cj, _ := toInt64(items[j]["created_at"])
		return ci > cj
	})
	if len(items) > limit {
		items = items[:limit]
	}
	return items
}

func (s *Store) DeleteTask(tid string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.Tasks[tid]; ok {
		delete(s.Tasks, tid)
		s.saveTasksLocked()
		return true
	}
	return false
}

func (s *Store) ClearTasks(keep int) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if keep < 0 {
		keep = 0
	}
	items := make([]map[string]any, 0, len(s.Tasks))
	for _, t := range s.Tasks {
		items = append(items, t)
	}
	sort.SliceStable(items, func(i, j int) bool {
		ci, _ := toInt64(items[i]["created_at"])
		cj, _ := toInt64(items[j]["created_at"])
		return ci > cj
	})
	keepIDs := map[string]bool{}
	for i := 0; i < len(items) && i < keep; i++ {
		if id, _ := items[i]["id"].(string); id != "" {
			keepIDs[id] = true
		}
	}
	removed := len(s.Tasks) - len(keepIDs)
	newTasks := map[string]map[string]any{}
	for k, v := range s.Tasks {
		if keepIDs[k] {
			newTasks[k] = v
		}
	}
	s.Tasks = newTasks
	s.saveTasksLocked()
	if removed < 0 {
		return 0
	}
	return removed
}
