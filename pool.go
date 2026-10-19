package main

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

type ConcurrencyLock struct {
	Limit       int
	sem         chan struct{}
	mu          sync.Mutex
	activeCount int
	AcquireHook func(blocking bool, timeoutSec float64) (bool, bool)
}

func NewConcurrencyLock(limit int) *ConcurrencyLock {
	if limit < 1 {
		limit = 1
	}
	return &ConcurrencyLock{
		Limit: limit,
		sem:   make(chan struct{}, limit),
	}
}

func (l *ConcurrencyLock) Acquire(blocking bool, timeoutSec float64) bool {
	if l.AcquireHook != nil {
		if handled, ok := l.AcquireHook(blocking, timeoutSec); handled {
			return ok
		}
	}
	if !blocking {
		select {
		case l.sem <- struct{}{}:
			l.mu.Lock()
			l.activeCount++
			l.mu.Unlock()
			return true
		default:
			return false
		}
	}
	if timeoutSec < 0 {
		l.sem <- struct{}{}
		l.mu.Lock()
		l.activeCount++
		l.mu.Unlock()
		return true
	}
	timer := time.NewTimer(time.Duration(timeoutSec * float64(time.Second)))
	defer timer.Stop()
	select {
	case l.sem <- struct{}{}:
		l.mu.Lock()
		l.activeCount++
		l.mu.Unlock()
		return true
	case <-timer.C:
		return false
	}
}

func (l *ConcurrencyLock) Release() {
	l.mu.Lock()
	if l.activeCount > 0 {
		l.activeCount--
	}
	l.mu.Unlock()
	select {
	case <-l.sem:
	default:
	}
}

func (l *ConcurrencyLock) Locked() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.activeCount > 0
}

type AccountPoolManager struct {
	store        *Store
	mu           sync.Mutex
	BusyAccounts map[string]bool
	notifyCh     chan struct{}
}

func NewAccountPoolManager(s *Store) *AccountPoolManager {
	return &AccountPoolManager{
		store:        s,
		BusyAccounts: map[string]bool{},
		notifyCh:     make(chan struct{}),
	}
}

type AcquireAccountOptions struct {
	AccountID   string
	PreferredID string
	Rotate      bool
	ForceRotate bool
	TimeoutSec  float64
	HasRecord   bool
	RecordUse   bool
}

func (p *AccountPoolManager) Acquire(opts AcquireAccountOptions) (map[string]any, error) {
	timeout := opts.TimeoutSec
	if timeout <= 0 {
		timeout = 60.0
	}
	recordUse := true
	if opts.HasRecord {
		recordUse = opts.RecordUse
	}
	deadline := time.Now().Add(time.Duration(timeout * float64(time.Second)))

	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, &MuseGenerationError{Msg: "Timed out waiting for an idle account; all accounts are currently busy"}
		}

		p.mu.Lock()
		if opts.AccountID != "" {
			acc := p.store.GetAccount(opts.AccountID)
			if acc == nil || !isAccountEnabled(acc) {
				p.mu.Unlock()
				return nil, &MuseAuthError{Msg: fmt.Sprintf("Specified account %s does not exist or is disabled", opts.AccountID)}
			}
			if !p.BusyAccounts[opts.AccountID] {
				p.BusyAccounts[opts.AccountID] = true
				p.mu.Unlock()
				return acc, nil
			}
		} else {
			pref := opts.PreferredID
			if p.BusyAccounts[pref] {
				pref = ""
			}
			busyCopy := map[string]bool{}
			for k, v := range p.BusyAccounts {
				if v {
					busyCopy[k] = true
				}
			}
			acc := p.store.PickAccount(PickOptions{
				PreferredID:   pref,
				Rotate:        opts.Rotate,
				ForceRotate:   opts.ForceRotate,
				ExcludeIDs:    busyCopy,
				HasExcludeIDs: true,
				RecordUse:     recordUse,
			})
			if acc != nil {
				id, _ := acc["id"].(string)
				p.BusyAccounts[id] = true
				p.mu.Unlock()
				return acc, nil
			}
		}

		hasLive := false
		p.store.mu.Lock()
		for _, a := range p.store.Accounts {
			if isAccountEnabled(a) && len(extractCookiesMap(a["cookies"])) > 0 {
				hasLive = true
				break
			}
		}
		p.store.mu.Unlock()
		if !hasLive {
			p.mu.Unlock()
			return nil, &MuseAuthError{Msg: "No active accounts available; please import cookies in the Admin Console"}
		}

		waitCh := p.notifyCh
		p.mu.Unlock()

		waitDur := remaining
		if waitDur > 500*time.Millisecond {
			waitDur = 500 * time.Millisecond
		}
		timer := time.NewTimer(waitDur)
		select {
		case <-waitCh:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (p *AccountPoolManager) Release(accountID string) {
	if accountID == "" {
		return
	}
	p.mu.Lock()
	delete(p.BusyAccounts, accountID)
	close(p.notifyCh)
	p.notifyCh = make(chan struct{})
	p.mu.Unlock()
}

var (
	renewAndPersistHook func(accID string, wakeVM, force bool) (map[string]any, error)
	syncCookiesHook     func(accID string) map[string]any
)

func RenewAndPersist(accID string, wakeVM, force bool) (map[string]any, error) {
	if renewAndPersistHook != nil {
		return renewAndPersistHook(accID, wakeVM, force)
	}
	acc := store.GetAccount(accID)
	if acc == nil {
		return nil, nil
	}
	cookies := extractCookiesMap(acc["cookies"])
	if len(cookies) == 0 {
		return acc, nil
	}
	now := nowFloat()
	syncedAt, _ := toInt64(acc["synced_at"])
	lastKA, _ := toInt64(acc["last_keepalive"])
	engine.renewMu.Lock()
	lastHTTP := int64(engine.lastHTTPRenew[accID])
	engine.renewMu.Unlock()

	lastSync := syncedAt
	if lastKA > lastSync {
		lastSync = lastKA
	}
	if lastHTTP > lastSync {
		lastSync = lastHTTP
	}

	okVal, hasOK := acc["ok"].(bool)
	if !force && hasOK && okVal && (now-float64(lastSync)) < 600 {
		engine.renewMu.Lock()
		engine.lastHTTPRenew[accID] = float64(lastSync)
		engine.renewMu.Unlock()
		return acc, nil
	}

	res, err := RenewSessionHTTP(cookies, extractExpMap(acc["cookies_exp"]), wakeVM)
	if err != nil {
		if isAuthErr(err) {
			store.Mark(accID, false, err.Error())
			return nil, err
		}
		log.Printf("[WARN] HTTP session pre-renewal warning for account %s: %v", accID, err)
		return store.GetAccount(accID), nil
	}
	engine.renewMu.Lock()
	engine.lastHTTPRenew[accID] = now
	engine.renewMu.Unlock()

	if newCookies, ok := res["cookies"].(map[string]string); ok && len(newCookies) > 0 {
		fields := map[string]any{
			"cookies":     newCookies,
			"cookies_exp": res["cookies_exp"],
			"synced_at":   int64(now),
		}
		if res["ok"] == true {
			fields["ok"] = true
		}
		store.UpdateAccount(accID, fields)
		vmState, _ := res["vm_state"].(string)
		if vmState == "" {
			vmState = "RUNNING"
		}
		tVal := true
		store.TouchKeepalive(accID, &tVal, fmt.Sprintf("Session OK (VM: %s)", vmState))
	}
	return store.GetAccount(accID), nil
}

func SyncCookies(accID string) map[string]any {
	if syncCookiesHook != nil {
		return syncCookiesHook(accID)
	}
	live := engine.ReadCookies(accID)
	if len(live) == 0 {
		return map[string]any{"synced": 0}
	}
	acc := store.GetAccount(accID)
	if acc == nil {
		return map[string]any{"synced": 0}
	}
	cur := extractCookiesMap(acc["cookies"])
	newVals := map[string]string{}
	for k, v := range live {
		if val, ok := v["value"].(string); ok && val != "" {
			newVals[k] = val
		}
	}
	exps := extractExpMap(acc["cookies_exp"])
	for k, v := range live {
		if exp, ok := toInt64(v["expires"]); ok && exp > 0 {
			exps[k] = exp
		}
	}
	changed := 0
	merged := map[string]string{}
	for k, v := range cur {
		merged[k] = v
	}
	for k, v := range newVals {
		if cur[k] != v {
			changed++
		}
		merged[k] = v
	}
	store.UpdateAccount(accID, map[string]any{
		"cookies":     merged,
		"cookies_exp": exps,
		"synced_at":   nowUnix(),
	})
	updated := store.GetAccount(accID)
	var expAt any
	if updated != nil {
		expAt = updated["expires_at"]
	}
	return map[string]any{
		"synced":     len(newVals),
		"changed":    changed,
		"expires_at": expAt,
	}
}

type StreamItem struct {
	Chunk string
	Err   error
}

func SafeChatStream(cookies map[string]string, prompt string, expires map[string]int64, timeout int, accountID string) (<-chan StreamItem, func()) {
	ch := make(chan StreamItem, 100)
	stopCh := make(chan struct{})
	var stopOnce sync.Once
	cancel := func() {
		stopOnce.Do(func() {
			close(stopCh)
		})
	}

	go func() {
		curID := accountID
		curCookies := cookies
		curExp := expires
		var lastErr error
		leasedID := ""

		defer func() {
			if leasedID != "" {
				ACCOUNT_POOL.Release(leasedID)
			}
			if GEN_LOCK.Acquire(true, 30.0) {
				engine.CloseCurrentPage(curID)
				GEN_LOCK.Release()
			}
			close(ch)
		}()

		for attempt := 0; attempt < 2; attempt++ {
			select {
			case <-stopCh:
				return
			default:
			}
			if attempt > 0 {
				if leasedID != "" {
					ACCOUNT_POOL.Release(leasedID)
					leasedID = ""
				}
				alt, err := ACCOUNT_POOL.Acquire(AcquireAccountOptions{
					Rotate:      true,
					ForceRotate: true,
					TimeoutSec:  float64(timeout),
				})
				if err != nil || alt == nil {
					break
				}
				altID, _ := alt["id"].(string)
				if altID == curID {
					leasedID = altID
					break
				}
				curID = altID
				leasedID = curID
				curCookies = extractCookiesMap(alt["cookies"])
				curExp = extractExpMap(alt["cookies_exp"])
				log.Printf("[INFO] [Chat Failover] Switching to backup account %v (%s)...", alt["label"], curID)
			} else if curID != "" {
				if _, err := ACCOUNT_POOL.Acquire(AcquireAccountOptions{
					AccountID:  curID,
					TimeoutSec: float64(timeout),
				}); err != nil {
					lastErr = err
					break
				}
				leasedID = curID
			}

			yielded := false
			err := func() error {
				if curID != "" {
					refreshed, rErr := RenewAndPersist(curID, true, attempt > 0)
					if rErr != nil {
						return rErr
					}
					if refreshed != nil {
						curCookies = extractCookiesMap(refreshed["cookies"])
						curExp = extractExpMap(refreshed["cookies_exp"])
					}
				}
				if !GEN_LOCK.Acquire(true, float64(timeout)) {
					return &MuseGenerationError{Msg: "Timed out waiting for browser queue; please retry later"}
				}
				defer GEN_LOCK.Release()

				select {
				case <-stopCh:
					return nil
				default:
				}
				if err := engine.Start(); err != nil {
					return err
				}
				return engine.ChatStream(curCookies, prompt, curExp, timeout, curID, stopCh, func(chunk string) {
					yielded = true
					select {
					case ch <- StreamItem{Chunk: chunk}:
					case <-stopCh:
					}
				})
			}()

			if err == nil {
				if curID != "" {
					store.Mark(curID, true, "")
					SyncCookies(curID)
				}
				return
			}
			lastErr = err
			if isAuthErr(err) {
				if curID != "" {
					store.Mark(curID, false, err.Error())
				}
				engine.CloseCurrentPage(curID)
				if yielded {
					break
				}
			} else {
				engine.ResetThread(curID, true)
				if yielded {
					break
				}
			}
		}
		if lastErr != nil {
			select {
			case ch <- StreamItem{Err: lastErr}:
			case <-stopCh:
			}
		}
	}()

	return ch, cancel
}

var (
	runGenerationHook       func(prompt, kind string, timeout int, accountID string, onProgress func(int), referenceImage string, onAcquired func()) (map[string]any, string, error)
	runGenerationLockedHook func(prompt, kind string, timeout int, accountID string, onProgress func(int), referenceImage string, deadline time.Time) (map[string]any, string, error)
)

func RunGeneration(prompt, kind string, timeout int, accountID string, onProgress func(int), referenceImage string, onAcquired func()) (map[string]any, string, error) {
	if runGenerationHook != nil {
		return runGenerationHook(prompt, kind, timeout, accountID, onProgress, referenceImage, onAcquired)
	}
	if timeout < 1 {
		timeout = 1
	}
	deadline := time.Now().Add(time.Duration(timeout) * time.Second)
	if !GEN_LOCK.Acquire(true, float64(timeout)) {
		return nil, "", &MuseGenerationError{Msg: "Timed out waiting for browser queue; please retry later"}
	}
	defer GEN_LOCK.Release()

	if onAcquired != nil {
		onAcquired()
	}
	return RunGenerationLocked(prompt, kind, timeout, accountID, onProgress, referenceImage, deadline)
}

func RunGenerationLocked(prompt, kind string, timeout int, accountID string, onProgress func(int), referenceImage string, deadline time.Time) (map[string]any, string, error) {
	if runGenerationLockedHook != nil {
		return runGenerationLockedHook(prompt, kind, timeout, accountID, onProgress, referenceImage, deadline)
	}
	remWait := time.Until(deadline).Seconds()
	if remWait < 1 {
		remWait = 1
	}
	preferredID := engine.CurrentAccID
	if kind == "video" && accountID == "" {
		recent := store.ListTasks(200)
		for _, task := range recent {
			if task["kind"] != "video" || task["status"] != "completed" {
				continue
			}
			accIDStr, _ := task["account"].(string)
			if accIDStr == "" {
				continue
			}
			candidate := store.GetAccount(accIDStr)
			if candidate != nil && isAccountEnabled(candidate) {
				okVal, hasOK := candidate["ok"].(bool)
				if !hasOK || okVal {
					preferredID = accIDStr
					break
				}
			}
		}
	}

	acc, err := ACCOUNT_POOL.Acquire(AcquireAccountOptions{
		AccountID:   accountID,
		PreferredID: preferredID,
		Rotate:      true,
		TimeoutSec:  remWait,
	})
	if err != nil {
		return nil, "", err
	}
	curAcc := acc
	var lastErr error

	defer func() {
		var curID string
		if curAcc != nil {
			curID, _ = curAcc["id"].(string)
			ACCOUNT_POOL.Release(curID)
		}
		engine.CloseCurrentPage(curID)
	}()

	for attempt := 0; attempt < 2; attempt++ {
		if time.Now().After(deadline) {
			return nil, "", &MuseGenerationError{Msg: "Total task timeout reached; stopping retries"}
		}
		if attempt > 0 {
			if lastErr != nil && strings.Contains(lastErr.Error(), "No media attachment produced; only text reply returned") {
				break
			}
			oldID := ""
			if curAcc != nil {
				oldID, _ = curAcc["id"].(string)
				ACCOUNT_POOL.Release(oldID)
				curAcc = nil
			}
			rem := time.Until(deadline).Seconds()
			if rem <= 0 {
				break
			}
			alt, aErr := ACCOUNT_POOL.Acquire(AcquireAccountOptions{
				Rotate:      true,
				ForceRotate: true,
				TimeoutSec:  rem,
			})
			if aErr != nil || alt == nil {
				break
			}
			altID, _ := alt["id"].(string)
			if oldID != "" && altID == oldID {
				curAcc = alt
				break
			}
			curAcc = alt
			log.Printf("[INFO] [Media Failover] Switching to backup account %v (%s)...", curAcc["label"], altID)
		}

		curID, _ := curAcc["id"].(string)
		refreshed, rErr := RenewAndPersist(curID, true, attempt > 0)
		if rErr != nil {
			lastErr = rErr
			store.Mark(curID, false, rErr.Error())
			engine.CloseCurrentPage(curID)
			continue
		}
		if refreshed != nil {
			curAcc = refreshed
		}
		if err := engine.Start(); err != nil {
			lastErr = err
			engine.CloseCurrentPage(curID)
			continue
		}
		remaining := int(time.Until(deadline).Seconds())
		if remaining <= 0 {
			return nil, "", &MuseGenerationError{Msg: "Total task timeout reached; stopping retries"}
		}
		res, gErr := engine.Generate(
			extractCookiesMap(curAcc["cookies"]),
			prompt,
			kind,
			remaining,
			extractExpMap(curAcc["cookies_exp"]),
			curID,
			onProgress,
			referenceImage,
			nil,
		)
		if gErr == nil {
			store.Mark(curID, true, "")
			SyncCookies(curID)
			return res, curID, nil
		}
		lastErr = gErr
		if isAuthErr(gErr) {
			store.Mark(curID, false, gErr.Error())
			engine.CloseCurrentPage(curID)
		} else if isGenErr(gErr) {
			note := gErr.Error()
			if len(note) > 60 {
				note = note[:60]
			}
			store.Mark(curID, true, "Task warning: "+note)
			engine.ResetThread(curID, false)
		} else {
			engine.CloseCurrentPage(curID)
			lastErr = &MuseGenerationError{Msg: fmt.Sprintf("Generation failed: %v", gErr)}
		}
	}
	if lastErr == nil {
		lastErr = &MuseGenerationError{Msg: "Generation failed"}
	}
	return nil, "", lastErr
}
