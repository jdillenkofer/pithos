package session

import (
	"sync"
	"time"
)

type attemptWindow struct {
	count int
	reset time.Time
}

// LoginLimiter bounds both login attempts and memory use. Client identities must
// come from the socket unless a forwarding proxy has explicitly been trusted.
type LoginLimiter struct {
	mu      sync.Mutex
	windows map[string]attemptWindow
	now     func() time.Time
}

func NewLoginLimiter() *LoginLimiter {
	return &LoginLimiter{windows: make(map[string]attemptWindow), now: time.Now}
}
func (l *LoginLimiter) Allow(ip, key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	for k, w := range l.windows {
		if !now.Before(w.reset) {
			delete(l.windows, k)
		}
	}
	ids := []string{"ip:" + ip, "key:" + Hash(key)}
	limits := []int{20, 10}
	for i, id := range ids {
		w, ok := l.windows[id]
		if !ok && len(l.windows) >= 10000 {
			return false
		}
		if w.count >= limits[i] {
			return false
		}
	}
	for _, id := range ids {
		w, ok := l.windows[id]
		if !ok {
			w.reset = now.Add(time.Minute)
		}
		w.count++
		l.windows[id] = w
	}
	return true
}
