package session

import (
	"net/netip"
	"sync"
	"time"
)

const (
	loginAttemptsPerMinute = 20
	maxLimiterWindows      = 10000
)

type attemptWindow struct {
	count int
	reset time.Time
}

// LoginLimiter bounds both login attempts and memory use. Client identities must
// come from the socket unless a forwarding proxy has explicitly been trusted.
// Attempts are limited per client only: a per-access-key limit would let anyone
// who knows an admin's key lock that admin out.
type LoginLimiter struct {
	mu      sync.Mutex
	windows map[string]attemptWindow
	now     func() time.Time
}

func NewLoginLimiter() *LoginLimiter {
	return &LoginLimiter{windows: make(map[string]attemptWindow), now: time.Now}
}

func (l *LoginLimiter) Allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	for k, w := range l.windows {
		if !now.Before(w.reset) {
			delete(l.windows, k)
		}
	}
	id := clientID(ip)
	w, ok := l.windows[id]
	if !ok {
		if len(l.windows) >= maxLimiterWindows {
			return false
		}
		w.reset = now.Add(time.Minute)
	}
	if w.count >= loginAttemptsPerMinute {
		return false
	}
	w.count++
	l.windows[id] = w
	return true
}

// clientID groups IPv6 clients by their /64 prefix, which a single subscriber
// usually controls in full.
func clientID(ip string) string {
	addr, err := netip.ParseAddr(ip)
	if err != nil {
		return ip
	}
	addr = addr.Unmap()
	if addr.Is6() {
		prefix, err := addr.WithZone("").Prefix(64)
		if err == nil {
			return prefix.String()
		}
	}
	return addr.String()
}
