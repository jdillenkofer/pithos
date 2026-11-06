package session

import (
	"fmt"
	"testing"
	"time"
)

func newTestLimiter(now *time.Time) *LoginLimiter {
	l := NewLoginLimiter()
	l.now = func() time.Time { return *now }
	return l
}

func TestLoginLimiterLimitsPerClientAndResets(t *testing.T) {
	now := time.Unix(0, 0)
	l := newTestLimiter(&now)
	for i := 0; i < loginAttemptsPerMinute; i++ {
		if !l.Allow("192.0.2.1") {
			t.Fatalf("attempt %d rejected", i+1)
		}
	}
	if l.Allow("192.0.2.1") {
		t.Fatal("attempt over the limit allowed")
	}
	if !l.Allow("192.0.2.2") {
		t.Fatal("other client rejected")
	}
	now = now.Add(time.Minute)
	if !l.Allow("192.0.2.1") {
		t.Fatal("attempt after window reset rejected")
	}
}

func TestLoginLimiterGroupsIPv6By64(t *testing.T) {
	now := time.Unix(0, 0)
	l := newTestLimiter(&now)
	for i := 0; i < loginAttemptsPerMinute; i++ {
		if !l.Allow(fmt.Sprintf("2001:db8:1:2::%x", i+1)) {
			t.Fatalf("attempt %d rejected", i+1)
		}
	}
	if l.Allow("2001:db8:1:2:ffff::1") {
		t.Fatal("address in the same /64 allowed over the limit")
	}
	if !l.Allow("2001:db8:1:3::1") {
		t.Fatal("address in another /64 rejected")
	}
}

func TestLoginLimiterTreatsMappedIPv4AsIPv4(t *testing.T) {
	if got := clientID("::ffff:192.0.2.1"); got != "192.0.2.1" {
		t.Fatalf("clientID = %q", got)
	}
}

func TestLoginLimiterRejectsNewClientsWhenFull(t *testing.T) {
	now := time.Unix(0, 0)
	l := newTestLimiter(&now)
	for i := 0; i < maxLimiterWindows; i++ {
		if !l.Allow(fmt.Sprintf("10.%d.%d.%d", i>>16&0xff, i>>8&0xff, i&0xff)) {
			t.Fatalf("client %d rejected", i)
		}
	}
	if l.Allow("192.0.2.1") {
		t.Fatal("new client allowed while full")
	}
	if !l.Allow("10.0.0.0") {
		t.Fatal("known client rejected while full")
	}
	now = now.Add(time.Minute)
	if !l.Allow("192.0.2.1") {
		t.Fatal("new client rejected after windows expired")
	}
}
