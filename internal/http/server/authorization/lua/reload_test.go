package lua

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
)

func TestReloadCodePublishesAndRetainsLastGood(t *testing.T) {
	a, err := NewLuaAuthorizer(`function authorizeRequest(r) return true end`)
	if err != nil {
		t.Fatal(err)
	}
	request := func() *authorization.Request { return &authorization.Request{Operation: "GetObject"} }
	if err = a.ReloadCode(`function authorizeRequest(r) return false end`); err != nil {
		t.Fatal(err)
	}
	if err = a.ReloadCode(`while true do end`); err == nil {
		t.Fatal("accepted unbounded policy")
	}
	decision, err := a.AuthorizeRequest(context.Background(), request())
	if err != nil || decision.Effect != authorization.ExplicitDeny {
		t.Fatalf("lost last good policy: %+v %v", decision, err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			if err := a.ReloadCode(`function authorizeRequest(r) return true end`); err != nil {
				t.Error(err)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			if _, err := a.AuthorizeRequest(context.Background(), request()); err != nil {
				t.Error(err)
			}
		}
	}()
	wg.Wait()
}

// Top-level setup above the former one-million instruction simulator budget
// must start, reload and simulate alike.
func TestReloadAndCandidateUseConfiguredLimits(t *testing.T) {
	heavy := `local t = {} for i = 1, 600000 do t[i] = i end function authorizeRequest(r) return true end`
	a, err := NewLuaAuthorizerWithOptions(heavy, Options{MaxExecutionTime: time.Minute})
	if err != nil {
		t.Fatalf("startup rejected policy within limits: %v", err)
	}
	if err = a.ReloadCode(heavy); err != nil {
		t.Fatalf("reload rejected policy that starts: %v", err)
	}
	if _, err = a.Candidate(heavy); err != nil {
		t.Fatalf("candidate rejected policy that starts: %v", err)
	}

	strict, err := NewLuaAuthorizerWithOptions(`function authorizeRequest(r) return true end`, Options{MaxExecutionTime: time.Minute, MaxInstructions: 100_000})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = strict.Candidate(heavy); err == nil || !strings.Contains(err.Error(), errInstructionLimitExceeded.Error()) {
		t.Fatalf("candidate ignored configured instruction limit: %v", err)
	}
	if err = strict.ReloadCode(heavy); err == nil {
		t.Fatal("reload ignored configured instruction limit")
	}
}
