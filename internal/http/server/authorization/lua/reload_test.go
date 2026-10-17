package lua

import (
	"context"
	"sync"
	"testing"

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
