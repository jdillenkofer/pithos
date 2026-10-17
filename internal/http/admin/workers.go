package admin

import (
	"context"
	"net/http"
	"sync"
)

// backgroundWorkers serializes shutdown against new jobs so Wait never races
// the first Add. The admin listener is shut down before Close is called.
type backgroundWorkers struct {
	mu      sync.Mutex
	closing bool
	wg      sync.WaitGroup
}

func (g *backgroundWorkers) goRun(work func()) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closing {
		return false
	}
	g.wg.Add(1)
	go func() { defer g.wg.Done(); work() }()
	return true
}
func (g *backgroundWorkers) wait() { g.mu.Lock(); g.closing = true; g.mu.Unlock(); g.wg.Wait() }

// Handler owns cleanup and validation workers as well as the router. Close must
// run before closing its database or stopping storage.
type Handler struct {
	http.Handler
	cancel  context.CancelFunc
	workers *backgroundWorkers
}

func (h *Handler) Close() error { h.cancel(); h.workers.wait(); return nil }
