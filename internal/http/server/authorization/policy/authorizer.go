package policy

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
)

// Authorizer atomically publishes immutable compiled snapshots. A failed
// refresh is logged and leaves the last valid snapshot active.
type Authorizer struct {
	path     string
	snapshot atomic.Pointer[Snapshot]
	cancel   context.CancelFunc
	loadedAt atomic.Int64
	proxy    *authorization.ProxyResolver
}

func NewAuthorizer(path string, interval time.Duration) (*Authorizer, error) {
	return NewAuthorizerWithOptions(path, interval, Options{})
}

type Options struct {
	TrustForwardedHeaders bool
	TrustedProxyCIDRs     []string
}

func NewAuthorizerWithOptions(path string, interval time.Duration, options Options) (*Authorizer, error) {
	s, err := Load(path)
	if err != nil {
		return nil, err
	}
	proxy, err := authorization.NewProxyResolver(authorization.ProxyOptions{
		TrustForwardedHeaders: options.TrustForwardedHeaders,
		TrustedProxyCIDRs:     options.TrustedProxyCIDRs,
	})
	if err != nil {
		return nil, err
	}
	a := &Authorizer{
		path:  path,
		proxy: proxy,
	}
	a.snapshot.Store(s)
	a.loadedAt.Store(time.Now().UnixNano())
	authorization.ObserveReload("policy", true)
	if interval > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		a.cancel = cancel
		go a.reloadLoop(ctx, interval)
	}
	return a, nil
}
func (a *Authorizer) reloadLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			started := time.Now()
			if err := a.Reload(); err != nil {
				slog.Error("Policy reload failed; retaining previous snapshot", "path", a.path, "error", err)
				continue
			}
			slog.Info("Policy snapshot reloaded", "path", a.path, "duration", time.Since(started))
		}
	}
}

func (a *Authorizer) Reload() error {
	s, err := Load(a.path)
	if err != nil {
		authorization.ObserveReload("policy", false)
		return err
	}
	a.snapshot.Store(s)
	a.loadedAt.Store(time.Now().UnixNano())
	authorization.ObserveReload("policy", true)
	return nil
}
func (a *Authorizer) Close() error {
	if a.cancel != nil {
		a.cancel()
	}
	return nil
}
func (a *Authorizer) AuthorizeRequest(ctx context.Context, r *authorization.Request) (authorization.Decision, error) {
	started := time.Now()
	r.HttpRequest.ClientIP, r.HttpRequest.Scheme = a.proxy.Resolve(r.HttpRequest)
	d, err := a.snapshot.Load().AuthorizeRequest(ctx, r)
	authorization.ObserveDecision("policy", d.Effect, started)
	authorization.SetSnapshotAge("policy", time.Since(time.Unix(0, a.loadedAt.Load())))
	return d, err
}
