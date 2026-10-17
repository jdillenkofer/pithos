package lua

import "errors"

var errExecutionLimit = errors.New("Lua policy execution limit exceeded")

// NewSimulator uses the production compiler, libraries and request evaluator,
// with a one-million VM instruction limit for both initialization and calls.
// The limit is deliberately not a Lua error: policy pcall cannot swallow it.
func NewSimulator(code string) (*LuaAuthorizer, error) {
	a, err := NewLuaAuthorizer(code)
	if err != nil {
		return nil, err
	}
	a.bounded = true
	return a, nil
}

// ReloadCode validates before atomically publishing code to subsequent requests.
// Existing requests retain their original immutable chunk and proxy settings.
func (a *LuaAuthorizer) ReloadCode(code string) error {
	candidate, err := NewSimulator(code)
	if err != nil {
		return err
	}
	a.codeMu.Lock()
	a.code, a.chunk = candidate.code, candidate.chunk
	a.codeMu.Unlock()
	return nil
}
