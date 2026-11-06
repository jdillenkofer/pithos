package lua

// Candidate compiles and dry-runs code with the same limits and proxy settings
// as this authorizer, without publishing it. Validating and simulating a policy
// with the candidate therefore matches what requests would see after a reload.
func (a *LuaAuthorizer) Candidate(code string) (*LuaAuthorizer, error) {
	return newLuaAuthorizer(code, a.maxExecutionTime, a.maxInstructions, a.proxyResolver)
}

// ReloadCode validates before atomically publishing code to subsequent requests.
// Existing requests retain their original immutable chunk and proxy settings.
func (a *LuaAuthorizer) ReloadCode(code string) error {
	candidate, err := a.Candidate(code)
	if err != nil {
		return err
	}
	a.codeMu.Lock()
	a.code, a.chunk = candidate.code, candidate.chunk
	a.codeMu.Unlock()
	return nil
}
