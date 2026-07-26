package pkcs11

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"

	miekgpkcs11 "github.com/miekg/pkcs11"
)

type tokenLogin struct {
	refs      int
	pinDigest [sha256.Size]byte
	ownsLogin bool
}

type sharedModule struct {
	ctx                cryptokiContext
	path               string
	refs               int
	ownsInitialization bool
	pool               *modulePool

	loginMu sync.Mutex
	logins  map[uint]*tokenLogin
}

type modulePool struct {
	mu      sync.Mutex
	modules map[string]*sharedModule
	load    func(string) cryptokiContext
}

func newModulePool(load func(string) cryptokiContext) *modulePool {
	return &modulePool{
		modules: make(map[string]*sharedModule),
		load:    load,
	}
}

var modules = newModulePool(loadCryptokiContext)

func (p *modulePool) acquire(modulePath string) (*sharedModule, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if module, ok := p.modules[modulePath]; ok {
		module.refs++
		return module, nil
	}

	ctx := p.load(modulePath)
	if ctx == nil {
		return nil, fmt.Errorf("failed to load PKCS#11 module: %s", modulePath)
	}

	ownsInitialization := true
	if err := ctx.Initialize(); err != nil {
		if !errors.Is(err, miekgpkcs11.Error(miekgpkcs11.CKR_CRYPTOKI_ALREADY_INITIALIZED)) {
			ctx.Destroy()
			return nil, fmt.Errorf("failed to initialize PKCS#11: %w", err)
		}
		ownsInitialization = false
	}

	module := &sharedModule{
		ctx:                ctx,
		path:               modulePath,
		refs:               1,
		ownsInitialization: ownsInitialization,
		pool:               p,
		logins:             make(map[uint]*tokenLogin),
	}
	p.modules[modulePath] = module
	return module, nil
}

func (p *modulePool) release(module *sharedModule) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	current, ok := p.modules[module.path]
	if !ok || current != module {
		return errors.New("PKCS#11 module is not acquired")
	}

	module.refs--
	if module.refs > 0 {
		return nil
	}

	delete(p.modules, module.path)

	var err error
	if module.ownsInitialization {
		if finalizeErr := module.ctx.Finalize(); finalizeErr != nil {
			err = fmt.Errorf("failed to finalize: %w", finalizeErr)
		}
	}
	module.ctx.Destroy()
	return err
}

func (m *sharedModule) acquireLogin(slotID uint, session miekgpkcs11.SessionHandle, pin string) error {
	m.loginMu.Lock()
	defer m.loginMu.Unlock()

	pinDigest := sha256.Sum256([]byte(pin))
	if login, ok := m.logins[slotID]; ok {
		if login.pinDigest != pinDigest {
			return errors.New("PKCS#11 token is already in use with a different PIN")
		}
		login.refs++
		return nil
	}

	ownsLogin := true
	if err := m.ctx.Login(session, miekgpkcs11.CKU_USER, pin); err != nil {
		if !errors.Is(err, miekgpkcs11.Error(miekgpkcs11.CKR_USER_ALREADY_LOGGED_IN)) {
			return err
		}
		ownsLogin = false
	}

	m.logins[slotID] = &tokenLogin{
		refs:      1,
		pinDigest: pinDigest,
		ownsLogin: ownsLogin,
	}
	return nil
}

func (m *sharedModule) releaseLogin(slotID uint, session miekgpkcs11.SessionHandle) error {
	m.loginMu.Lock()
	defer m.loginMu.Unlock()

	login, ok := m.logins[slotID]
	if !ok {
		return errors.New("PKCS#11 token login is not acquired")
	}

	login.refs--
	if login.refs > 0 {
		return nil
	}

	delete(m.logins, slotID)
	if login.ownsLogin {
		return m.ctx.Logout(session)
	}
	return nil
}
