package pkcs11

import (
	"testing"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	miekgpkcs11 "github.com/miekg/pkcs11"
)

type fakeCryptokiContext struct {
	initializeErr   error
	initializeCalls int
	finalizeCalls   int
	destroyCalls    int
	loginCalls      int
	logoutCalls     int
	slots           []uint
	tokenLabel      string
	sessionFlags    uint
	objectHandles   []miekgpkcs11.ObjectHandle
}

func (f *fakeCryptokiContext) Initialize(...miekgpkcs11.InitializeOption) error {
	f.initializeCalls++
	return f.initializeErr
}

func (f *fakeCryptokiContext) Finalize() error {
	f.finalizeCalls++
	return nil
}

func (f *fakeCryptokiContext) Destroy() {
	f.destroyCalls++
}

func (f *fakeCryptokiContext) GetSlotList(bool) ([]uint, error) {
	return f.slots, nil
}

func (f *fakeCryptokiContext) GetTokenInfo(uint) (miekgpkcs11.TokenInfo, error) {
	return miekgpkcs11.TokenInfo{Label: f.tokenLabel}, nil
}

func (f *fakeCryptokiContext) OpenSession(_ uint, flags uint) (miekgpkcs11.SessionHandle, error) {
	f.sessionFlags = flags
	return 1, nil
}

func (f *fakeCryptokiContext) CloseSession(miekgpkcs11.SessionHandle) error {
	return nil
}

func (f *fakeCryptokiContext) Login(miekgpkcs11.SessionHandle, uint, string) error {
	f.loginCalls++
	return nil
}

func (f *fakeCryptokiContext) Logout(miekgpkcs11.SessionHandle) error {
	f.logoutCalls++
	return nil
}

func (f *fakeCryptokiContext) FindObjectsInit(miekgpkcs11.SessionHandle, []*miekgpkcs11.Attribute) error {
	return nil
}

func (f *fakeCryptokiContext) FindObjects(miekgpkcs11.SessionHandle, int) ([]miekgpkcs11.ObjectHandle, bool, error) {
	return f.objectHandles, false, nil
}

func (f *fakeCryptokiContext) FindObjectsFinal(miekgpkcs11.SessionHandle) error {
	return nil
}

func (f *fakeCryptokiContext) EncryptInit(miekgpkcs11.SessionHandle, []*miekgpkcs11.Mechanism, miekgpkcs11.ObjectHandle) error {
	return nil
}

func (f *fakeCryptokiContext) Encrypt(miekgpkcs11.SessionHandle, []byte) ([]byte, error) {
	return nil, nil
}

func (f *fakeCryptokiContext) DecryptInit(miekgpkcs11.SessionHandle, []*miekgpkcs11.Mechanism, miekgpkcs11.ObjectHandle) error {
	return nil
}

func (f *fakeCryptokiContext) Decrypt(miekgpkcs11.SessionHandle, []byte) ([]byte, error) {
	return nil, nil
}

func TestModulePoolSharesModuleUntilLastRelease(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fake := &fakeCryptokiContext{}
	loadCalls := 0
	pool := newModulePool(func(string) cryptokiContext {
		loadCalls++
		return fake
	})

	first, err := pool.acquire("/module.so")
	if err != nil {
		t.Fatal(err)
	}
	second, err := pool.acquire("/module.so")
	if err != nil {
		t.Fatal(err)
	}

	if first != second {
		t.Fatal("expected the same shared module")
	}
	if loadCalls != 1 || fake.initializeCalls != 1 {
		t.Fatalf("module loaded %d times and initialized %d times", loadCalls, fake.initializeCalls)
	}

	if err := pool.release(first); err != nil {
		t.Fatal(err)
	}
	if fake.finalizeCalls != 0 || fake.destroyCalls != 0 {
		t.Fatal("module released while it still had a user")
	}

	if err := pool.release(second); err != nil {
		t.Fatal(err)
	}
	if fake.finalizeCalls != 1 || fake.destroyCalls != 1 {
		t.Fatalf("module finalized %d times and destroyed %d times", fake.finalizeCalls, fake.destroyCalls)
	}
}

func TestModulePoolDoesNotFinalizeExternalInitialization(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fake := &fakeCryptokiContext{
		initializeErr: miekgpkcs11.Error(miekgpkcs11.CKR_CRYPTOKI_ALREADY_INITIALIZED),
	}
	pool := newModulePool(func(string) cryptokiContext {
		return fake
	})

	module, err := pool.acquire("/module.so")
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.release(module); err != nil {
		t.Fatal(err)
	}

	if fake.finalizeCalls != 0 {
		t.Fatalf("externally initialized module finalized %d times", fake.finalizeCalls)
	}
	if fake.destroyCalls != 1 {
		t.Fatalf("module handle destroyed %d times", fake.destroyCalls)
	}
}

func TestSharedModuleKeepsTokenLoggedInUntilLastRelease(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fake := &fakeCryptokiContext{}
	module := &sharedModule{
		ctx:    fake,
		logins: make(map[uint]*tokenLogin),
	}

	if err := module.acquireLogin(1, 10, "pin"); err != nil {
		t.Fatal(err)
	}
	if err := module.acquireLogin(1, 11, "pin"); err != nil {
		t.Fatal(err)
	}
	if fake.loginCalls != 1 {
		t.Fatalf("token logged in %d times", fake.loginCalls)
	}

	if err := module.releaseLogin(1, 10); err != nil {
		t.Fatal(err)
	}
	if fake.logoutCalls != 0 {
		t.Fatal("token logged out while it still had a user")
	}

	if err := module.releaseLogin(1, 11); err != nil {
		t.Fatal(err)
	}
	if fake.logoutCalls != 1 {
		t.Fatalf("token logged out %d times", fake.logoutCalls)
	}
}

func TestSharedModuleRejectsDifferentPINForLoggedInToken(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fake := &fakeCryptokiContext{}
	module := &sharedModule{
		ctx:    fake,
		logins: make(map[uint]*tokenLogin),
	}

	if err := module.acquireLogin(1, 10, "first-pin"); err != nil {
		t.Fatal(err)
	}
	err := module.acquireLogin(1, 11, "other-pin")
	if err == nil {
		t.Fatal("expected a PIN mismatch error")
	}
}

func TestFindKeyByLabelRejectsMultipleMatches(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fake := &fakeCryptokiContext{
		objectHandles: []miekgpkcs11.ObjectHandle{1, 2},
	}

	_, err := findKeyByLabel(fake, 1, "duplicate-key")
	if err == nil {
		t.Fatal("expected an ambiguous key label error")
	}
}

func TestNewAEADOpensReadOnlySession(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fake := &fakeCryptokiContext{
		slots:         []uint{1},
		tokenLabel:    "token",
		objectHandles: []miekgpkcs11.ObjectHandle{2},
	}
	originalModules := modules
	modules = newModulePool(func(string) cryptokiContext {
		return fake
	})
	t.Cleanup(func() {
		modules = originalModules
	})

	aead, err := NewAEAD("/module.so", "token", "pin", "key")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := aead.Close(); err != nil {
			t.Error(err)
		}
	})

	if fake.sessionFlags != miekgpkcs11.CKF_SERIAL_SESSION {
		t.Fatalf("session flags = %#x, want %#x", fake.sessionFlags, miekgpkcs11.CKF_SERIAL_SESSION)
	}
}
