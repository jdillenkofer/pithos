package partstore

import (
	"context"
	"io"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakePartStore is a no-op PartStore with a configurable tx-free capability.
type fakePartStore struct {
	txFree bool
}

func (f *fakePartStore) Start(ctx context.Context) error { return nil }
func (f *fakePartStore) Stop(ctx context.Context) error  { return nil }
func (f *fakePartStore) PutPart(ctx context.Context, tx database.Tx, partId PartId, reader io.Reader) error {
	return nil
}
func (f *fakePartStore) GetPart(ctx context.Context, tx database.Tx, partId PartId) (io.ReadCloser, error) {
	return nil, ErrPartNotFound
}
func (f *fakePartStore) GetPartIds(ctx context.Context, tx database.Tx) ([]PartId, error) {
	return nil, nil
}
func (f *fakePartStore) DeletePart(ctx context.Context, tx database.Tx, partId PartId) error {
	return nil
}
func (f *fakePartStore) SupportsTxFreeGetPart() bool { return f.txFree }

func TestNamedPartStoresRoutesClassesToMappedStores(t *testing.T) {
	testutils.SkipIfIntegration(t)

	defaultStore := &fakePartStore{}
	coldStore := &fakePartStore{}
	stores, err := NewNamedPartStores(defaultStore, map[string]PartStore{"cold": coldStore}, map[string]string{
		"GLACIER":      "cold",
		"DEEP_ARCHIVE": "cold",
		"STANDARD_IA":  DefaultPartStoreName,
	})
	require.NoError(t, err)

	name, store := stores.StoreForClass("GLACIER")
	require.NotNil(t, name)
	assert.Equal(t, "cold", *name)
	assert.Same(t, coldStore, store)

	// Unmapped classes fall back to the default store, recorded as nil.
	name, store = stores.StoreForClass("STANDARD")
	assert.Nil(t, name)
	assert.Same(t, defaultStore, store)

	// A class explicitly mapped to the default name is also recorded as nil.
	name, store = stores.StoreForClass("STANDARD_IA")
	assert.Nil(t, name)
	assert.Same(t, defaultStore, store)
}

func TestNamedPartStoresResolvesByName(t *testing.T) {
	testutils.SkipIfIntegration(t)

	defaultStore := &fakePartStore{}
	coldStore := &fakePartStore{}
	stores, err := NewNamedPartStores(defaultStore, map[string]PartStore{"cold": coldStore}, nil)
	require.NoError(t, err)

	store, err := stores.ByName(nil)
	require.NoError(t, err)
	assert.Same(t, defaultStore, store)

	coldName := "cold"
	store, err = stores.ByName(&coldName)
	require.NoError(t, err)
	assert.Same(t, coldStore, store)

	unknownName := "gone"
	_, err = stores.ByName(&unknownName)
	assert.ErrorContains(t, err, "unknown part store")
}

func TestNamedPartStoresRejectsInvalidConfigurations(t *testing.T) {
	testutils.SkipIfIntegration(t)

	defaultStore := &fakePartStore{}

	_, err := NewNamedPartStores(nil, nil, nil)
	assert.ErrorContains(t, err, "default part store")

	_, err = NewNamedPartStores(defaultStore, map[string]PartStore{DefaultPartStoreName: &fakePartStore{}}, nil)
	assert.ErrorContains(t, err, "reserved")

	_, err = NewNamedPartStores(defaultStore, nil, map[string]string{"GLACIER": "missing"})
	assert.ErrorContains(t, err, "unknown part store")
}

func TestNamedPartStoresSupportsTxFreeGetPartOnlyWhenAllStoresDo(t *testing.T) {
	testutils.SkipIfIntegration(t)

	stores, err := NewNamedPartStores(&fakePartStore{txFree: true}, map[string]PartStore{"cold": &fakePartStore{txFree: true}}, nil)
	require.NoError(t, err)
	assert.True(t, stores.SupportsTxFreeGetPart())

	stores, err = NewNamedPartStores(&fakePartStore{txFree: true}, map[string]PartStore{"cold": &fakePartStore{txFree: false}}, nil)
	require.NoError(t, err)
	assert.False(t, stores.SupportsTxFreeGetPart())
}
