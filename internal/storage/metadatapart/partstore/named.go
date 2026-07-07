package partstore

import (
	"context"
	"fmt"
	"maps"
)

// DefaultPartStoreName is the reserved name of the part store configured via
// the single "partStore" field. Part rows record nil instead of this name, so
// rows written before named stores existed stay indistinguishable from rows
// written to the default store afterwards.
const DefaultPartStoreName = "default"

// NamedPartStores is a fixed set of part stores addressed by name, together
// with a storage-class-to-store-name mapping that routes writes. It always
// contains the default store; classes without an explicit mapping fall back
// to it.
type NamedPartStores struct {
	stores      map[string]PartStore
	classToName map[string]string
}

// NewNamedPartStores builds the store set. extraStores must not use the
// reserved default name and every mapping target must reference a configured
// store (the default name is a valid target). Validation of the mapping keys
// as storage classes is up to the caller, which knows the recognized classes.
func NewNamedPartStores(defaultStore PartStore, extraStores map[string]PartStore, classToStoreName map[string]string) (*NamedPartStores, error) {
	if defaultStore == nil {
		return nil, fmt.Errorf("default part store must not be nil")
	}
	stores := map[string]PartStore{
		DefaultPartStoreName: defaultStore,
	}
	for name, store := range extraStores {
		if name == DefaultPartStoreName {
			return nil, fmt.Errorf("part store name %q is reserved for the default part store", DefaultPartStoreName)
		}
		if store == nil {
			return nil, fmt.Errorf("part store %q must not be nil", name)
		}
		stores[name] = store
	}
	for storageClass, name := range classToStoreName {
		if _, ok := stores[name]; !ok {
			return nil, fmt.Errorf("storage class %q maps to unknown part store %q", storageClass, name)
		}
	}
	return &NamedPartStores{
		stores:      stores,
		classToName: maps.Clone(classToStoreName),
	}, nil
}

// StoreForClass resolves the part store that data of the given (effective)
// storage class is written to. The returned name is the value to record on
// part rows: nil for the default store.
func (n *NamedPartStores) StoreForClass(storageClass string) (*string, PartStore) {
	if name, ok := n.classToName[storageClass]; ok && name != DefaultPartStoreName {
		return &name, n.stores[name]
	}
	return nil, n.stores[DefaultPartStoreName]
}

// ByName resolves a store by the name recorded on a part row; nil means the
// default store. An unknown name fails loudly instead of guessing, because
// reading or deleting a part through the wrong store would corrupt state.
func (n *NamedPartStores) ByName(name *string) (PartStore, error) {
	if name == nil {
		return n.stores[DefaultPartStoreName], nil
	}
	store, ok := n.stores[*name]
	if !ok {
		return nil, fmt.Errorf("part references unknown part store %q (was it removed from the storage configuration?)", *name)
	}
	return store, nil
}

// Default returns the default part store.
func (n *NamedPartStores) Default() PartStore {
	return n.stores[DefaultPartStoreName]
}

// All returns every configured store keyed by name, including the default.
func (n *NamedPartStores) All() map[string]PartStore {
	return maps.Clone(n.stores)
}

// SupportsTxFreeGetPart reports whether every configured store allows GetPart
// with a nil transaction. A single tx-bound store (e.g. the SQL part store)
// disables tx-free streaming for the whole set, because an object's parts may
// live in any of the stores.
func (n *NamedPartStores) SupportsTxFreeGetPart() bool {
	for _, store := range n.stores {
		if !SupportsTxFreeGetPart(store) {
			return false
		}
	}
	return true
}

// Start starts every configured store.
func (n *NamedPartStores) Start(ctx context.Context) error {
	for name, store := range n.stores {
		if err := store.Start(ctx); err != nil {
			return fmt.Errorf("starting part store %q: %w", name, err)
		}
	}
	return nil
}

// Stop stops every configured store.
func (n *NamedPartStores) Stop(ctx context.Context) error {
	for name, store := range n.stores {
		if err := store.Stop(ctx); err != nil {
			return fmt.Errorf("stopping part store %q: %w", name, err)
		}
	}
	return nil
}
