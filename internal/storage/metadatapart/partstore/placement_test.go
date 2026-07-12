package partstore

import "testing"

func TestPlacementHintsValidate(t *testing.T) {
	objectID := DeriveObjectId("bucket", "key", "")
	partNumber := uint64(1)

	cases := []struct {
		name    string
		hints   PlacementHints
		wantErr bool
	}{
		{name: "empty", hints: PlacementHints{}},
		{name: "object only", hints: PlacementHints{ObjectID: &objectID}},
		{name: "object and part number", hints: PlacementHints{ObjectID: &objectID, PartNumber: &partNumber}},
		{name: "part number without object", hints: PlacementHints{PartNumber: &partNumber}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.hints.Validate()
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestDeriveObjectIdIsStableAndDistinct(t *testing.T) {
	a := DeriveObjectId("bucket", "key", "")
	if a != DeriveObjectId("bucket", "key", "") {
		t.Fatal("DeriveObjectId is not deterministic")
	}
	if a == DeriveObjectId("bucket", "key", "upload-1") {
		t.Fatal("discriminator did not change the object id")
	}
	if a == DeriveObjectId("bucket", "key2", "") {
		t.Fatal("different key produced the same object id")
	}
	// Field boundaries must not collide: ("a","bc") vs ("ab","c").
	if DeriveObjectId("a", "bc", "") == DeriveObjectId("ab", "c", "") {
		t.Fatal("field separator failed to prevent a collision")
	}
}
