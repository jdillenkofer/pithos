package partstore

import (
	"testing"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
)

func TestPlacementHintsValidate(t *testing.T) {
	testutils.SkipIfIntegration(t)

	objectID := DeriveObjectId("bucket", "key", "")
	partNumber := uint64(1)
	partTwo := uint64(2)
	partCount := uint64(1)
	zero := uint64(0)
	objectSize := uint64(100)

	cases := []struct {
		name    string
		hints   PlacementHints
		wantErr bool
	}{
		{name: "empty", hints: PlacementHints{}},
		{name: "object only", hints: PlacementHints{ObjectID: &objectID}},
		{name: "object and part number", hints: PlacementHints{ObjectID: &objectID, PartNumber: &partNumber}},
		{name: "part number without object", hints: PlacementHints{PartNumber: &partNumber}, wantErr: true},
		{name: "part count without object", hints: PlacementHints{PartCount: &partCount}, wantErr: true},
		{name: "object size without object", hints: PlacementHints{ObjectSize: &objectSize}, wantErr: true},
		{name: "zero part number", hints: PlacementHints{ObjectID: &objectID, PartNumber: &zero}, wantErr: true},
		{name: "zero part count", hints: PlacementHints{ObjectID: &objectID, PartCount: &zero}, wantErr: true},
		{name: "part exceeds count", hints: PlacementHints{ObjectID: &objectID, PartNumber: &partTwo, PartCount: &partCount}, wantErr: true},
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
	testutils.SkipIfIntegration(t)

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
