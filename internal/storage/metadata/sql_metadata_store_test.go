package metadata

import (
	"log"
	"os"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/stretchr/testify/assert"
)

func TestSqlMetadataStore(t *testing.T) {
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		log.Fatalf("Could not create temp directory: %s", err)
	}
	db, err := database.OpenDatabase(storagePath)
	if err != nil {
		log.Fatal("Couldn't open database")
	}
	defer func() {
		err = db.Close()
		if err != nil {
			log.Fatalf("Could not close database %s", err)
		}
		err = os.RemoveAll(storagePath)
		if err != nil {
			log.Fatalf("Could not remove storagePath %s: %s", storagePath, err)
		}
	}()
	sqlMetadataStore, err := NewSqlMetadataStore()
	if err != nil {
		log.Fatalf("Could not create SqlMetadataStore: %s", err)
	}
	err = MetadataStoreTester(sqlMetadataStore, db)
	assert.Nil(t, err)
}
