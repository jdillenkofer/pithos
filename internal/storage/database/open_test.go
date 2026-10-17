package database_test

import (
	"reflect"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	_ "github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
)

func TestOpenSQLInstrumentsOnlyWhenQueryTracingIsEnabled(t *testing.T) {
	testutils.SkipIfIntegration(t)
	t.Cleanup(func() { database.SetQueryTracingEnabled(false) })

	database.SetQueryTracingEnabled(false)
	plain, err := database.OpenSQL("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()

	database.SetQueryTracingEnabled(true)
	traced, err := database.OpenSQL("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer traced.Close()

	if reflect.TypeOf(plain.Driver()) == reflect.TypeOf(traced.Driver()) {
		t.Fatalf("expected traced handle to use an instrumented driver, both use %T", plain.Driver())
	}
}
