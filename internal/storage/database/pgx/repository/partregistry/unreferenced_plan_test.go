package partregistry

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	databasebackend "github.com/jdillenkofer/pithos/internal/storage/database/pgx"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partregistry"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestFindUnreferencedUsesPartialIndexWithGenericPlan(t *testing.T) {
	testutils.SkipIfIntegration(t)
	testutils.SkipOnWindowsInGitHubActions(t)
	testutils.SkipOnMacOSInGitHubActions(t)
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	container, err := postgres.Run(ctx, testutils.PostgresImage, postgres.WithUsername("postgres"), postgres.WithPassword("postgres"), postgres.WithDatabase("postgres"), postgres.BasicWaitStrategies())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Terminate(ctx)) })
	url, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	db, err := databasebackend.OpenDatabase(url, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	controller, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = controller.Rollback(context.Background()) })
	tx := controller.SqlTx()
	repo, err := NewRepository()
	require.NoError(t, err)

	const live, unreferenced = 20000, 50
	refs := make([]partregistry.Ref, 0, live+unreferenced)
	for i := range live + unreferenced {
		id, err := partstore.NewRandomPartId()
		require.NoError(t, err)
		delta := int64(1)
		if i < unreferenced {
			delta = 0
		}
		refs = append(refs, partregistry.Ref{PartId: *id, Delta: delta})
	}
	require.NoError(t, repo.RegisterParts(t.Context(), tx, refs))
	_, err = tx.ExecContext(t.Context(), "ANALYZE part_registry")
	require.NoError(t, err)

	// pgx caches prepared statements, so PostgreSQL may switch to a generic plan.
	_, err = tx.ExecContext(t.Context(), "SET LOCAL plan_cache_mode = force_generic_plan")
	require.NoError(t, err)
	_, err = tx.ExecContext(t.Context(), "PREPARE find_unreferenced(text, timestamp, int) AS "+findUnreferencedStmt)
	require.NoError(t, err)
	var encoded []byte
	require.NoError(t, tx.QueryRowContext(t.Context(), "EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE find_unreferenced('', now() AT TIME ZONE 'UTC', 256)").Scan(&encoded))
	t.Logf("plan=%s", encoded)
	var plans []struct{ Plan map[string]any }
	require.NoError(t, json.Unmarshal(encoded, &plans))
	indexFound := false
	var inspect func(map[string]any)
	inspect = func(node map[string]any) {
		if node["Node Type"] == "Index Scan" {
			indexFound = true
			require.Equal(t, "part_registry_unreferenced_idx", node["Index Name"])
		}
		if removed, ok := node["Rows Removed by Filter"].(float64); ok {
			require.Zero(t, removed, "the sweep must not read live rows")
		}
		if children, ok := node["Plans"].([]any); ok {
			for _, child := range children {
				inspect(child.(map[string]any))
			}
		}
	}
	inspect(plans[0].Plan)
	require.True(t, indexFound)

	found, err := repo.FindUnreferenced(t.Context(), tx, time.Now(), nil, 256)
	require.NoError(t, err)
	require.Len(t, found, unreferenced)
}
