package metadatapart

import (
	"testing"
	"time"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestReaderLifetimeRetentionMargin(t *testing.T) {
	testutils.SkipIfIntegration(t)
	st, cleanup := newTestStorage(t)
	defer cleanup()
	for _, retention := range []time.Duration{time.Nanosecond, 2 * time.Nanosecond, time.Second, 10 * time.Minute, time.Hour} {
		t.Run(retention.String(), func(t *testing.T) {
			configured, err := NewStorage(st.db, st.metadataStore, st.partStores.Default(), WithGCGraceWindow(retention))
			require.NoError(t, err)
			lifetime := configured.(*metadataPartStorage).readerLifetime
			require.Equal(t, min(5*time.Minute, retention/2), lifetime)
			require.Less(t, lifetime, retention)
			// The remaining margin must cover cross-instance clock skew and wall-clock
			// jumps relative to the monotonic reader deadline; this is operational,
			// not enforced merely by using a database statement timestamp.
			require.GreaterOrEqual(t, retention-lifetime, retention/2)
		})
	}
	for _, invalid := range []time.Duration{0, -time.Second} {
		_, err := NewStorage(st.db, st.metadataStore, st.partStores.Default(), WithGCGraceWindow(invalid))
		require.ErrorContains(t, err, "positive")
	}
}
