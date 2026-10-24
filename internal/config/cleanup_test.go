package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestFileStringProviderCleansAbandonedWrites(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, current := range []bool{true, false} {
		name := "missing current file"
		if current {
			name = "keep current file"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			target := filepath.Join(root, "token")
			if current {
				require.NoError(t, os.WriteFile(target, []byte("old"), 0o600))
			}
			staged := filepath.Join(root, ".token.tmp-123456")
			require.NoError(t, os.WriteFile(staged, []byte("new"), 0o600))
			unrelated := filepath.Join(root, ".token.tmp-not-ours")
			require.NoError(t, os.WriteFile(unrelated, []byte("keep"), 0o600))
			directory := filepath.Join(root, ".token.tmp-654321")
			require.NoError(t, os.Mkdir(directory, 0o700))
			config, err := json.Marshal(map[string]string{"type": "File", "path": target})
			require.NoError(t, err)
			var provider StringProvider
			err = json.Unmarshal(config, &provider)
			if current {
				require.NoError(t, err)
				require.Equal(t, "old", provider.Value())
				persisted, err := os.ReadFile(target)
				require.NoError(t, err)
				require.Equal(t, "old", string(persisted))
				require.NoFileExists(t, staged)
				// Cleanup is safe to repeat on the next startup.
				require.NoError(t, json.Unmarshal(config, &provider))
			} else {
				require.ErrorIs(t, err, os.ErrNotExist)
				require.FileExists(t, staged, "failed loading must preserve temporary files")
				require.NoFileExists(t, target)
			}
			require.FileExists(t, unrelated)
			require.DirExists(t, directory)
		})
	}
}
