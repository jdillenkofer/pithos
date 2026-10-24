package sftp

import (
	"errors"
	"os"
	"testing"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

type recordingPartPublisher struct {
	atomic    bool
	removeErr error
	renameErr error
	calls     []string
}

func (p *recordingPartPublisher) HasExtension(name string) (string, bool) {
	return "1", p.atomic && name == "posix-rename@openssh.com"
}

func (p *recordingPartPublisher) PosixRename(old, new string) error {
	p.calls = append(p.calls, "posix-rename "+old+" "+new)
	return p.renameErr
}

func (p *recordingPartPublisher) Rename(old, new string) error {
	p.calls = append(p.calls, "rename "+old+" "+new)
	return p.renameErr
}

func (p *recordingPartPublisher) Remove(name string) error {
	p.calls = append(p.calls, "remove "+name)
	return p.removeErr
}

func TestPublishTemporaryPart(t *testing.T) {
	testutils.SkipIfIntegration(t)
	failed := errors.New("operation failed")
	for _, tc := range []struct {
		name      string
		atomic    bool
		removeErr error
		renameErr error
		wantErr   error
		calls     []string
	}{
		{
			name: "prefer atomic replacement", atomic: true,
			calls: []string{"posix-rename temporary final"},
		},
		{
			name: "do not fall back after atomic rename failure", atomic: true,
			renameErr: failed, wantErr: failed,
			calls: []string{"posix-rename temporary final"},
		},
		{
			name:  "remove destination before standard rename",
			calls: []string{"remove final", "rename temporary final"},
		},
		{
			name: "missing destination is safe", removeErr: os.ErrNotExist,
			calls: []string{"remove final", "rename temporary final"},
		},
		{
			name: "stop on deletion error", removeErr: failed, wantErr: failed,
			calls: []string{"remove final"},
		},
		{
			name: "propagate standard rename failure", renameErr: failed, wantErr: failed,
			calls: []string{"remove final", "rename temporary final"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &recordingPartPublisher{atomic: tc.atomic, removeErr: tc.removeErr, renameErr: tc.renameErr}
			err := publishTemporaryPart(client, "temporary", "final")
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.calls, client.calls)
		})
	}
}
