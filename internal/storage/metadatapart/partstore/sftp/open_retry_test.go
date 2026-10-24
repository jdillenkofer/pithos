package sftp

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/pkg/sftp"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"golang.org/x/crypto/ssh"
)

// lostOpenResponseReader forwards complete SFTP packets until the server
// acknowledges an open with SSH_FXP_HANDLE (102). Dropping that response models
// a connection loss after file creation but before the client learns its handle.
type lostOpenResponseReader struct {
	reader  io.Reader
	pending *bytes.Reader
	dropped bool
}

func (r *lostOpenResponseReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.dropped {
		return 0, io.EOF
	}
	if r.pending == nil || r.pending.Len() == 0 {
		var header [4]byte
		if _, err := io.ReadFull(r.reader, header[:]); err != nil {
			return 0, err
		}
		payload := make([]byte, binary.BigEndian.Uint32(header[:]))
		if _, err := io.ReadFull(r.reader, payload); err != nil {
			return 0, err
		}
		if len(payload) > 0 && payload[0] == 102 {
			r.dropped = true
			return 0, io.EOF
		}
		r.pending = bytes.NewReader(append(header[:], payload...))
	}
	return r.pending.Read(p)
}

func TestSftpPartStoreRetriesLostTemporaryFileOpenResponse(t *testing.T) {
	testutils.SkipIfIntegration(t)
	testutils.SkipOnWindowsInGitHubActions(t)
	testutils.SkipOnMacOSInGitHubActions(t)
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := context.Background()
	addr, config := prepareSshServer(t, false)
	var handshakes atomic.Int32
	hostKeyCallback := config.HostKeyCallback
	config.HostKeyCallback = func(hostname string, remote net.Addr, key ssh.PublicKey) error {
		handshakes.Add(1)
		return hostKeyCallback(hostname, remote, key)
	}
	ps, err := New(addr, config, "/tmp/pithos")
	require.NoError(t, err)
	require.NoError(t, ps.Start(ctx))
	t.Cleanup(func() { _ = ps.Stop(ctx) })

	store := ps.(*sftpPartStore)
	conn := store.slots[0].conn.Load()
	require.NoError(t, conn.client.Close())
	session, err := conn.sshClient.NewSession()
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	stdout, err := session.StdoutPipe()
	require.NoError(t, err)
	stdin, err := session.StdinPipe()
	require.NoError(t, err)
	require.NoError(t, session.RequestSubsystem("sftp"))
	responses := &lostOpenResponseReader{reader: stdout}
	conn.client, err = sftp.NewClientPipe(responses, stdin)
	require.NoError(t, err)

	id, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	content := []byte("immutable part content")
	// The first open creates the private temporary file, but its response is
	// lost. Retrying must reopen that same file without extra reconnects.
	require.NoError(t, ps.PutPart(ctx, nil, *id, bytes.NewReader(content)))
	require.Equal(t, int32(2), handshakes.Load())

	rc, err := ps.GetPart(ctx, nil, *id)
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, content, got)
	require.NoError(t, store.withClient(func(client *sftp.Client) error {
		entries, err := client.ReadDir(store.root)
		if err != nil {
			return err
		}
		require.Len(t, entries, 1)
		require.Equal(t, filepath.Base(store.getFilename(*id)), entries[0].Name())
		return nil
	}))
}
