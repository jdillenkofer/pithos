package sftp

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"os"
	"path/filepath"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/moby/moby/api/types/container"
	"github.com/pkg/sftp"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/crypto/ssh"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) (*string, error) {
	b := make([]rune, n)
	for i := range b {
		max := big.NewInt(int64(len(letters)))
		randLetterIdx, err := rand.Int(rand.Reader, max)
		if err != nil {
			return nil, err
		}
		b[i] = letters[randLetterIdx.Int64()]
	}
	seq := string(b)
	return &seq, nil
}

func prepareSshServer(t testing.TB, usePassword bool) (string, *ssh.ClientConfig) {
	sshAddr, clientConfig, _ := prepareSshServerWithShaping(t, usePassword, false)
	return sshAddr, clientConfig
}

// networkProfile describes the link between the client and the SSH server.
// Zero values mean no added delay and no bandwidth limit.
type networkProfile struct {
	name     string
	rtt      time.Duration
	rateMbit int
}

// prepareSshServerWithShaping starts an OpenSSH server container. If
// shapeable is set, the container gets NET_ADMIN and tc so its network can
// be shaped with setNetworkProfile.
func prepareSshServerWithShaping(t testing.TB, usePassword bool, shapeable bool) (string, *ssh.ClientConfig, testcontainers.Container) {
	const sshUsername = "user"
	sshPasswordPtr, err := randSeq(16)
	assert.Nil(t, err)
	sshPassword := *sshPasswordPtr
	sshPrivateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	assert.Nil(t, err)
	sshPublicKey, err := ssh.NewPublicKey(&sshPrivateKey.PublicKey)
	assert.Nil(t, err)

	const internalSshPort = "2222/tcp"

	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        testutils.OpenSSHServerImage,
		ExposedPorts: []string{"2222/tcp"},
		Env: map[string]string{
			"PUID":            "1000",
			"PGID":            "1000",
			"TZ":              "Etc/UTC",
			"PASSWORD_ACCESS": strconv.FormatBool(usePassword),
			"PUBLIC_KEY":      string(ssh.MarshalAuthorizedKey(sshPublicKey)),
			"USER_NAME":       sshUsername,
			"USER_PASSWORD":   sshPassword,
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("sshd is listening on port"),
			wait.ForLog("[ls.io-init] done."),
			wait.ForListeningPort(internalSshPort),
		),
	}
	if shapeable {
		req.HostConfigModifier = func(hc *container.HostConfig) {
			hc.CapAdd = append(hc.CapAdd, "NET_ADMIN")
		}
	}
	opensshServerContainer, err := testutils.RunContainer(func() (testcontainers.Container, error) {
		return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
	})
	defer testcontainers.CleanupContainer(t, opensshServerContainer)
	assert.Nil(t, err)

	if shapeable {
		execInContainer(t, ctx, opensshServerContainer, "apk", "add", "--no-cache", "iproute2")
	}

	externalSshPort, err := opensshServerContainer.MappedPort(ctx, internalSshPort)
	assert.Nil(t, err)

	signer, err := ssh.NewSignerFromKey(sshPrivateKey)
	assert.Nil(t, err)

	hostKeyReader, err := opensshServerContainer.CopyFileFromContainer(ctx, "/config/ssh_host_keys/ssh_host_rsa_key.pub")
	assert.Nil(t, err)

	defer hostKeyReader.Close()

	hostKeyBytes, err := io.ReadAll(hostKeyReader)
	assert.Nil(t, err)

	hostPublicKey, _, _, _, err := ssh.ParseAuthorizedKey(hostKeyBytes)
	assert.Nil(t, err)

	var auth ssh.AuthMethod
	if usePassword {
		auth = ssh.Password(sshPassword)
	} else {
		auth = ssh.PublicKeys(signer)
	}

	clientConfig := &ssh.ClientConfig{
		User: sshUsername,
		Auth: []ssh.AuthMethod{
			auth,
		},
		HostKeyCallback: ssh.FixedHostKey(hostPublicKey),
		HostKeyAlgorithms: []string{
			ssh.KeyAlgoRSASHA256,
			ssh.KeyAlgoRSASHA512,
		},
	}

	host := "127.0.0.1"
	externalSshPortStr := externalSshPort.Port()
	sshAddr := fmt.Sprintf("%v:%v", host, externalSshPortStr)

	return sshAddr, clientConfig, opensshServerContainer
}

func execInContainer(t testing.TB, ctx context.Context, c testcontainers.Container, cmd ...string) {
	exitCode, output, err := c.Exec(ctx, cmd, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	if exitCode != 0 {
		out, _ := io.ReadAll(output)
		t.Fatalf("%v failed with exit code %d: %s", cmd, exitCode, out)
	}
}

// setNetworkProfile replaces the shaping of the container's eth0 with
// profile. netem on egress delays every packet the server sends by the full
// round trip time and limits download bandwidth. The kernel of Docker
// Desktop lacks ifb, so upload bandwidth is limited with an ingress policer
// instead, which drops excess packets like a bottleneck link.
func setNetworkProfile(t testing.TB, ctx context.Context, c testcontainers.Container, profile networkProfile) {
	// Deleting fails when no qdisc is configured yet, which is fine.
	_, _, _ = c.Exec(ctx, []string{"tc", "qdisc", "del", "dev", "eth0", "root"})
	_, _, _ = c.Exec(ctx, []string{"tc", "qdisc", "del", "dev", "eth0", "ingress"})

	if profile.rtt > 0 || profile.rateMbit > 0 {
		netem := []string{"tc", "qdisc", "add", "dev", "eth0", "root", "netem"}
		if profile.rtt > 0 {
			netem = append(netem, "delay", fmt.Sprintf("%dms", profile.rtt.Milliseconds()))
		}
		if profile.rateMbit > 0 {
			netem = append(netem, "rate", fmt.Sprintf("%dmbit", profile.rateMbit))
		}
		// Raise the queue limit so netem does not drop packets of
		// bandwidth-delay products beyond the default 1000 packets.
		netem = append(netem, "limit", "100000")
		execInContainer(t, ctx, c, netem...)
	}
	if profile.rateMbit > 0 {
		// A policer has no queue, so a burst below the bandwidth-delay
		// product drops packets of every full window. Allow one round trip
		// (at least 20ms) worth of data, like a bottleneck buffered for one
		// BDP. Docker Desktop uses an MTU of 65535, so the policer must also
		// accept packets of that size or it drops every large segment and
		// stalls TCP.
		burstMs := max(int(profile.rtt.Milliseconds()), 20)
		burstKB := max(profile.rateMbit*burstMs/8, 128)
		execInContainer(t, ctx, c, "tc", "qdisc", "add", "dev", "eth0", "handle", "ffff:", "ingress")
		execInContainer(t, ctx, c, "tc", "filter", "add", "dev", "eth0", "parent", "ffff:", "protocol", "all", "u32", "match", "u32", "0", "0",
			"police", "rate", fmt.Sprintf("%dmbit", profile.rateMbit), "burst", fmt.Sprintf("%dk", burstKB), "mtu", "64k", "drop", "flowid", ":1")
	}
}

func TestSftpPartStore(t *testing.T) {
	testutils.SkipIfIntegration(t)
	testutils.SkipOnWindowsInGitHubActions(t)
	testutils.SkipOnMacOSInGitHubActions(t)

	testcontainers.SkipIfProviderIsNotHealthy(t)

	for _, usePassword := range []bool{false, true} {
		var authType string
		if usePassword {
			authType = " password auth"
		} else {
			authType = " key auth"
		}
		t.Run("it should work with"+authType, func(t *testing.T) {
			sshAddr, clientConfig := prepareSshServer(t, usePassword)

			storagePath, err := os.MkdirTemp("", "pithos-test-data-")
			if err != nil {
				slog.Error(fmt.Sprintf("Could not create temp directory: %s", err))
				os.Exit(1)
			}
			dbPath := filepath.Join(storagePath, "pithos.db")
			db, err := sqlite.OpenDatabase(dbPath)
			if err != nil {
				slog.Error("Couldn't open database")
				os.Exit(1)
			}
			defer func() {
				err = db.Close()
				if err != nil {
					slog.Error(fmt.Sprintf("Could not close database %s", err))
					os.Exit(1)
				}
				err = os.RemoveAll(storagePath)
				if err != nil {
					slog.Error(fmt.Sprintf("Could not remove storagePath %s: %s", storagePath, err))
					os.Exit(1)
				}
			}()

			sftpPartStore, err := New(sshAddr, clientConfig, "/tmp/pithos")
			if err != nil {
				slog.Error(fmt.Sprintf("Could not create SftpPartStore: %s", err))
				os.Exit(1)
			}
			content := []byte("SftpPartStore")
			err = partstore.Tester(sftpPartStore, db, content)
			assert.Nil(t, err)
		})
	}
}

func TestSftpPartStoreTxPutPartPreservesImmutableContent(t *testing.T) {
	testutils.SkipIfIntegration(t)
	testutils.SkipOnWindowsInGitHubActions(t)
	testutils.SkipOnMacOSInGitHubActions(t)

	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := context.Background()
	sshAddr, clientConfig := prepareSshServer(t, false)
	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "pithos.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ps, err := New(sshAddr, clientConfig, "/tmp/pithos")
	if err != nil {
		t.Fatal(err)
	}
	if err := ps.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer ps.Stop(ctx)

	partId, err := partstore.NewRandomPartId()
	if err != nil {
		t.Fatal(err)
	}
	putPartInTx := func(content string, txErr error) error {
		return database.WithTx(ctx, db, nil, func(ctx context.Context, tx database.Tx) error {
			if err := ps.PutPart(ctx, tx, *partId, bytes.NewReader([]byte(content))); err != nil {
				return err
			}
			return txErr
		})
	}
	readPart := func() string {
		rc, err := ps.GetPart(ctx, nil, *partId)
		if err != nil {
			t.Fatal(err)
		}
		defer rc.Close()
		content, err := io.ReadAll(rc)
		if err != nil {
			t.Fatal(err)
		}
		return string(content)
	}
	assertNoLeftoverFiles := func() {
		store := ps.(*sftpPartStore)
		var entries []os.FileInfo
		err := store.withClient(func(client *sftp.Client) error {
			var err error
			entries, err = client.ReadDir(store.root)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			assert.Equal(t, filepath.Base(store.getFilename(*partId)), entry.Name(), "unexpected leftover file")
		}
	}

	errRollback := errors.New("rollback")

	// Rollback leaves a complete, unreferenced part for GC.
	err = putPartInTx("first", errRollback)
	assert.ErrorIs(t, err, errRollback)
	assert.Equal(t, "first", readPart())
	ids, err := ps.GetPartIds(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, []partstore.PartId{*partId}, ids)
	assertNoLeftoverFiles()

	assert.NoError(t, putPartInTx("first", nil))
	assert.Equal(t, "first", readPart())

	// A retry of an existing immutable part uses a temporary file, and
	// rollback must not delete the published replacement.
	err = putPartInTx("first", errRollback)
	assert.ErrorIs(t, err, errRollback)
	assert.Equal(t, "first", readPart())
	assertNoLeftoverFiles()

	// A committed retry also preserves the same logical content.
	assert.NoError(t, putPartInTx("first", nil))
	assert.Equal(t, "first", readPart())
	assertNoLeftoverFiles()
}

func TestSftpPartStoreReconnectUnderLoad(t *testing.T) {
	testutils.SkipIfIntegration(t)
	testutils.SkipOnWindowsInGitHubActions(t)
	testutils.SkipOnMacOSInGitHubActions(t)

	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := context.Background()
	sshAddr, clientConfig := prepareSshServer(t, false)
	// Count SSH handshakes to tell how often the store reconnected.
	var handshakes atomic.Int32
	hostKeyCallback := clientConfig.HostKeyCallback
	clientConfig.HostKeyCallback = func(hostname string, remote net.Addr, key ssh.PublicKey) error {
		handshakes.Add(1)
		return hostKeyCallback(hostname, remote, key)
	}

	ps, err := New(sshAddr, clientConfig, "/tmp/pithos", WithConnectionPoolSize(2))
	if err != nil {
		t.Fatal(err)
	}
	if err := ps.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer ps.Stop(ctx)
	store := ps.(*sftpPartStore)

	content := make([]byte, 256*1024)
	if _, err := rand.Read(content); err != nil {
		t.Fatal(err)
	}
	putAndGet := func() error {
		partId, err := partstore.NewRandomPartId()
		if err != nil {
			return err
		}
		if err := ps.PutPart(ctx, nil, *partId, streamReader{bytes.NewReader(content)}); err != nil {
			return err
		}
		rc, err := ps.GetPart(ctx, nil, *partId)
		if err != nil {
			return err
		}
		defer rc.Close()
		got, err := io.ReadAll(rc)
		if err != nil {
			return err
		}
		if !bytes.Equal(content, got) {
			return errors.New("part content mismatch")
		}
		return ps.DeletePart(ctx, nil, *partId)
	}

	const workers = 8
	const connectionDrops = 5
	stop := make(chan struct{})
	var succeeded atomic.Int32
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Transfers cut off by a dropped connection fail, since only
				// opening a file is retried. Only the recovery matters here.
				if putAndGet() == nil {
					succeeded.Add(1)
				}
			}
		})
	}

	for i := range connectionDrops {
		time.Sleep(200 * time.Millisecond)
		slot := store.slots[i%len(store.slots)]
		conn := slot.conn.Load()
		_ = conn.sshClient.Close()
		// Wait for the workers to replace the connection before dropping
		// the next one.
		assert.Eventually(t, func() bool {
			return slot.conn.Load() != conn
		}, 10*time.Second, 10*time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()

	assert.Positive(t, succeeded.Load())
	// Every worker on a dropped connection sees it fail, but only the first
	// one to notice reconnects.
	assert.Equal(t, int32(len(store.slots)+connectionDrops), handshakes.Load())
	assert.NoError(t, putAndGet())
}
