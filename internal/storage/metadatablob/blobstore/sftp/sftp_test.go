package sftp

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"strconv"
	"testing"

	"os"
	"path/filepath"

	"github.com/docker/go-connections/nat"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
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

func prepareSshServer(t *testing.T, usePassword bool) (string, *ssh.ClientConfig) {
	const sshUsername = "user"
	sshPasswordPtr, err := randSeq(16)
	assert.Nil(t, err)
	sshPassword := *sshPasswordPtr
	sshPrivateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	assert.Nil(t, err)
	sshPublicKey, err := ssh.NewPublicKey(&sshPrivateKey.PublicKey)
	assert.Nil(t, err)

	internalSshPort, err := nat.NewPort("tcp", "2222")
	assert.Nil(t, err)

	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "lscr.io/linuxserver/openssh-server:latest",
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
			wait.ForListeningPort(internalSshPort),
		),
	}
	opensshServerContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	defer testcontainers.CleanupContainer(t, opensshServerContainer)
	assert.Nil(t, err)

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

	return sshAddr, clientConfig
}

func TestSftpBlobStore(t *testing.T) {
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

			tmpPath := filepath.Join(os.TempDir(), "pithos")
			sftpBlobStore, err := New(sshAddr, clientConfig, tmpPath)
			if err != nil {
				slog.Error(fmt.Sprintf("Could not create SftpBlobStore: %s", err))
				os.Exit(1)
			}
			content := []byte("SftpBlobStore")
			err = blobstore.Tester(sftpBlobStore, db, content)
			assert.Nil(t, err)
		})
	}
}
