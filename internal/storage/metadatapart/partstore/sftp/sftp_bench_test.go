package sftp

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"golang.org/x/crypto/ssh"
)

// streamReader hides the Len/Size methods of the wrapped reader, like the
// hashing and encryption readers PutPart receives in production.
type streamReader struct {
	io.Reader
}

var benchNetworkProfiles = []networkProfile{
	{name: "local"},
	{name: "lan", rtt: 1 * time.Millisecond, rateMbit: 1000},
	{name: "wan", rtt: 20 * time.Millisecond, rateMbit: 100},
	{name: "remote", rtt: 80 * time.Millisecond, rateMbit: 50},
}

// BenchmarkSftpPartStore measures the part store against a containerized
// OpenSSH server under several network profiles. Shaped profiles take
// hundreds of milliseconds per operation, so run it with a fixed iteration
// count, e.g. -benchtime=3x.
func BenchmarkSftpPartStore(b *testing.B) {
	testutils.SkipOnWindowsInGitHubActions(b)
	testutils.SkipOnMacOSInGitHubActions(b)

	ctx := context.Background()
	// One container serves all profiles; only the tc shaping changes.
	sshAddr, clientConfig, c := prepareSshServerWithShaping(b, false, true)

	db, err := sqlite.OpenDatabase(filepath.Join(b.TempDir(), "pithos.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })

	newPartId := func() partstore.PartId {
		partId, err := partstore.NewRandomPartId()
		if err != nil {
			b.Fatal(err)
		}
		return *partId
	}

	sizes := []int{4 * 1024, 1024 * 1024, 4 * 1024 * 1024}
	payloads := make(map[int][]byte, len(sizes))
	getPartIds := make(map[int]partstore.PartId, len(sizes))
	// Upload the GetPart fixtures before shaping, so slow profiles do not
	// pay for them.
	fixtureStore := startSftpPartStore(b, sshAddr, clientConfig)
	for _, size := range sizes {
		data := make([]byte, size)
		if _, err := rand.Read(data); err != nil {
			b.Fatal(err)
		}
		payloads[size] = data
		getPartIds[size] = newPartId()
		if err := fixtureStore.PutPart(ctx, nil, getPartIds[size], bytes.NewReader(data)); err != nil {
			b.Fatal(err)
		}
	}
	if err := fixtureStore.Stop(ctx); err != nil {
		b.Fatal(err)
	}

	for _, profile := range benchNetworkProfiles {
		b.Run(profile.name, func(b *testing.B) {
			setNetworkProfile(b, ctx, c, profile)
			// A fresh connection per profile, so TCP state does not carry over.
			ps := startSftpPartStore(b, sshAddr, clientConfig)
			defer ps.Stop(ctx)

			for _, size := range sizes {
				data := payloads[size]

				b.Run(fmt.Sprintf("PutPart/%dKB", size/1024), func(b *testing.B) {
					b.SetBytes(int64(size))
					for b.Loop() {
						err := ps.PutPart(ctx, nil, newPartId(), streamReader{bytes.NewReader(data)})
						if err != nil {
							b.Fatal(err)
						}
					}
				})

				// Transactions add a fixed number of round trips per part,
				// which only matters for small parts.
				if size <= 4*1024 {
					b.Run(fmt.Sprintf("PutPartTx/%dKB", size/1024), func(b *testing.B) {
						b.SetBytes(int64(size))
						for b.Loop() {
							err := database.WithTx(ctx, db, nil, func(ctx context.Context, tx database.Tx) error {
								return ps.PutPart(ctx, tx, newPartId(), streamReader{bytes.NewReader(data)})
							})
							if err != nil {
								b.Fatal(err)
							}
						}
					})
				}

				b.Run(fmt.Sprintf("GetPart/%dKB", size/1024), func(b *testing.B) {
					b.SetBytes(int64(size))
					for b.Loop() {
						rc, err := ps.GetPart(ctx, nil, getPartIds[size])
						if err != nil {
							b.Fatal(err)
						}
						if _, err := io.Copy(io.Discard, rc); err != nil {
							b.Fatal(err)
						}
						if err := rc.Close(); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

// BenchmarkSftpPartStoreConcurrent measures the total throughput of
// concurrent part transfers, which BenchmarkSftpPartStore cannot show since
// it runs one operation at a time. Each iteration transfers
// concurrentBenchParts parts of 1-4MB in parallel.
func BenchmarkSftpPartStoreConcurrent(b *testing.B) {
	testutils.SkipOnWindowsInGitHubActions(b)
	testutils.SkipOnMacOSInGitHubActions(b)

	const concurrentBenchParts = 8

	ctx := context.Background()
	sshAddr, clientConfig, c := prepareSshServerWithShaping(b, false, true)

	payloads := make([][]byte, concurrentBenchParts)
	getPartIds := make([]partstore.PartId, concurrentBenchParts)
	totalBytes := 0
	fixtureStore := startSftpPartStore(b, sshAddr, clientConfig)
	for i := range payloads {
		payloads[i] = make([]byte, (i%4+1)*1024*1024)
		if _, err := rand.Read(payloads[i]); err != nil {
			b.Fatal(err)
		}
		totalBytes += len(payloads[i])
		partId, err := partstore.NewRandomPartId()
		if err != nil {
			b.Fatal(err)
		}
		getPartIds[i] = *partId
		if err := fixtureStore.PutPart(ctx, nil, getPartIds[i], bytes.NewReader(payloads[i])); err != nil {
			b.Fatal(err)
		}
	}
	if err := fixtureStore.Stop(ctx); err != nil {
		b.Fatal(err)
	}

	runConcurrently := func(b *testing.B, op func(i int) error) {
		var wg sync.WaitGroup
		errs := make([]error, concurrentBenchParts)
		for i := range concurrentBenchParts {
			wg.Go(func() {
				errs[i] = op(i)
			})
		}
		wg.Wait()
		if err := errors.Join(errs...); err != nil {
			b.Fatal(err)
		}
	}

	// The rate limits of wan and remote sit below what a single SSH channel
	// window allows at their round trip times. The unlimited variants make
	// the window the bottleneck instead.
	profiles := append(benchNetworkProfiles,
		networkProfile{name: "wan-nolimit", rtt: 20 * time.Millisecond},
		networkProfile{name: "remote-nolimit", rtt: 80 * time.Millisecond},
	)
	for _, profile := range profiles {
		b.Run(profile.name, func(b *testing.B) {
			setNetworkProfile(b, ctx, c, profile)
			ps := startSftpPartStore(b, sshAddr, clientConfig)
			defer ps.Stop(ctx)

			b.Run("PutPart", func(b *testing.B) {
				b.SetBytes(int64(totalBytes))
				for b.Loop() {
					runConcurrently(b, func(i int) error {
						partId, err := partstore.NewRandomPartId()
						if err != nil {
							return err
						}
						return ps.PutPart(ctx, nil, *partId, streamReader{bytes.NewReader(payloads[i])})
					})
				}
			})

			b.Run("GetPart", func(b *testing.B) {
				b.SetBytes(int64(totalBytes))
				for b.Loop() {
					runConcurrently(b, func(i int) error {
						rc, err := ps.GetPart(ctx, nil, getPartIds[i])
						if err != nil {
							return err
						}
						defer rc.Close()
						_, err = io.Copy(io.Discard, rc)
						return err
					})
				}
			})
		})
	}
}

func startSftpPartStore(b *testing.B, sshAddr string, clientConfig *ssh.ClientConfig) partstore.PartStore {
	ps, err := New(sshAddr, clientConfig, "/tmp/pithos")
	if err != nil {
		b.Fatal(err)
	}
	if err := ps.Start(context.Background()); err != nil {
		b.Fatal(err)
	}
	return ps
}
