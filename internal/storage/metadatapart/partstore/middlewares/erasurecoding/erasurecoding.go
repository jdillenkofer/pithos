package erasurecoding

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/task"
	"github.com/klauspost/reedsolomon"
)

const shardMagic = "PEC1"
const shardHeaderVersion = uint8(1)
const shardHeaderSize = 4 + 1 + 2 + 2 + 2 + 4
const frameHeaderSize = 8 + 4 + 4 + 32
const DefaultHealScanInterval = 7 * 24 * time.Hour

type erasureCodingPartStore struct {
	*lifecycle.ValidatedLifecycle
	partStores    []partstore.PartStore
	partLocker    partLocker
	dataShards    int
	parityShards  int
	totalShards   int
	stripeShardSz int
	healScanEvery time.Duration
	healTask      *task.TaskHandle
	// enc is built once and reused; reedsolomon encoders depend only on the
	// (fixed) shard counts and are safe for concurrent use across streams.
	enc reedsolomon.Encoder
}

type Option func(*erasureCodingPartStore) error

func WithHealScanInterval(interval time.Duration) Option {
	return func(e *erasureCodingPartStore) error {
		if interval < 0 {
			return errors.New("heal scan interval must be >= 0")
		}
		e.healScanEvery = interval
		return nil
	}
}

var _ partstore.PartStore = (*erasureCodingPartStore)(nil)

func NewWithPartStores(dataShards int, parityShards int, stripeShardSize int, partStores []partstore.PartStore, opts ...Option) (partstore.PartStore, error) {
	if dataShards < 1 {
		return nil, errors.New("dataShards must be >= 1")
	}
	if parityShards < 1 {
		return nil, errors.New("parityShards must be >= 1")
	}
	totalShards := dataShards + parityShards
	if totalShards > 256 {
		return nil, errors.New("dataShards + parityShards must be <= 256")
	}
	if stripeShardSize < 1024 {
		return nil, errors.New("streamBlockSize must be >= 1024")
	}
	if len(partStores) != totalShards {
		return nil, errors.New("partStores length must equal dataShards + parityShards")
	}
	v, err := lifecycle.NewValidatedLifecycle("erasureCodingPartStore")
	if err != nil {
		return nil, err
	}
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, err
	}
	store := &erasureCodingPartStore{
		ValidatedLifecycle: v,
		partStores:         partStores,
		partLocker:         newPartLocker(),
		dataShards:         dataShards,
		parityShards:       parityShards,
		totalShards:        totalShards,
		stripeShardSz:      stripeShardSize,
		healScanEvery:      DefaultHealScanInterval,
		enc:                enc,
	}
	for _, opt := range opts {
		if err := opt(store); err != nil {
			return nil, err
		}
	}
	return store, nil
}

func (e *erasureCodingPartStore) Start(ctx context.Context) error {
	if err := e.ValidatedLifecycle.Start(ctx); err != nil {
		return err
	}
	for _, store := range e.partStores {
		if err := store.Start(ctx); err != nil {
			return err
		}
	}
	if e.healScanEvery > 0 {
		e.healTask = task.Start(func(cancelTask *atomic.Bool) {
			e.healScanLoop(cancelTask)
		})
	}
	return nil
}

func (e *erasureCodingPartStore) Stop(ctx context.Context) error {
	if err := e.ValidatedLifecycle.Stop(ctx); err != nil {
		return err
	}
	if e.healTask != nil {
		e.healTask.Cancel()
		e.healTask.Join()
		e.healTask = nil
	}
	for _, store := range e.partStores {
		if err := store.Stop(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (e *erasureCodingPartStore) healScanLoop(cancelTask *atomic.Bool) {
	ctx := context.Background()
	e.healScanOnce(ctx, cancelTask)
	ticker := time.NewTicker(e.healScanEvery)
	defer ticker.Stop()
	for {
		if cancelTask.Load() {
			return
		}
		select {
		case <-ticker.C:
			e.healScanOnce(ctx, cancelTask)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (e *erasureCodingPartStore) healScanOnce(ctx context.Context, cancelTask *atomic.Bool) {
	partIds, err := e.GetPartIds(ctx, nil)
	if err != nil {
		slog.Warn("erasurecoding heal scan failed to list part ids", "err", err)
		return
	}
	for _, partId := range partIds {
		if cancelTask.Load() {
			return
		}
		rc, err := e.GetPart(ctx, nil, partId)
		if err != nil {
			slog.Warn("erasurecoding heal scan failed to open part", "partId", partId.String(), "err", err)
			continue
		}
		_, err = io.Copy(io.Discard, rc)
		_ = rc.Close()
		if err != nil {
			slog.Warn("erasurecoding heal scan failed to read part", "partId", partId.String(), "err", err)
		}
	}
}

func (e *erasureCodingPartStore) shardHeader(shardIndex int) []byte {
	b := make([]byte, shardHeaderSize)
	copy(b[0:4], []byte(shardMagic))
	b[4] = shardHeaderVersion
	binary.BigEndian.PutUint16(b[5:7], uint16(e.dataShards))
	binary.BigEndian.PutUint16(b[7:9], uint16(e.totalShards))
	binary.BigEndian.PutUint16(b[9:11], uint16(shardIndex))
	binary.BigEndian.PutUint32(b[11:15], uint32(e.stripeShardSz))
	return b
}

func parseShardHeader(b []byte) (int, int, int, int, error) {
	if len(b) != shardHeaderSize {
		return 0, 0, 0, 0, errors.New("invalid shard header")
	}
	if string(b[0:4]) != shardMagic || b[4] != shardHeaderVersion {
		return 0, 0, 0, 0, errors.New("invalid shard magic/version")
	}
	data := int(binary.BigEndian.Uint16(b[5:7]))
	total := int(binary.BigEndian.Uint16(b[7:9]))
	idx := int(binary.BigEndian.Uint16(b[9:11]))
	stripe := int(binary.BigEndian.Uint32(b[11:15]))
	if data < 1 || total < data || idx < 0 || idx >= total || stripe < 1024 {
		return 0, 0, 0, 0, errors.New("invalid shard header fields")
	}
	return data, total, idx, stripe, nil
}

func encodeFrameHeader(stripeIndex uint64, dataBytes int, payload []byte) []byte {
	b := make([]byte, frameHeaderSize)
	binary.BigEndian.PutUint64(b[0:8], stripeIndex)
	binary.BigEndian.PutUint32(b[8:12], uint32(dataBytes))
	binary.BigEndian.PutUint32(b[12:16], uint32(len(payload)))
	h := sha256.Sum256(payload)
	copy(b[16:48], h[:])
	return b
}

func parseFrameHeader(b []byte) (uint64, int, int, []byte, error) {
	if len(b) != frameHeaderSize {
		return 0, 0, 0, nil, errors.New("invalid frame header")
	}
	idx := binary.BigEndian.Uint64(b[0:8])
	dataBytes := int(binary.BigEndian.Uint32(b[8:12]))
	payloadLen := int(binary.BigEndian.Uint32(b[12:16]))
	h := make([]byte, 32)
	copy(h, b[16:48])
	if dataBytes < 1 || payloadLen < 1 {
		return 0, 0, 0, nil, errors.New("invalid frame header fields")
	}
	return idx, dataBytes, payloadLen, h, nil
}

func (e *erasureCodingPartStore) PutPart(ctx context.Context, tx database.Tx, partId partstore.PartId, reader io.Reader) error {
	unlock := e.partLocker.Lock(partId)
	defer unlock()

	enc := e.enc

	pipeReaders := make([]*io.PipeReader, e.totalShards)
	pipeWriters := make([]*io.PipeWriter, e.totalShards)
	errCh := make(chan error, e.totalShards)
	for i := 0; i < e.totalShards; i++ {
		pr, pw := io.Pipe()
		pipeReaders[i], pipeWriters[i] = pr, pw
		go func(idx int) {
			errCh <- e.partStores[idx].PutPart(ctx, tx, partId, pr)
		}(i)
		if _, err := pipeWriters[i].Write(e.shardHeader(i)); err != nil {
			return err
		}
	}

	stripeDataCap := e.dataShards * e.stripeShardSz
	stripeBuf := make([]byte, stripeDataCap)
	stripeIndex := uint64(0)
	for {
		n, readErr := io.ReadFull(reader, stripeBuf)
		if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			for _, pw := range pipeWriters {
				_ = pw.CloseWithError(readErr)
			}
			return readErr
		}
		if n == 0 {
			break
		}

		shardLen := (n + e.dataShards - 1) / e.dataShards
		shards := make([][]byte, e.totalShards)
		for i := 0; i < e.totalShards; i++ {
			shards[i] = make([]byte, shardLen)
		}
		for i := 0; i < e.dataShards; i++ {
			start := i * shardLen
			end := start + shardLen
			if start > n {
				start = n
			}
			if end > n {
				end = n
			}
			copy(shards[i], stripeBuf[start:end])
		}
		if err := enc.Encode(shards); err != nil {
			for _, pw := range pipeWriters {
				_ = pw.CloseWithError(err)
			}
			return err
		}

		for i := 0; i < e.totalShards; i++ {
			fh := encodeFrameHeader(stripeIndex, n, shards[i])
			if _, err := pipeWriters[i].Write(fh); err != nil {
				return err
			}
			if _, err := pipeWriters[i].Write(shards[i]); err != nil {
				return err
			}
		}
		stripeIndex++
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
	}

	for _, pw := range pipeWriters {
		_ = pw.Close()
	}
	for i := 0; i < e.totalShards; i++ {
		if err := <-errCh; err != nil {
			return err
		}
	}
	return nil
}

// SupportsTxFreeGetPart reports true only when every shard store supports
// tx-free reads (healing may also write, which the shard stores handle via
// their non-transactional paths when tx is nil).
func (e *erasureCodingPartStore) SupportsTxFreeGetPart() bool {
	for _, ps := range e.partStores {
		if !partstore.SupportsTxFreeGetPart(ps) {
			return false
		}
	}
	return true
}

func (e *erasureCodingPartStore) GetPart(ctx context.Context, tx database.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	unlock := e.partLocker.RLock(partId)
	readers, healShards, err := e.openPartReaders(ctx, tx, partId)
	if err != nil {
		unlock()
		return nil, err
	}
	if hasHealShards(healShards) {
		closePartReaders(readers)
		unlock()
		return e.getPartWithHealing(ctx, tx, partId)
	}

	// Healthy reads hold only a shared lock, so concurrent readers of the same
	// part can proceed while writes/deletes/repairs remain serialized.
	return e.newPartReader(ctx, tx, partId, readers, healShards, false, unlock), nil
}

func (e *erasureCodingPartStore) getPartWithHealing(ctx context.Context, tx database.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	unlock := e.partLocker.Lock(partId)
	readers, healShards, err := e.openPartReaders(ctx, tx, partId)
	if err != nil {
		unlock()
		return nil, err
	}

	return e.newPartReader(ctx, tx, partId, readers, healShards, true, unlock), nil
}

func (e *erasureCodingPartStore) openPartReaders(ctx context.Context, tx database.Tx, partId partstore.PartId) ([]io.ReadCloser, []bool, error) {
	readers := make([]io.ReadCloser, e.totalShards)
	healShards := make([]bool, e.totalShards)
	for i := 0; i < e.totalShards; i++ {
		rc, err := e.partStores[i].GetPart(ctx, tx, partId)
		if err != nil {
			if errors.Is(err, partstore.ErrPartNotFound) {
				readers[i] = nil
				healShards[i] = true
				continue
			}
			closePartReaders(readers)
			return nil, nil, err
		}
		if rc == nil {
			healShards[i] = true
			continue
		}
		head := make([]byte, shardHeaderSize)
		_, err = io.ReadFull(rc, head)
		if err != nil {
			_ = rc.Close()
			readers[i] = nil
			healShards[i] = true
			continue
		}
		d, t, idx, stripe, err := parseShardHeader(head)
		if err != nil || d != e.dataShards || t != e.totalShards || idx != i || stripe != e.stripeShardSz {
			_ = rc.Close()
			readers[i] = nil
			healShards[i] = true
			continue
		}
		readers[i] = rc
	}
	return readers, healShards, nil
}

func hasHealShards(healShards []bool) bool {
	for _, heal := range healShards {
		if heal {
			return true
		}
	}
	return false
}

func closePartReaders(readers []io.ReadCloser) {
	for _, rc := range readers {
		if rc != nil {
			_ = rc.Close()
		}
	}
}

func (e *erasureCodingPartStore) newPartReader(ctx context.Context, tx database.Tx, partId partstore.PartId, readers []io.ReadCloser, healShards []bool, healMissing bool, unlock func()) io.ReadCloser {
	pr, pw := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		closeReaderAt := func(i int) {
			if readers[i] != nil {
				_ = readers[i].Close()
				readers[i] = nil
			}
		}
		defer func() {
			for _, rc := range readers {
				if rc != nil {
					_ = rc.Close()
				}
			}
		}()

		healPipeWriters := make([]*io.PipeWriter, e.totalShards)
		healErrCh := make(chan error, e.totalShards)
		healingShardCount := 0
		if healMissing {
			for i := 0; i < e.totalShards; i++ {
				if !healShards[i] {
					continue
				}
				prHeal, pwHeal := io.Pipe()
				healPipeWriters[i] = pwHeal
				healingShardCount++
				go func(idx int, reader *io.PipeReader) {
					healErrCh <- e.partStores[idx].PutPart(ctx, tx, partId, reader)
				}(i, prHeal)
				if _, err := pwHeal.Write(e.shardHeader(i)); err != nil {
					_ = pwHeal.CloseWithError(err)
					_ = pw.CloseWithError(err)
					return
				}
			}
		}

		closeHealingWriters := func(closeErr error) {
			for i := 0; i < e.totalShards; i++ {
				if healPipeWriters[i] == nil {
					continue
				}
				if closeErr != nil {
					_ = healPipeWriters[i].CloseWithError(closeErr)
					continue
				}
				_ = healPipeWriters[i].Close()
			}
			for i := 0; i < healingShardCount; i++ {
				_ = <-healErrCh
			}
		}

		enc := e.enc
		for stripeIndex := uint64(0); ; stripeIndex++ {
			shards := make([][]byte, e.totalShards)
			available := 0
			dataBytes := 0
			seenAny := false
			for i := 0; i < e.totalShards; i++ {
				if readers[i] == nil {
					continue
				}
				fh := make([]byte, frameHeaderSize)
				_, err := io.ReadFull(readers[i], fh)
				if err != nil {
					if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
						closeReaderAt(i)
						healShards[i] = true
						continue
					}
					closeHealingWriters(err)
					_ = pw.CloseWithError(err)
					return
				}
				seenAny = true
				fIdx, fDataBytes, payloadLen, expectedHash, err := parseFrameHeader(fh)
				if err != nil || fIdx != stripeIndex {
					closeReaderAt(i)
					healShards[i] = true
					continue
				}
				payload := make([]byte, payloadLen)
				_, err = io.ReadFull(readers[i], payload)
				if err != nil {
					closeReaderAt(i)
					healShards[i] = true
					continue
				}
				h := sha256.Sum256(payload)
				if !bytes.Equal(expectedHash, h[:]) {
					closeReaderAt(i)
					healShards[i] = true
					continue
				}
				if dataBytes == 0 {
					dataBytes = fDataBytes
				}
				shards[i] = payload
				available++
			}
			if !seenAny {
				closeHealingWriters(nil)
				_ = pw.Close()
				return
			}
			if available < e.dataShards {
				err := fmt.Errorf("insufficient shards in stripe %d", stripeIndex)
				closeHealingWriters(err)
				_ = pw.CloseWithError(fmt.Errorf("insufficient shards in stripe %d", stripeIndex))
				return
			}
			if err := enc.ReconstructData(shards); err != nil {
				closeHealingWriters(err)
				_ = pw.CloseWithError(err)
				return
			}
			for i := 0; i < e.totalShards; i++ {
				if !healShards[i] || healPipeWriters[i] == nil {
					continue
				}
				fh := encodeFrameHeader(stripeIndex, dataBytes, shards[i])
				if _, err := healPipeWriters[i].Write(fh); err != nil {
					closeHealingWriters(err)
					_ = pw.CloseWithError(err)
					return
				}
				if _, err := healPipeWriters[i].Write(shards[i]); err != nil {
					closeHealingWriters(err)
					_ = pw.CloseWithError(err)
					return
				}
			}
			buf := bytes.NewBuffer(nil)
			for i := 0; i < e.dataShards; i++ {
				buf.Write(shards[i])
			}
			out := buf.Bytes()
			if dataBytes < len(out) {
				out = out[:dataBytes]
			}
			if _, err := pw.Write(out); err != nil {
				closeHealingWriters(err)
				_ = pw.CloseWithError(err)
				return
			}
		}
	}()

	return &readCloserWithWait{ReadCloser: pr, done: done, unlock: unlock}
}

type readCloserWithWait struct {
	io.ReadCloser
	done   chan struct{}
	once   sync.Once
	unlock func()
}

func (r *readCloserWithWait) Close() error {
	err := r.ReadCloser.Close()
	r.once.Do(func() {
		<-r.done
		if r.unlock != nil {
			// Unlock only after the read loop exits so writes/deletes cannot race the stream.
			r.unlock()
		}
	})
	return err
}

func (e *erasureCodingPartStore) GetPartIds(ctx context.Context, tx database.Tx) ([]partstore.PartId, error) {
	ids := make([]partstore.PartId, 0)
	seen := make(map[string]struct{})
	for _, store := range e.partStores {
		storeIds, err := store.GetPartIds(ctx, tx)
		if err != nil {
			return nil, err
		}
		for _, id := range storeIds {
			k := id.String()
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func (e *erasureCodingPartStore) DeletePart(ctx context.Context, tx database.Tx, partId partstore.PartId) error {
	unlock := e.partLocker.Lock(partId)
	defer unlock()

	for i := 0; i < e.totalShards; i++ {
		if err := e.partStores[i].DeletePart(ctx, tx, partId); err != nil {
			return err
		}
	}
	return nil
}

func (e *erasureCodingPartStore) SupportsTxFreeDeletePart() bool {
	for _, store := range e.partStores {
		if !partstore.SupportsTxFreeDeletePart(store) {
			return false
		}
	}
	return true
}
