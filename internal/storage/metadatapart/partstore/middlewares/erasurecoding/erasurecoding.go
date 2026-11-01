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
	pithosmetrics "github.com/jdillenkofer/pithos/internal/metrics"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/task"
	"github.com/klauspost/reedsolomon"
	"github.com/prometheus/client_golang/prometheus"
)

var erasureMetricsOnce sync.Once
var erasureMetrics struct {
	scans, reconstructed, healWrites prometheus.Counter
	missing                          prometheus.Gauge
}

func registerErasureMetrics() {
	erasureMetricsOnce.Do(func() {
		erasureMetrics.scans = prometheus.NewCounter(prometheus.CounterOpts{Namespace: "pithos", Subsystem: "erasure_coding", Name: "heal_scans_total", Help: "Number of erasure coding heal scans"})
		erasureMetrics.missing = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "pithos", Subsystem: "erasure_coding", Name: "shards_missing", Help: "Number of missing shards observed by the latest healing read"})
		erasureMetrics.reconstructed = prometheus.NewCounter(prometheus.CounterOpts{Namespace: "pithos", Subsystem: "erasure_coding", Name: "reads_reconstructed_total", Help: "Number of reads requiring shard reconstruction"})
		erasureMetrics.healWrites = prometheus.NewCounter(prometheus.CounterOpts{Namespace: "pithos", Subsystem: "erasure_coding", Name: "heal_writes_total", Help: "Number of replacement shard writes started"})
	})
	pithosmetrics.Register(erasureMetrics.scans, erasureMetrics.missing, erasureMetrics.reconstructed, erasureMetrics.healWrites)
}

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
	// stripePool and shardBufPool recycle PutPart stripe buffers and GetPart
	// shard payload buffers across requests.
	stripePool   sync.Pool
	shardBufPool sync.Pool
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

func (e *erasureCodingPartStore) SupportsStats() bool {
	for _, store := range e.partStores {
		provider, ok := store.(partstore.StatsProvider)
		if !ok || !provider.SupportsStats() {
			return false
		}
	}
	return true
}

func (e *erasureCodingPartStore) Stats(ctx context.Context, tx database.Tx) (partstore.Stats, error) {
	var total partstore.Stats
	for _, store := range e.partStores {
		stats, _, err := partstore.StatsOf(ctx, tx, store)
		if err != nil {
			return partstore.Stats{}, err
		}
		total.Parts += stats.Parts
		total.Bytes += stats.Bytes
	}
	return total, nil
}

func NewWithPartStores(dataShards int, parityShards int, stripeShardSize int, partStores []partstore.PartStore, opts ...Option) (partstore.PartStore, error) {
	registerErasureMetrics()
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
	erasureMetrics.scans.Inc()
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

func putFrameHeader(b []byte, stripeIndex uint64, dataBytes int, payload []byte) {
	binary.BigEndian.PutUint64(b[0:8], stripeIndex)
	binary.BigEndian.PutUint32(b[8:12], uint32(dataBytes))
	binary.BigEndian.PutUint32(b[12:16], uint32(len(payload)))
	h := sha256.Sum256(payload)
	copy(b[16:48], h[:])
}

// parseFrameHeader returns the frame fields; the hash aliases b.
func parseFrameHeader(b []byte) (uint64, int, int, []byte, error) {
	if len(b) != frameHeaderSize {
		return 0, 0, 0, nil, errors.New("invalid frame header")
	}
	idx := binary.BigEndian.Uint64(b[0:8])
	dataBytes := int(binary.BigEndian.Uint32(b[8:12]))
	payloadLen := int(binary.BigEndian.Uint32(b[12:16]))
	if dataBytes < 1 || payloadLen < 1 {
		return 0, 0, 0, nil, errors.New("invalid frame header fields")
	}
	return idx, dataBytes, payloadLen, b[16:48], nil
}

// stripeFrames holds one encoded stripe. frames[i] is shard i's frame: a
// frame header followed by room for a full shard payload, so each frame goes
// to its shard store in a single write.
type stripeFrames struct {
	frames   [][]byte
	shards   [][]byte
	scratch  []byte
	n        int
	shardLen int
	wg       sync.WaitGroup
}

func (e *erasureCodingPartStore) getStripeFrames() *stripeFrames {
	if s, ok := e.stripePool.Get().(*stripeFrames); ok {
		return s
	}
	frameCap := frameHeaderSize + e.stripeShardSz
	buf := make([]byte, e.totalShards*frameCap)
	s := &stripeFrames{frames: make([][]byte, e.totalShards), shards: make([][]byte, e.totalShards)}
	for i := range s.frames {
		s.frames[i] = buf[i*frameCap : (i+1)*frameCap]
	}
	return s
}

// readStripe fills the data shards of s from reader. A short final stripe is
// laid out with the shorter shard length the encoding expects.
func (e *erasureCodingPartStore) readStripe(reader io.Reader, s *stripeFrames) error {
	s.n = 0
	s.shardLen = e.stripeShardSz
	for i := 0; i < e.dataShards; i++ {
		m, err := io.ReadFull(reader, s.frames[i][frameHeaderSize:])
		s.n += m
		if err == nil {
			continue
		}
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			return err
		}
		if s.n == 0 {
			return nil
		}
		if s.scratch == nil {
			s.scratch = make([]byte, e.dataShards*e.stripeShardSz)
		}
		scratch := s.scratch
		for j := 0; j <= i; j++ {
			copy(scratch[j*e.stripeShardSz:], s.frames[j][frameHeaderSize:])
		}
		s.shardLen = (s.n + e.dataShards - 1) / e.dataShards
		for j := 0; j < e.dataShards; j++ {
			payload := s.frames[j][frameHeaderSize : frameHeaderSize+s.shardLen]
			start := min(j*s.shardLen, s.n)
			end := min(start+s.shardLen, s.n)
			clear(payload[copy(payload, scratch[start:end]):])
		}
		break
	}
	for i := range s.shards {
		s.shards[i] = s.frames[i][frameHeaderSize : frameHeaderSize+s.shardLen]
	}
	return nil
}

func (e *erasureCodingPartStore) PutPart(ctx context.Context, tx database.Tx, partId partstore.PartId, reader io.Reader) error {
	unlock := e.partLocker.Lock(partId)
	defer unlock()

	pipeWriters := make([]*io.PipeWriter, e.totalShards)
	innerErrs := make([]error, e.totalShards)
	var innerWg sync.WaitGroup
	for i := 0; i < e.totalShards; i++ {
		pr, pw := io.Pipe()
		pipeWriters[i] = pw
		innerWg.Add(1)
		go func(idx int) {
			defer innerWg.Done()
			innerErrs[idx] = e.partStores[idx].PutPart(ctx, tx, partId, pr)
			// Fail pending writes if the store stopped reading early.
			_ = pr.CloseWithError(io.ErrClosedPipe)
		}(i)
	}

	// Each shard is written by one goroutine per stripe, and a stripe starts
	// only after the previous one finished, so writes stay in order.
	writeErrs := make([]error, e.totalShards)
	write := func(i int, b []byte) {
		if writeErrs[i] == nil {
			_, writeErrs[i] = pipeWriters[i].Write(b)
		}
	}
	finish := func(err error) error {
		for _, pw := range pipeWriters {
			if err != nil {
				_ = pw.CloseWithError(err)
			} else {
				_ = pw.Close()
			}
		}
		innerWg.Wait()
		if err != nil {
			return err
		}
		for _, innerErr := range innerErrs {
			if innerErr != nil {
				return innerErr
			}
		}
		for _, writeErr := range writeErrs {
			if writeErr != nil {
				return writeErr
			}
		}
		return nil
	}

	var inflight sync.WaitGroup
	inflight.Add(e.totalShards)
	for i := 0; i < e.totalShards; i++ {
		go func(i int) {
			defer inflight.Done()
			write(i, e.shardHeader(i))
		}(i)
	}
	pending := &inflight

	// Two stripe buffers let the next stripe be read and encoded while the
	// previous one is still being hashed and written.
	var stripes [2]*stripeFrames
	defer func() {
		for _, s := range stripes {
			if s != nil {
				e.stripePool.Put(s)
			}
		}
	}()
	for stripeIndex := uint64(0); ; stripeIndex++ {
		if stripes[stripeIndex%2] == nil {
			stripes[stripeIndex%2] = e.getStripeFrames()
		}
		s := stripes[stripeIndex%2]
		err := e.readStripe(reader, s)
		if err == nil && s.n > 0 {
			err = e.enc.Encode(s.shards)
		}
		pending.Wait()
		if err != nil {
			return finish(err)
		}
		if s.n == 0 {
			break
		}
		s.wg.Add(e.totalShards)
		for i := 0; i < e.totalShards; i++ {
			go func(i int, stripeIndex uint64) {
				defer s.wg.Done()
				frame := s.frames[i][:frameHeaderSize+s.shardLen]
				putFrameHeader(frame[:frameHeaderSize], stripeIndex, s.n, s.shards[i])
				write(i, frame)
			}(i, stripeIndex)
		}
		pending = &s.wg
		if s.n < e.dataShards*e.stripeShardSz {
			break
		}
	}
	pending.Wait()
	return finish(nil)
}

func (e *erasureCodingPartStore) Capabilities() partstore.Capabilities {
	if len(e.partStores) == 0 {
		return 0
	}
	capabilities := partstore.CapabilitiesOf(e.partStores[0])
	for _, store := range e.partStores[1:] {
		capabilities &= partstore.CapabilitiesOf(store)
	}
	// GetPart heals missing shards with PutPart in the caller's transaction.
	if !capabilities.Has(partstore.CapabilityTxFreePutPart) {
		capabilities &^= partstore.NewCapabilities(partstore.CapabilityTxFreeGetPart)
	}
	return capabilities
}

func (e *erasureCodingPartStore) GetPart(ctx context.Context, tx database.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	unlock := e.partLocker.RLock(partId)
	readers, healShards, err := e.openPartReaders(ctx, tx, partId)
	if err != nil {
		unlock()
		return nil, err
	}
	missing := 0
	for _, heal := range healShards {
		if heal {
			missing++
		}
	}
	erasureMetrics.missing.Set(float64(missing))
	if missing > 0 {
		erasureMetrics.reconstructed.Inc()
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

// shardStream reads the frames of one shard. Reads stream only as many
// shards as reconstruction needs plus one spare, whose payloads are hashed
// only when they are used. The spare also reveals data shards that end early.
// Other shards stay open but unread and skip ahead to the current stripe when
// they are needed.
type shardStream struct {
	rc         io.ReadCloser
	active     bool
	spare      bool
	next       uint64
	header     [frameHeaderSize]byte
	buf        []byte
	payload    []byte
	ok         bool
	unverified bool
	sawFrame   bool
	err        error
}

func (e *erasureCodingPartStore) readFrame(s *shardStream, stripeIndex uint64) {
	s.ok, s.unverified, s.sawFrame, s.payload = false, false, false, nil
	for {
		atStripe := s.next == stripeIndex
		if _, err := io.ReadFull(s.rc, s.header[:]); err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				s.err = err
			}
			return
		}
		s.sawFrame = atStripe
		idx, _, payloadLen, expectedHash, err := parseFrameHeader(s.header[:])
		if err != nil || idx != s.next || payloadLen > e.stripeShardSz {
			return
		}
		s.next++
		if idx < stripeIndex {
			if !skipBytes(s.rc, payloadLen) {
				return
			}
			continue
		}
		if s.buf == nil {
			s.buf = e.getShardBuf()
		}
		payload := s.buf[:payloadLen]
		if _, err := io.ReadFull(s.rc, payload); err != nil {
			return
		}
		s.payload = payload
		if s.spare {
			s.unverified = true
			return
		}
		s.ok = verifyPayload(expectedHash, payload)
		return
	}
}

func verifyPayload(expectedHash []byte, payload []byte) bool {
	h := sha256.Sum256(payload)
	return bytes.Equal(expectedHash, h[:])
}

func (e *erasureCodingPartStore) getShardBuf() []byte {
	if b, ok := e.shardBufPool.Get().(*[]byte); ok {
		return *b
	}
	return make([]byte, e.stripeShardSz)
}

func (e *erasureCodingPartStore) putShardBuf(b []byte) {
	if cap(b) == e.stripeShardSz {
		b = b[:cap(b)]
		e.shardBufPool.Put(&b)
	}
}

func skipBytes(r io.Reader, n int) bool {
	if seeker, ok := r.(io.Seeker); ok {
		_, err := seeker.Seek(int64(n), io.SeekCurrent)
		return err == nil
	}
	_, err := io.CopyN(io.Discard, r, int64(n))
	return err == nil
}

func (e *erasureCodingPartStore) newPartReader(ctx context.Context, tx database.Tx, partId partstore.PartId, readers []io.ReadCloser, healShards []bool, healMissing bool, unlock func()) io.ReadCloser {
	pr, pw := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		streams := make([]*shardStream, e.totalShards)
		for i, rc := range readers {
			streams[i] = &shardStream{rc: rc}
		}
		defer func() {
			for _, s := range streams {
				if s.rc != nil {
					_ = s.rc.Close()
				}
				if s.buf != nil {
					e.putShardBuf(s.buf)
				}
			}
		}()

		healPipeWriters := make([]*io.PipeWriter, e.totalShards)
		healErrCh := make(chan error, e.totalShards)
		healingShardCount := 0
		closeHealingWriters := func(closeErr error) error {
			for _, writer := range healPipeWriters {
				if writer != nil {
					_ = writer.CloseWithError(closeErr)
				}
			}
			for i := 0; i < healingShardCount; i++ {
				closeErr = errors.Join(closeErr, <-healErrCh)
			}
			return closeErr
		}
		if healMissing {
			for i := 0; i < e.totalShards; i++ {
				if !healShards[i] {
					continue
				}
				prHeal, pwHeal := io.Pipe()
				healPipeWriters[i] = pwHeal
				healingShardCount++
				erasureMetrics.healWrites.Inc()
				go func(idx int, reader *io.PipeReader) {
					err := e.partStores[idx].PutPart(ctx, tx, partId, reader)
					_ = reader.CloseWithError(io.ErrClosedPipe)
					healErrCh <- err
				}(i, prHeal)
				if _, err := pwHeal.Write(e.shardHeader(i)); err != nil {
					_ = pw.CloseWithError(closeHealingWriters(err))
					return
				}
			}
		}

		fail := func(err error) {
			_ = pw.CloseWithError(closeHealingWriters(err))
		}

		// Reconstruction must produce the data shards and any shard being healed.
		required := make([]bool, e.totalShards)
		for i := range required {
			required[i] = i < e.dataShards || healPipeWriters[i] != nil
		}

		// activate starts streaming up to want more shards, preferring data
		// shards so healthy stripes need no reconstruction.
		activate := func(want int, spare bool) []int {
			var started []int
			for i, s := range streams {
				if len(started) == want {
					break
				}
				if s.rc != nil && !s.active {
					s.active, s.spare = true, spare
					started = append(started, i)
				}
			}
			return started
		}
		hasSpare := func() bool {
			for _, s := range streams {
				if s.spare {
					return true
				}
			}
			return false
		}
		readFrames := func(idxs []int, stripeIndex uint64) error {
			if len(idxs) == 1 {
				e.readFrame(streams[idxs[0]], stripeIndex)
			} else {
				var wg sync.WaitGroup
				for _, i := range idxs {
					wg.Add(1)
					go func(s *shardStream) {
						defer wg.Done()
						e.readFrame(s, stripeIndex)
					}(streams[i])
				}
				wg.Wait()
			}
			for _, i := range idxs {
				s := streams[i]
				if s.err != nil {
					return s.err
				}
				if !s.ok && !s.unverified {
					_ = s.rc.Close()
					s.rc, s.active, s.spare = nil, false, false
					healShards[i] = true
				}
			}
			return nil
		}

		activate(e.dataShards, false)
		shards := make([][]byte, e.totalShards)
		for stripeIndex := uint64(0); ; stripeIndex++ {
			if !hasSpare() {
				activate(1, true)
			}
			var idxs []int
			for i, s := range streams {
				s.ok, s.unverified, s.sawFrame = false, false, false
				if s.active {
					idxs = append(idxs, i)
				}
			}
			if err := readFrames(idxs, stripeIndex); err != nil {
				fail(err)
				return
			}
			for {
				available, seen := 0, false
				for _, s := range streams {
					if s.ok {
						available++
					}
					seen = seen || s.sawFrame
				}
				// No streamed shard, the spare included, has another frame:
				// the part ended.
				if available >= e.dataShards || !seen {
					break
				}
				// A shard failed: use the spare first, then stream more.
				usedSpare := false
				for i, s := range streams {
					if !s.spare {
						continue
					}
					s.spare = false
					if s.unverified {
						s.unverified = false
						s.ok = verifyPayload(s.header[16:48], s.payload)
					}
					if !s.ok {
						_ = s.rc.Close()
						s.rc, s.active = nil, false
						healShards[i] = true
					}
					usedSpare = true
				}
				if usedSpare {
					continue
				}
				started := activate(e.dataShards-available, false)
				if len(started) == 0 {
					break
				}
				if err := readFrames(started, stripeIndex); err != nil {
					fail(err)
					return
				}
			}

			available, seen, dataBytes := 0, false, 0
			for i, s := range streams {
				seen = seen || s.sawFrame
				if s.ok {
					available++
					shards[i] = s.payload
					if dataBytes == 0 {
						dataBytes = int(binary.BigEndian.Uint32(s.header[8:12]))
					}
				} else {
					if s.buf == nil && required[i] {
						s.buf = e.getShardBuf()
					}
					shards[i] = s.buf[:0]
				}
			}
			if !seen {
				_ = pw.CloseWithError(closeHealingWriters(nil))
				return
			}
			if available < e.dataShards {
				fail(fmt.Errorf("insufficient shards in stripe %d", stripeIndex))
				return
			}
			needsReconstruct := false
			for i := range shards {
				needsReconstruct = needsReconstruct || (required[i] && len(shards[i]) == 0)
			}
			if needsReconstruct {
				if err := e.enc.ReconstructSome(shards, required); err != nil {
					fail(err)
					return
				}
				for i, s := range streams {
					if s.buf == nil && cap(shards[i]) > 0 {
						s.buf = shards[i][:cap(shards[i])]
					}
				}
			}
			var frameHeader [frameHeaderSize]byte
			for i := 0; i < e.totalShards; i++ {
				if healPipeWriters[i] == nil {
					continue
				}
				putFrameHeader(frameHeader[:], stripeIndex, dataBytes, shards[i])
				if _, err := healPipeWriters[i].Write(frameHeader[:]); err != nil {
					fail(err)
					return
				}
				if _, err := healPipeWriters[i].Write(shards[i]); err != nil {
					fail(err)
					return
				}
			}
			remaining := dataBytes
			for i := 0; i < e.dataShards && remaining > 0; i++ {
				chunk := shards[i][:min(len(shards[i]), remaining)]
				if _, err := pw.Write(chunk); err != nil {
					fail(err)
					return
				}
				remaining -= len(chunk)
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
