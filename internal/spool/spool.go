// Package spool is the agent's disk buffer: an append-only log of records that are kept until
// they are acknowledged, so points survive a Redpanda outage of hours and an agent restart.
//
// Think of a filing cabinet with numbered folders (segments). New records go in the last
// folder; the sender reads them in order and crosses each off once Redpanda confirms it (Ack).
// A bookmark (the cursor file) remembers the first record not yet crossed off. Folders entirely
// before the bookmark are thrown away. If the cabinet is over its size cap, the oldest folder is
// thrown away even though it wasn't sent, and those records are counted as dropped.
//
// Delivery is at-least-once: the bookmark is saved by Sync (about once a second), so after a
// crash the records acknowledged since the last save are sent again. The backend's dedup drops
// them.
//
// On disk, in dir: segment files %020d.seg holding records framed as
// uint32 length | uint32 CRC-32 (IEEE) of the payload | payload (big-endian), and a "cursor"
// file holding "<segment id> <byte offset>" of the first unacknowledged record.
package spool

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// ErrClosed is returned by Append and Next once the spool is closed.
var ErrClosed = errors.New("spool closed")

const (
	headerLen  = 8
	maxRecord  = 16 << 20 // a length above this is corruption, not a record
	cursorFile = "cursor"
)

// Options size the spool. Zero values take the defaults.
type Options struct {
	SegmentBytes int64 // start a new segment once the active one reaches this (default 4 MiB)
	MaxBytes     int64 // drop the oldest segment once the total exceeds this (default 256 MiB)
	Logf         func(format string, args ...any)
}

// Pos is where a record starts: its segment and byte offset.
type Pos struct {
	Seg uint64
	Off int64
}

func (p Pos) before(q Pos) bool { return p.Seg < q.Seg || (p.Seg == q.Seg && p.Off < q.Off) }

type segment struct {
	id    uint64
	size  int64 // bytes of valid records
	count int   // valid records
}

type inflight struct {
	pos, next Pos
	acked     bool
}

// Spool is safe for one reader (Next/Ack) and any number of writers (Append) at once.
type Spool struct {
	dir string
	opt Options

	mu       sync.Mutex
	segs     []*segment // oldest first; the last is the active one
	active   *os.File   // append handle on the last segment
	rf       *os.File   // read handle on segment rfID
	rfID     uint64
	rd       Pos        // next record Next returns
	flight   []inflight // read but not yet acknowledged, in read order
	cursor   Pos        // first record not yet acknowledged
	pending  int        // records at or after cursor
	dropped  int
	wake     chan struct{} // closed on Append, so a waiting Next looks again
	closed   bool
	overfull bool // inside a cap-drop episode, to log once
}

// Open opens or creates the spool in dir, recovering from a crash: a half-written record at
// the end of the last segment is cut off, and reading resumes at the saved cursor.
func Open(dir string, opt Options) (*Spool, error) {
	if opt.SegmentBytes <= 0 {
		opt.SegmentBytes = 4 << 20
	}
	if opt.MaxBytes <= 0 {
		opt.MaxBytes = 256 << 20
	}
	if opt.MaxBytes < 2*opt.SegmentBytes { // room for the active segment plus one full one
		opt.MaxBytes = 2 * opt.SegmentBytes
	}
	if opt.Logf == nil {
		opt.Logf = func(string, ...any) {}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("spool dir: %w", err)
	}
	s := &Spool{dir: dir, opt: opt, wake: make(chan struct{})}

	ids, err := s.listSegments()
	if err != nil {
		return nil, err
	}
	for i, id := range ids {
		seg, err := s.scan(id, i == len(ids)-1)
		if err != nil {
			return nil, err
		}
		s.segs = append(s.segs, seg)
	}
	if len(s.segs) == 0 {
		s.segs = []*segment{{id: 1}}
	}
	last := s.segs[len(s.segs)-1]
	if s.active, err = os.OpenFile(s.path(last.id), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err != nil {
		return nil, fmt.Errorf("open active segment: %w", err)
	}

	s.cursor = s.loadCursor()
	s.rd = s.cursor
	if s.pending, err = s.countFrom(s.cursor); err != nil {
		s.active.Close()
		return nil, err
	}
	return s, nil
}

func (s *Spool) path(id uint64) string { return filepath.Join(s.dir, fmt.Sprintf("%020d.seg", id)) }

func (s *Spool) listSegments() ([]uint64, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("spool dir: %w", err)
	}
	var ids []uint64
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), ".seg")
		if !ok {
			continue
		}
		if id, err := strconv.ParseUint(name, 10, 64); err == nil {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// scan validates a segment's records. A bad record ends the segment: on the last segment it
// is a crash mid-write and the file is cut there; on an earlier one the rest is skipped.
func (s *Spool) scan(id uint64, last bool) (*segment, error) {
	f, err := os.Open(s.path(id))
	if err != nil {
		return nil, fmt.Errorf("open segment %d: %w", id, err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	seg := &segment{id: id}
	for seg.size < st.Size() {
		n, _, err := readRecord(f, seg.size)
		if err != nil {
			break
		}
		seg.size += n
		seg.count++
	}
	f.Close()
	if seg.size < st.Size() {
		if last {
			s.opt.Logf("spool: cutting %d bytes of a half-written record from segment %d", st.Size()-seg.size, id)
			if err := os.Truncate(s.path(id), seg.size); err != nil {
				return nil, fmt.Errorf("truncate segment %d: %w", id, err)
			}
		} else {
			s.opt.Logf("spool: segment %d is damaged after byte %d; skipping the rest of it", id, seg.size)
		}
	}
	return seg, nil
}

// readRecord reads the record at off and returns its total length and payload.
func readRecord(f *os.File, off int64) (int64, []byte, error) {
	var h [headerLen]byte
	if _, err := f.ReadAt(h[:], off); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(h[0:4])
	if n > maxRecord {
		return 0, nil, fmt.Errorf("record length %d too large", n)
	}
	b := make([]byte, n)
	if _, err := f.ReadAt(b, off+headerLen); err != nil {
		return 0, nil, err
	}
	if crc32.ChecksumIEEE(b) != binary.BigEndian.Uint32(h[4:8]) {
		return 0, nil, errors.New("checksum mismatch")
	}
	return headerLen + int64(n), b, nil
}

func (s *Spool) loadCursor() Pos {
	first := Pos{Seg: s.segs[0].id}
	b, err := os.ReadFile(filepath.Join(s.dir, cursorFile))
	if err != nil {
		return first
	}
	var p Pos
	if _, err := fmt.Sscan(string(b), &p.Seg, &p.Off); err != nil {
		s.opt.Logf("spool: unreadable cursor %q; starting from the oldest record", b)
		return first
	}
	if p.before(first) {
		return first // its segment was dropped or deleted
	}
	if seg := s.seg(p.Seg); seg == nil {
		return first
	} else if p.Off > seg.size {
		p.Off = seg.size
	}
	return s.normalize(p)
}

// seg returns the segment with this id, or nil. Ids are consecutive.
func (s *Spool) seg(id uint64) *segment {
	if len(s.segs) == 0 || id < s.segs[0].id {
		return nil
	}
	i := id - s.segs[0].id
	if i >= uint64(len(s.segs)) {
		return nil
	}
	return s.segs[i]
}

// normalize moves a position at the end of a finished segment to the start of the next one.
func (s *Spool) normalize(p Pos) Pos {
	for {
		seg := s.seg(p.Seg)
		if seg == nil || p.Off < seg.size || seg == s.segs[len(s.segs)-1] {
			return p
		}
		p = Pos{Seg: p.Seg + 1}
	}
}

// countFrom counts the records from p to the end.
func (s *Spool) countFrom(p Pos) (int, error) {
	n := 0
	for _, seg := range s.segs {
		switch {
		case seg.id < p.Seg:
		case seg.id > p.Seg || p.Off == 0:
			n += seg.count
		default:
			c, err := s.countIn(seg, p.Off)
			if err != nil {
				return 0, err
			}
			n += c
		}
	}
	return n, nil
}

func (s *Spool) countIn(seg *segment, off int64) (int, error) {
	f, err := os.Open(s.path(seg.id))
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n := 0
	for off < seg.size {
		l, _, err := readRecord(f, off)
		if err != nil {
			return 0, fmt.Errorf("count segment %d: %w", seg.id, err)
		}
		off += l
		n++
	}
	return n, nil
}

// Append adds a record. It is written straight to the file, so a killed process loses
// nothing; Sync makes it survive a machine crash too.
func (s *Spool) Append(payload []byte) error {
	if len(payload) > maxRecord {
		return fmt.Errorf("record of %d bytes is too large", len(payload))
	}
	buf := make([]byte, headerLen+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(payload))
	copy(buf[headerLen:], payload)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if _, err := s.active.Write(buf); err != nil { // one write, so a crash tears at most one record
		return fmt.Errorf("spool append: %w", err)
	}
	last := s.segs[len(s.segs)-1]
	last.size += int64(len(buf))
	last.count++
	s.pending++
	if last.size >= s.opt.SegmentBytes {
		if err := s.roll(); err != nil {
			return err
		}
	}
	s.enforceCap()
	close(s.wake)
	s.wake = make(chan struct{})
	return nil
}

// roll starts a new active segment.
func (s *Spool) roll() error {
	if err := s.active.Sync(); err != nil {
		return fmt.Errorf("spool sync: %w", err)
	}
	if err := s.active.Close(); err != nil {
		return err
	}
	id := s.segs[len(s.segs)-1].id + 1
	f, err := os.OpenFile(s.path(id), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("spool new segment: %w", err)
	}
	s.active = f
	s.segs = append(s.segs, &segment{id: id})
	return nil
}

// enforceCap drops the oldest segments while the total is over MaxBytes. The active segment
// is never dropped.
func (s *Spool) enforceCap() {
	total := int64(0)
	for _, seg := range s.segs {
		total += seg.size
	}
	if total <= s.opt.MaxBytes {
		s.overfull = false
		return
	}
	for total > s.opt.MaxBytes && len(s.segs) > 1 {
		old := s.segs[0]
		lost := old.count
		if s.cursor.Seg == old.id && s.cursor.Off > 0 {
			if c, err := s.countIn(old, s.cursor.Off); err == nil {
				lost = c
			}
		} else if s.cursor.Seg > old.id {
			lost = 0 // already acknowledged; Sync just hadn't deleted it yet
		}
		s.closeReader(old.id)
		if err := os.Remove(s.path(old.id)); err != nil {
			s.opt.Logf("spool: remove segment %d: %v", old.id, err)
		}
		s.segs = s.segs[1:]
		total -= old.size
		s.dropped += lost
		s.pending -= lost
		next := Pos{Seg: s.segs[0].id}
		if s.cursor.before(next) {
			s.cursor = next
		}
		if s.rd.before(next) {
			s.rd = next
		}
		// Records of the dropped segment still in flight may yet be delivered; their acks are
		// ignored, as the cursor has already moved past them.
		kept := s.flight[:0]
		for _, f := range s.flight {
			if !f.pos.before(next) {
				kept = append(kept, f)
			}
		}
		s.flight = kept
		if !s.overfull {
			s.opt.Logf("spool: over its %d MB cap; dropping the oldest unsent points", s.opt.MaxBytes>>20)
			s.overfull = true
		}
	}
}

func (s *Spool) closeReader(id uint64) {
	if s.rf != nil && s.rfID == id {
		s.rf.Close()
		s.rf = nil
	}
}

// Next returns the next unread record, waiting until there is one or ctx is done. Every
// record returned must eventually be passed to Ack once it is safely delivered.
func (s *Spool) Next(ctx context.Context) (Pos, []byte, error) {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return Pos{}, nil, ErrClosed
		}
		p, b, ok, err := s.read()
		wake := s.wake
		s.mu.Unlock()
		if err != nil || ok {
			return p, b, err
		}
		select {
		case <-ctx.Done():
			return Pos{}, nil, ctx.Err()
		case <-wake:
		}
	}
}

// read returns the record at rd if there is one. Called with mu held.
func (s *Spool) read() (Pos, []byte, bool, error) {
	for {
		s.rd = s.normalize(s.rd)
		seg := s.seg(s.rd.Seg)
		if seg == nil || s.rd.Off >= seg.size {
			return Pos{}, nil, false, nil
		}
		if s.rf == nil || s.rfID != seg.id {
			if s.rf != nil {
				s.rf.Close()
			}
			f, err := os.Open(s.path(seg.id))
			if err != nil {
				s.rf = nil
				return Pos{}, nil, false, fmt.Errorf("spool read: %w", err)
			}
			s.rf, s.rfID = f, seg.id
		}
		n, b, err := readRecord(s.rf, s.rd.Off)
		if err != nil {
			// Validated on open or written by us, so this is damage on disk: skip the segment.
			s.opt.Logf("spool: segment %d unreadable at byte %d: %v; skipping the rest of it", seg.id, s.rd.Off, err)
			seg.size = s.rd.Off
			continue
		}
		p := s.rd
		s.rd = Pos{Seg: p.Seg, Off: p.Off + n}
		s.flight = append(s.flight, inflight{pos: p, next: s.rd})
		return p, b, true, nil
	}
}

// Ack marks a record returned by Next as delivered. Acks may arrive in any order; the cursor
// only moves past records that are all acknowledged.
func (s *Spool) Ack(p Pos) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.flight {
		if s.flight[i].pos == p {
			s.flight[i].acked = true
			break
		}
	}
	popped := 0
	for popped < len(s.flight) && s.flight[popped].acked {
		s.cursor = s.flight[popped].next
		popped++
	}
	s.flight = s.flight[popped:]
	s.pending -= popped
}

// Sync makes appended records durable, saves the cursor, and deletes segments that are fully
// acknowledged.
func (s *Spool) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	return s.sync()
}

func (s *Spool) sync() error {
	if err := s.active.Sync(); err != nil {
		return fmt.Errorf("spool sync: %w", err)
	}
	s.cursor = s.normalize(s.cursor)
	tmp := filepath.Join(s.dir, cursorFile+".tmp")
	if err := os.WriteFile(tmp, []byte(fmt.Sprintf("%d %d\n", s.cursor.Seg, s.cursor.Off)), 0o644); err != nil {
		return fmt.Errorf("spool cursor: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, cursorFile)); err != nil {
		return fmt.Errorf("spool cursor: %w", err)
	}
	for len(s.segs) > 1 && s.segs[0].id < s.cursor.Seg {
		s.closeReader(s.segs[0].id)
		if err := os.Remove(s.path(s.segs[0].id)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("spool remove segment: %w", err)
		}
		s.segs = s.segs[1:]
	}
	return nil
}

// Pending is how many records are not yet acknowledged: the backlog waiting on disk.
func (s *Spool) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pending
}

// Dropped is how many records the size cap has thrown away since Open.
func (s *Spool) Dropped() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dropped
}

// Close saves the cursor and releases the files. A waiting Next returns ErrClosed.
func (s *Spool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	err := s.sync()
	s.closed = true
	close(s.wake)
	if s.rf != nil {
		s.rf.Close()
	}
	if cerr := s.active.Close(); err == nil {
		err = cerr
	}
	return err
}

var _ io.Closer = (*Spool)(nil)
