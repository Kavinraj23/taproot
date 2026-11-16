package spool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func open(t *testing.T, dir string, opt Options) *Spool {
	t.Helper()
	if opt.Logf == nil {
		opt.Logf = t.Logf
	}
	s, err := Open(dir, opt)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

func appendN(t *testing.T, s *Spool, from, n int) {
	t.Helper()
	for i := from; i < from+n; i++ {
		if err := s.Append([]byte(fmt.Sprintf("rec-%03d", i))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
}

// next reads one record, failing if none arrives quickly.
func next(t *testing.T, s *Spool) (Pos, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p, b, err := s.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	return p, string(b)
}

// none checks that nothing is waiting to be read.
func none(t *testing.T, s *Spool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, b, err := s.Next(ctx); err == nil {
		t.Fatalf("Next returned %q, want nothing", b)
	}
}

func segments(t *testing.T, dir string) []string {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// Case 1
func TestAppendThenReadInOrder(t *testing.T) {
	s := open(t, t.TempDir(), Options{})
	defer s.Close()
	appendN(t, s, 0, 3)
	for i := 0; i < 3; i++ {
		if _, got := next(t, s); got != fmt.Sprintf("rec-%03d", i) {
			t.Fatalf("record %d = %q", i, got)
		}
	}
	none(t, s)
}

// Case 2
func TestAckedRecordsNotRereadAfterReopen(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, Options{})
	appendN(t, s, 0, 3)
	p0, _ := next(t, s)
	p1, _ := next(t, s)
	s.Ack(p0)
	s.Ack(p1)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = open(t, dir, Options{})
	defer s.Close()
	if _, got := next(t, s); got != "rec-002" {
		t.Fatalf("after reopen got %q, want rec-002", got)
	}
	none(t, s)
}

// Case 3
func TestOutOfOrderAck(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, Options{})
	appendN(t, s, 0, 3)
	p0, _ := next(t, s)
	p1, _ := next(t, s)
	s.Ack(p1) // the later one first: the cursor must not skip rec-000
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := s.Pending(); got != 3 {
		t.Fatalf("Pending = %d after acking only the 2nd, want 3", got)
	}
	s.Ack(p0)
	if got := s.Pending(); got != 1 {
		t.Fatalf("Pending = %d after acking both, want 1", got)
	}
	s.Close()

	s = open(t, dir, Options{})
	defer s.Close()
	if _, got := next(t, s); got != "rec-002" {
		t.Fatalf("after reopen got %q, want rec-002", got)
	}
}

// Case 4
func TestUnsyncedAcksAreResent(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, Options{})
	appendN(t, s, 0, 2)
	p0, _ := next(t, s)
	s.Ack(p0)
	// A crash: no Sync, no Close. Release the files without saving the cursor.
	s.abandon()

	s = open(t, dir, Options{})
	defer s.Close()
	if _, got := next(t, s); got != "rec-000" {
		t.Fatalf("got %q, want rec-000 again (at-least-once)", got)
	}
}

// Case 5
func TestSegmentsRollAndAckedOnesAreDeleted(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, Options{SegmentBytes: 64, MaxBytes: 1 << 20})
	defer s.Close()
	appendN(t, s, 0, 100)
	if n := len(segments(t, dir)); n < 5 {
		t.Fatalf("%d segments, want several", n)
	}
	for i := 0; i < 100; i++ {
		p, got := next(t, s)
		if got != fmt.Sprintf("rec-%03d", i) {
			t.Fatalf("record %d = %q", i, got)
		}
		s.Ack(p)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if n := len(segments(t, dir)); n != 1 {
		t.Fatalf("%d segments left after acking everything, want 1 (the active one)", n)
	}
	if got := s.Pending(); got != 0 {
		t.Fatalf("Pending = %d, want 0", got)
	}
}

// Case 6
func TestCapDropsOldestSegment(t *testing.T) {
	dir := t.TempDir()
	// Each record is 8 header + 7 payload = 15 bytes; 4 per 60-byte segment.
	s := open(t, dir, Options{SegmentBytes: 60, MaxBytes: 180})
	defer s.Close()
	appendN(t, s, 0, 12) // fills 3 segments (180 bytes) and opens a 4th
	if s.Dropped() != 0 {
		t.Fatalf("Dropped = %d before going over the cap", s.Dropped())
	}
	appendN(t, s, 12, 1) // 195 bytes: over the cap, so the oldest segment goes
	if s.Dropped() != 4 {
		t.Fatalf("Dropped = %d, want 4 (one segment)", s.Dropped())
	}
	if _, got := next(t, s); got != "rec-004" {
		t.Fatalf("first record after the drop = %q, want rec-004", got)
	}
	if got := s.Pending(); got != 9 {
		t.Fatalf("Pending = %d, want 9", got)
	}
}

// Case 7
func TestTornTailIsTruncated(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, Options{})
	appendN(t, s, 0, 2)
	s.Close()

	segs := segments(t, dir)
	f, err := os.OpenFile(segs[len(segs)-1], os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0, 0, 0, 50, 1, 2}) // half a header: a crash mid-write
	f.Close()

	s = open(t, dir, Options{})
	defer s.Close()
	appendN(t, s, 2, 1)
	for _, want := range []string{"rec-000", "rec-001", "rec-002"} {
		if _, got := next(t, s); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
	none(t, s)
}

// Case 8
func TestBadChecksumOnLastRecordIsTruncated(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, Options{})
	appendN(t, s, 0, 2)
	s.Close()

	segs := segments(t, dir)
	b, err := os.ReadFile(segs[len(segs)-1])
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 0xff // flip a payload byte of the last record
	if err := os.WriteFile(segs[len(segs)-1], b, 0o644); err != nil {
		t.Fatal(err)
	}

	s = open(t, dir, Options{})
	defer s.Close()
	if _, got := next(t, s); got != "rec-000" {
		t.Fatalf("got %q, want rec-000", got)
	}
	none(t, s)
}

// Case 9
func TestNextWaitsForAppendAndHonoursCancel(t *testing.T) {
	s := open(t, t.TempDir(), Options{})
	defer s.Close()

	got := make(chan string, 1)
	go func() {
		_, b, err := s.Next(context.Background())
		if err != nil {
			got <- "error: " + err.Error()
			return
		}
		got <- string(b)
	}()
	time.Sleep(50 * time.Millisecond)
	appendN(t, s, 0, 1)
	select {
	case g := <-got:
		if g != "rec-000" {
			t.Fatalf("got %q", g)
		}
	case <-time.After(time.Second):
		t.Fatal("Next did not wake up after Append")
	}

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { _, _, err := s.Next(ctx); errc <- err }()
	cancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Next did not return after cancel")
	}
}

// Case 10
func TestCursorIntoDeletedSegmentStartsAtOldest(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, Options{SegmentBytes: 60, MaxBytes: 1 << 20})
	appendN(t, s, 0, 8) // two full segments, an empty third
	s.Close()
	if err := os.WriteFile(filepath.Join(dir, "cursor"), []byte("0 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	segs := segments(t, dir)
	os.Remove(segs[0]) // records 0-3 gone, the cursor points before them

	s = open(t, dir, Options{SegmentBytes: 60, MaxBytes: 1 << 20})
	defer s.Close()
	if _, got := next(t, s); got != "rec-004" {
		t.Fatalf("got %q, want rec-004 (the oldest remaining)", got)
	}
}

func TestEmptyDirStartsClean(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "new", "buffer") // created by Open
	s := open(t, dir, Options{})
	defer s.Close()
	none(t, s)
	if s.Pending() != 0 || s.Dropped() != 0 {
		t.Fatalf("Pending %d Dropped %d, want 0 0", s.Pending(), s.Dropped())
	}
}

func TestPendingCountsUnackedAfterReopen(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, Options{SegmentBytes: 60, MaxBytes: 1 << 20})
	appendN(t, s, 0, 10)
	p, _ := next(t, s)
	s.Ack(p)
	s.Close()
	s = open(t, dir, Options{SegmentBytes: 60, MaxBytes: 1 << 20})
	defer s.Close()
	if got := s.Pending(); got != 9 {
		t.Fatalf("Pending = %d, want 9", got)
	}
}

func TestClosedSpoolRejects(t *testing.T) {
	s := open(t, t.TempDir(), Options{})
	s.Close()
	if err := s.Append([]byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append after Close: %v, want ErrClosed", err)
	}
	if _, _, err := s.Next(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Next after Close: %v, want ErrClosed", err)
	}
}

// abandon releases the files like a crash would: no Sync, so the cursor isn't saved.
func (s *Spool) abandon() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.rf != nil {
		s.rf.Close()
	}
	s.active.Close()
}
