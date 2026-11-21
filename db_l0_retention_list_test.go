package litestream

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/superfly/ltx"
)

// countingLTXClient records every level-1 listing. Retention tests use it to
// show that repeated L0 checks do not walk the whole L1 level.
type countingLTXClient struct {
	mu      sync.Mutex
	files   map[int][]*ltx.FileInfo
	l1      []l1ListCall
	deleted []ltx.TXID
}

type l1ListCall struct {
	seek  ltx.TXID
	items int
}

func newCountingLTXClient() *countingLTXClient {
	return &countingLTXClient{files: make(map[int][]*ltx.FileInfo)}
}

func (c *countingLTXClient) add(info *ltx.FileInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := *info
	c.files[info.Level] = append(c.files[info.Level], &cp)
}

func (c *countingLTXClient) has(level int, minTXID, maxTXID ltx.TXID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, info := range c.files[level] {
		if info.MinTXID == minTXID && info.MaxTXID == maxTXID {
			return true
		}
	}
	return false
}

func (c *countingLTXClient) l1Lists() []l1ListCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]l1ListCall, len(c.l1))
	copy(out, c.l1)
	return out
}

func (c *countingLTXClient) deletedMaxTXIDs() []ltx.TXID {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]ltx.TXID, len(c.deleted))
	copy(out, c.deleted)
	return out
}

func (c *countingLTXClient) Type() string { return "file" }

func (c *countingLTXClient) Init(context.Context) error { return nil }

func (c *countingLTXClient) SetLogger(*slog.Logger) {}

func (c *countingLTXClient) LTXFiles(_ context.Context, level int, seek ltx.TXID, _ bool) (ltx.FileIterator, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var infos []*ltx.FileInfo
	for _, info := range c.files[level] {
		if info.MinTXID < seek {
			continue
		}
		cp := *info
		infos = append(infos, &cp)
	}
	if level == 1 {
		c.l1 = append(c.l1, l1ListCall{seek: seek, items: len(infos)})
	}
	return ltx.NewFileInfoSliceIterator(infos), nil
}

func (c *countingLTXClient) OpenLTXFile(context.Context, int, ltx.TXID, ltx.TXID, int64, int64) (io.ReadCloser, error) {
	return nil, os.ErrNotExist
}

func (c *countingLTXClient) WriteLTXFile(context.Context, int, ltx.TXID, ltx.TXID, io.Reader) (*ltx.FileInfo, error) {
	return nil, os.ErrInvalid
}

func (c *countingLTXClient) DeleteLTXFiles(_ context.Context, infos []*ltx.FileInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, info := range infos {
		if info == nil {
			continue
		}
		c.deleted = append(c.deleted, info.MaxTXID)
		remaining := make([]*ltx.FileInfo, 0, len(c.files[info.Level]))
		for _, cur := range c.files[info.Level] {
			if cur.MinTXID == info.MinTXID && cur.MaxTXID == info.MaxTXID {
				continue
			}
			remaining = append(remaining, cur)
		}
		c.files[info.Level] = remaining
	}
	return nil
}

func (c *countingLTXClient) DeleteAll(context.Context) error { return nil }

func agedLTX(level int, minTXID, maxTXID ltx.TXID) *ltx.FileInfo {
	return &ltx.FileInfo{
		Level:     level,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		CreatedAt: time.Now().Add(-time.Hour),
		Size:      1,
	}
}

func openRetentionDB(t *testing.T, client ReplicaClient) *DB {
	t.Helper()
	db := NewDB(t.TempDir() + "/db")
	db.MonitorInterval = 0
	db.ShutdownSyncTimeout = 0
	db.L0Retention = time.Second
	db.Replica = NewReplicaWithClient(db, client)
	db.Replica.MonitorEnabled = false
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(context.Background()); err != nil {
			t.Errorf("close db: %v", err)
		}
	})
	return db
}

func l1ListStats(lists []l1ListCall) (full, items int) {
	for _, call := range lists {
		items += call.items
		if call.seek == 0 {
			full++
		}
	}
	return full, items
}

// TestDB_EnforceL0RetentionByTime_ReusesCachedL1Max is the regression test for
// issue 1633. Before the fix each check listed every L1 file. After it, K
// checks over M L1 files perform one full level-1 listing.
func TestDB_EnforceL0RetentionByTime_ReusesCachedL1Max(t *testing.T) {
	const (
		l1Files = 200
		checks  = 5
	)
	client := newCountingLTXClient()
	for i := 1; i <= l1Files; i++ {
		tx := ltx.TXID(i)
		client.add(agedLTX(1, tx, tx))
	}
	for i := 1; i <= 3; i++ {
		tx := ltx.TXID(i)
		client.add(agedLTX(0, tx, tx))
	}
	uncompacted := ltx.TXID(l1Files + 1)
	client.add(agedLTX(0, uncompacted, uncompacted))

	db := openRetentionDB(t, client)
	// Position stays at or ahead of every L1 file this test writes, which is
	// the steady state of a replica that compacted its own L0 uploads.
	db.Replica.SetPos(ltx.Pos{TXID: uncompacted + 1})

	ctx := context.Background()
	for i := 0; i < checks; i++ {
		if err := db.EnforceL0RetentionByTime(ctx); err != nil {
			t.Fatalf("check %d: %v", i, err)
		}
	}

	lists := client.l1Lists()
	full, items := l1ListStats(lists)
	t.Logf("checks=%d l1Files=%d l1ListCalls=%d l1ItemsIterated=%d fullL1Lists=%d",
		checks, l1Files, len(lists), items, full)
	if len(lists) != 1 || full != 1 || items != l1Files {
		t.Fatalf("L1 listings=%+v, want 1 full listing of %d files (not %d listings of %d items)",
			lists, l1Files, checks, checks*l1Files)
	}
	for i := 1; i <= 3; i++ {
		tx := ltx.TXID(i)
		if client.has(0, tx, tx) {
			t.Fatalf("compacted L0 %s was kept", tx)
		}
	}
	if !client.has(0, uncompacted, uncompacted) {
		t.Fatal("deleted L0 file that is not compacted into L1")
	}

	// A later L1 compaction records the new file in the same cache. The next
	// retention check must observe that TXID without listing L1 again.
	client.add(agedLTX(1, uncompacted, uncompacted))
	next := uncompacted + 1
	client.add(agedLTX(0, next, next))
	db.compactor.CacheSetter(1, agedLTX(1, 1, uncompacted))
	if err := db.EnforceL0RetentionByTime(ctx); err != nil {
		t.Fatal(err)
	}
	lists = client.l1Lists()
	full, items = l1ListStats(lists)
	t.Logf("after compaction cache update: l1ListCalls=%d l1ItemsIterated=%d fullL1Lists=%d",
		len(lists), items, full)
	if len(lists) != 1 || items != l1Files {
		t.Fatalf("L1 listed again after compaction updated the cache: %+v", lists)
	}
	if client.has(0, uncompacted, uncompacted) {
		t.Fatal("L0 file covered by the new L1 file was kept")
	}
	if !client.has(0, next, next) {
		t.Fatal("deleted L0 file past the new L1 max")
	}
}

// TestDB_EnforceL0RetentionByTime_KeepsUncompactedL0 checks the safety property
// directly: L0 files newer than the real L1 max stay, including on a second
// check that reuses the cache.
func TestDB_EnforceL0RetentionByTime_KeepsUncompactedL0(t *testing.T) {
	client := newCountingLTXClient()
	client.add(agedLTX(1, 1, 5))
	for tx := ltx.TXID(1); tx <= 10; tx++ {
		client.add(agedLTX(0, tx, tx))
	}

	db := openRetentionDB(t, client)
	db.Replica.SetPos(ltx.Pos{TXID: 10})

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := db.EnforceL0RetentionByTime(ctx); err != nil {
			t.Fatalf("check %d: %v", i, err)
		}
	}

	for tx := ltx.TXID(1); tx <= 5; tx++ {
		if client.has(0, tx, tx) {
			t.Fatalf("compacted L0 %s was kept", tx)
		}
	}
	for tx := ltx.TXID(6); tx <= 10; tx++ {
		if !client.has(0, tx, tx) {
			t.Fatalf("uncompacted L0 %s was deleted", tx)
		}
	}
}

// TestDB_EnforceL0RetentionByTime_DiscardsL1CacheAheadOfReplica ensures a
// cached L1 TXID past the replicated position cannot delete L0 files that L1
// does not actually cover.
func TestDB_EnforceL0RetentionByTime_DiscardsL1CacheAheadOfReplica(t *testing.T) {
	client := newCountingLTXClient()
	client.add(agedLTX(1, 1, 5))
	for tx := ltx.TXID(1); tx <= 10; tx++ {
		client.add(agedLTX(0, tx, tx))
	}

	db := openRetentionDB(t, client)
	db.Replica.SetPos(ltx.Pos{TXID: 10})
	db.compactor.CacheSetter(1, agedLTX(1, 1, 100))

	if err := db.EnforceL0RetentionByTime(context.Background()); err != nil {
		t.Fatal(err)
	}

	lists := client.l1Lists()
	full, items := l1ListStats(lists)
	t.Logf("ahead cache: l1ListCalls=%d l1ItemsIterated=%d fullL1Lists=%d", len(lists), items, full)
	if len(lists) != 1 || full != 1 || items != 1 {
		t.Fatalf("L1 listings=%+v, want one full listing after discarding the cached TXID", lists)
	}
	for tx := ltx.TXID(1); tx <= 5; tx++ {
		if client.has(0, tx, tx) {
			t.Fatalf("compacted L0 %s was kept", tx)
		}
	}
	for tx := ltx.TXID(6); tx <= 10; tx++ {
		if !client.has(0, tx, tx) {
			t.Fatalf("uncompacted L0 %s was deleted using a cached L1 TXID of 100", tx)
		}
	}
}

// TestDB_EnforceL0RetentionByTime_RelistsAfterCachedL1Deleted removes the
// cached L1 file through level retention and requires the next L0 check to
// list again. The L0 files that only the deleted L1 file covered must stay.
func TestDB_EnforceL0RetentionByTime_RelistsAfterCachedL1Deleted(t *testing.T) {
	client := newCountingLTXClient()
	// The cached file has the higher max TXID but a lower min TXID, so a
	// sorted listing keeps the other file and removes this one.
	client.add(agedLTX(1, 1, 100))
	client.add(agedLTX(1, 50, 60))
	for tx := ltx.TXID(1); tx <= 70; tx++ {
		client.add(agedLTX(0, tx, tx))
	}

	db := openRetentionDB(t, client)
	db.Replica.SetPos(ltx.Pos{TXID: 100})
	db.compactor.CacheSetter(1, agedLTX(1, 1, 100))

	ctx := context.Background()
	if err := db.EnforceRetentionByTXID(ctx, 1, 200); err != nil {
		t.Fatal(err)
	}
	if client.has(1, 1, 100) {
		t.Fatal("cached L1 file was not deleted")
	}
	if !client.has(1, 50, 60) {
		t.Fatal("remaining L1 file was deleted")
	}
	before := len(client.l1Lists())

	if err := db.EnforceL0RetentionByTime(ctx); err != nil {
		t.Fatal(err)
	}
	if got := len(client.l1Lists()); got != before+1 {
		t.Fatalf("L1 listings=%d, want a fresh listing after the cached file was removed (before=%d)", got, before)
	}
	for tx := ltx.TXID(1); tx <= 60; tx++ {
		if client.has(0, tx, tx) {
			t.Fatalf("L0 %s covered by the remaining L1 file was kept", tx)
		}
	}
	for tx := ltx.TXID(61); tx <= 70; tx++ {
		if !client.has(0, tx, tx) {
			t.Fatalf("L0 %s was deleted after its L1 file was removed", tx)
		}
	}
}

func TestDB_EnforceL0RetentionByTime_RelistsAfterReset(t *testing.T) {
	client := newCountingLTXClient()
	for i := 1; i <= 4; i++ {
		tx := ltx.TXID(i)
		client.add(agedLTX(1, tx, tx))
	}
	client.add(agedLTX(0, 1, 1))
	client.add(agedLTX(0, 5, 5))

	db := openRetentionDB(t, client)
	db.Replica.SetPos(ltx.Pos{TXID: 5})

	ctx := context.Background()
	if err := db.EnforceL0RetentionByTime(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.ResetLocalState(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.EnforceL0RetentionByTime(ctx); err != nil {
		t.Fatal(err)
	}

	lists := client.l1Lists()
	full, items := l1ListStats(lists)
	t.Logf("after reset: l1ListCalls=%d l1ItemsIterated=%d fullL1Lists=%d", len(lists), items, full)
	if len(lists) != 2 || full != 2 || items != 8 {
		t.Fatalf("L1 listings=%+v, want two full listings of 4 files", lists)
	}
	if !client.has(0, 5, 5) {
		t.Fatal("deleted L0 file that is not compacted into L1")
	}
}

// gateLTXClient blocks the first listing of one level so a test can update
// the max-file cache while that listing is in flight.
type gateLTXClient struct {
	*countingLTXClient
	level   int
	started chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (c *gateLTXClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	if level == c.level {
		c.once.Do(func() { close(c.started) })
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.release:
		}
	}
	return c.countingLTXClient.LTXFiles(ctx, level, seek, useMetadata)
}

// TestDB_MaxLTXFileInfo_DelayedListKeepsCompactionTXID is the regression test
// for a cache fill that finishes after compaction. The listing observes TXID
// 100, compaction records 200 while the listing is blocked, and the fill must
// not replace 200. The next compaction seeks from the cached max.
func TestDB_MaxLTXFileInfo_DelayedListKeepsCompactionTXID(t *testing.T) {
	inner := newCountingLTXClient()
	inner.add(agedLTX(1, 1, 100))
	release := make(chan struct{})
	client := &gateLTXClient{
		countingLTXClient: inner,
		level:             1,
		started:           make(chan struct{}),
		release:           release,
	}
	db := openRetentionDB(t, client)

	type result struct {
		info ltx.FileInfo
		err  error
	}
	done := make(chan result, 1)
	go func() {
		info, err := db.compactor.MaxLTXFileInfo(context.Background(), 1)
		done <- result{info: info, err: err}
	}()

	select {
	case <-client.started:
	case <-time.After(5 * time.Second):
		t.Fatal("level 1 listing did not start")
	}

	// Compaction records the L1 file it just wrote.
	db.compactor.CacheSetter(1, agedLTX(1, 1, 200))
	close(release)

	got := <-done
	if got.err != nil {
		t.Fatal(got.err)
	}
	cached, ok := db.cachedMaxLTXFileInfo(1)
	if !ok {
		t.Fatal("cache empty after listing")
	}
	if cached.MaxTXID != 200 {
		t.Fatalf("cached L1 max=%s, want 200 (listing observed 100)", cached.MaxTXID)
	}
	if got.info.MaxTXID != 200 {
		t.Fatalf("MaxLTXFileInfo=%s, want 200", got.info.MaxTXID)
	}
}

// TestDB_MaxLTXFileInfo_DelayedListRespectsReset keeps a listing that started
// before ResetLocalState from refilling the cache afterward.
func TestDB_MaxLTXFileInfo_DelayedListRespectsReset(t *testing.T) {
	inner := newCountingLTXClient()
	inner.add(agedLTX(1, 1, 100))
	release := make(chan struct{})
	client := &gateLTXClient{
		countingLTXClient: inner,
		level:             1,
		started:           make(chan struct{}),
		release:           release,
	}
	db := openRetentionDB(t, client)

	done := make(chan error, 1)
	go func() {
		_, err := db.compactor.MaxLTXFileInfo(context.Background(), 1)
		done <- err
	}()

	select {
	case <-client.started:
	case <-time.After(5 * time.Second):
		t.Fatal("level 1 listing did not start")
	}

	if err := db.ResetLocalState(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, ok := db.cachedMaxLTXFileInfo(1); ok {
		t.Fatal("listing that started before reset refilled the L1 cache")
	}
}
