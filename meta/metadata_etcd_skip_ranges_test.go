package meta

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/proto"
)

func setupSkipRangeTest(t *testing.T) MetadataProvider {
	t.Helper()
	etcdCli, err := etcd.GetEtcdClient(true, false, []string{}, "", "", "", "")
	require.NoError(t, err)
	_, err = etcdCli.Delete(context.Background(), LegacyServicePrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	provider := NewMetadataProvider(context.Background(), etcdCli, testMetaCfg(t))
	require.NoError(t, provider.InitIfNecessary(context.Background()))
	return provider
}

func rangesFor(logID, segID int64, from, to int64) *proto.AllSkipRanges {
	return &proto.AllSkipRanges{ByLogId: map[int64]*proto.LogSkipRanges{
		logID: {BySegmentId: map[int64]*proto.SegmentSkipRanges{
			segID: {Ranges: []*proto.SkipRange{{FromEntryId: from, ToEntryId: to, Reason: "test"}}},
		}},
	}}
}

// testSkipRangesAbsentRecordReadsAsEmpty covers the state every cluster is in until an
// operator declares a range. Returning an error for it would make the read path log a
// failure on a cluster where nothing is wrong, which is how a real signal gets ignored.
func testSkipRangesAbsentRecordReadsAsEmpty(t *testing.T) {
	provider := setupSkipRangeTest(t)

	set, err := provider.GetAllSkipRanges(context.Background())

	require.NoError(t, err)
	require.NotNil(t, set)
	require.Empty(t, set.Metadata.GetByLogId())
	require.Zero(t, set.Revision, "an absent record has no revision, which is what the write path guards on")
	require.Nil(t, set.For(7), "a log with no ranges answers nil, not an empty map")
}

// testSkipRangesAccessorIsNilSafeAtEveryLevel pins what the accessor may be called on. Three
// things can be nil -- the wrapper, the message inside it, and the map inside that -- and the
// reader calls this on a record most clusters never have, so it has to answer nil rather than
// panic at each of them.
func testSkipRangesAccessorIsNilSafeAtEveryLevel(t *testing.T) {
	var nilWrapper *AllSkipRanges
	require.Nil(t, nilWrapper.For(7), "a nil wrapper")

	require.Nil(t, (&AllSkipRanges{}).For(7), "a nil proto message inside the wrapper")

	require.Nil(t, (&AllSkipRanges{Metadata: &proto.AllSkipRanges{}}).For(7), "a nil map inside the message")

	// And a log the record does not mention, which is the common case.
	held := &AllSkipRanges{Metadata: rangesFor(7, 3, 10, 19)}
	require.Nil(t, held.For(8))
	require.NotNil(t, held.For(7))
	require.Nil(t, held.For(7).GetBySegmentId()[4], "a segment the log does not mention")
	require.Empty(t, held.For(8).GetBySegmentId()[3].GetRanges(),
		"the whole chain answers empty rather than panicking")
}

// testSkipRangesRoundTripsByLogAndSegment is the lookup the reader depends on: two map
// probes, no scan, and the segment id carried as the map key rather than as a field.
func testSkipRangesRoundTripsByLogAndSegment(t *testing.T) {
	provider := setupSkipRangeTest(t)
	ctx := context.Background()

	read, err := provider.GetAllSkipRanges(ctx)
	require.NoError(t, err)
	read.Metadata = rangesFor(7, 3, 10, 19)
	require.NoError(t, provider.UpdateAllSkipRanges(ctx, read))

	back, err := provider.GetAllSkipRanges(ctx)
	require.NoError(t, err)
	seg := back.For(7).GetBySegmentId()[3]
	require.NotNil(t, seg)
	require.Len(t, seg.Ranges, 1)
	require.EqualValues(t, 10, seg.Ranges[0].FromEntryId)
	require.EqualValues(t, 19, seg.Ranges[0].ToEntryId)
	require.Nil(t, back.For(7).GetBySegmentId()[4], "another segment of the same log holds nothing")
	require.Nil(t, back.For(8), "another log holds nothing")
	require.NotZero(t, back.Revision)
}

// testSkipRangesUndecodableRecordIsAnError covers a record that cannot be parsed. Reading it as
// empty would be worse than failing: every reader would quietly stop skipping, and the operator
// who declared the ranges would see readers hang again with nothing saying why.
func testSkipRangesUndecodableRecordIsAnError(t *testing.T) {
	provider := setupSkipRangeTest(t)
	ctx := context.Background()
	etcdProvider, ok := provider.(*metadataProviderEtcd)
	require.True(t, ok)

	_, err := etcdProvider.client.Put(ctx, etcdProvider.keyBuilder.AllSkipRangesKey(), "not a proto")
	require.NoError(t, err)

	_, err = provider.GetAllSkipRanges(ctx)
	require.Error(t, err, "a record that cannot be parsed is not a record with nothing in it")
}

// testSkipRangesCachedReadAnswersFromTheCacheNotFromEtcd is the property the read path depends on.
// The state that makes a reader ask -- a position that has not moved since its last report -- is
// also what a reader that has simply caught up with the tail of its log looks like, so this runs
// for every parked reader. Reading inline put an etcd round trip inside ReadNext and showed up as a
// second of tail-read lag in the stability suite.
//
// Asserted by what comes back rather than by how long it took: a stale entry that disagrees with
// etcd is answered from the entry, which an implementation that read inline could not do. A timing
// assertion would pass against an inline read whenever etcd happened to be quick.
func testSkipRangesCachedReadAnswersFromTheCacheNotFromEtcd(t *testing.T) {
	provider := setupSkipRangeTest(t)
	ctx := context.Background()
	etcdProvider, ok := provider.(*metadataProviderEtcd)
	require.True(t, ok)

	// etcd holds nothing; the stale entry holds a range. They disagree on purpose.
	etcdProvider.skipRangeCache.Store(&cachedSkipRanges{
		set:    &AllSkipRanges{Metadata: rangesFor(7, 3, 10, 19)},
		readAt: time.Now().Add(-2 * SkipRangeCacheTTL),
	})

	require.NotNil(t, provider.GetAllSkipRangesCached(ctx).For(7),
		"the caller is answered from the cache, so it cannot have waited for etcd")

	// And the refresh it started does land, so the staleness is bounded rather than permanent.
	require.Eventually(t, func() bool { return provider.GetAllSkipRangesCached(ctx).For(7) == nil },
		2*time.Second, 20*time.Millisecond, "the background refresh replaces what was held")
}

// testSkipRangesRefreshIsSingleFlighted covers what makes "answer from the cache" cheap. Every
// parked reader asks on its own report tick, so a burst of them all seeing the same stale entry
// would each pay for a read of the same record if nothing held them to one.
func testSkipRangesRefreshIsSingleFlighted(t *testing.T) {
	provider := setupSkipRangeTest(t)
	ctx := context.Background()
	etcdProvider, ok := provider.(*metadataProviderEtcd)
	require.True(t, ok)

	// Stand in for a refresh already running, deterministically.
	require.True(t, etcdProvider.skipRangeRefreshing.CompareAndSwap(false, true))
	require.False(t, etcdProvider.refreshSkipRangesInBackground(ctx),
		"a second asker joins the refresh in flight rather than starting another")

	etcdProvider.skipRangeRefreshing.Store(false)
	require.True(t, etcdProvider.refreshSkipRangesInBackground(ctx),
		"and once it is done the next asker does start one")
}

// testSkipRangesCachedReadHoldsForItsWindow covers the window. A reader consults this while it is
// making no progress, which can be every report tick and for many readers per log, so the answer is
// shared for a few seconds rather than re-read each time.
func testSkipRangesCachedReadHoldsForItsWindow(t *testing.T) {
	provider := setupSkipRangeTest(t)
	ctx := context.Background()

	// Prime the cache and let the first refresh land.
	provider.GetAllSkipRangesCached(ctx)
	require.Eventually(t, func() bool {
		return provider.GetAllSkipRangesCached(ctx) != nil
	}, 2*time.Second, 20*time.Millisecond)

	// Write a range straight through the uncached path, so only the cache can hide it.
	write, err := provider.GetAllSkipRanges(ctx)
	require.NoError(t, err)
	write.Metadata = rangesFor(7, 3, 10, 19)
	require.NoError(t, provider.UpdateAllSkipRanges(ctx, write))

	require.Nil(t, provider.GetAllSkipRangesCached(ctx).For(7),
		"within the window the reader keeps the answer it already had")
	uncached, err := provider.GetAllSkipRanges(ctx)
	require.NoError(t, err)
	require.NotNil(t, uncached.For(7), "and the uncached read sees it, so the write really did land")
}

// testSkipRangesCachedReadSurvivesAnUnreadableRecord covers what a reader does when the record
// cannot be read: it keeps behaving as it did. Answering "no ranges" on a failed read would make a
// reader that had been skipping stop, over a record most clusters never have.
func testSkipRangesCachedReadSurvivesAnUnreadableRecord(t *testing.T) {
	provider := setupSkipRangeTest(t)
	ctx := context.Background()
	etcdProvider, ok := provider.(*metadataProviderEtcd)
	require.True(t, ok)

	write, err := provider.GetAllSkipRanges(ctx)
	require.NoError(t, err)
	write.Metadata = rangesFor(7, 3, 10, 19)
	require.NoError(t, provider.UpdateAllSkipRanges(ctx, write))
	require.Eventually(t, func() bool { return provider.GetAllSkipRangesCached(ctx).For(7) != nil },
		2*time.Second, 20*time.Millisecond, "the background refresh picks the range up")

	// Make the record unparseable and age the entry, so the next refresh fails.
	_, err = etcdProvider.client.Put(ctx, etcdProvider.keyBuilder.AllSkipRangesKey(), "not a proto")
	require.NoError(t, err)
	held := etcdProvider.skipRangeCache.Load()
	require.NotNil(t, held)
	etcdProvider.skipRangeCache.Store(&cachedSkipRanges{
		set: held.set, readAt: time.Now().Add(-2 * SkipRangeCacheTTL),
	})

	require.NotNil(t, provider.GetAllSkipRangesCached(ctx).For(7),
		"a refresh that fails leaves the reader with what it had")
	time.Sleep(200 * time.Millisecond) // let the failing refresh finish
	require.NotNil(t, provider.GetAllSkipRangesCached(ctx).For(7),
		"and it stays that way rather than being cleared")
}

// testSkipRangesStaleWriteIsRefused is what keeps two operators from losing each
// other's work: the record is one key for the whole root, so a blind write would drop
// every range someone else added since the read.
func testSkipRangesStaleWriteIsRefused(t *testing.T) {
	provider := setupSkipRangeTest(t)
	ctx := context.Background()

	first, err := provider.GetAllSkipRanges(ctx)
	require.NoError(t, err)
	stale, err := provider.GetAllSkipRanges(ctx)
	require.NoError(t, err)

	first.Metadata = rangesFor(7, 3, 10, 19)
	require.NoError(t, provider.UpdateAllSkipRanges(ctx, first))

	stale.Metadata = rangesFor(8, 1, 0, 5)
	require.Error(t, provider.UpdateAllSkipRanges(ctx, stale), "the record moved since it was read")

	// Re-reading and writing again succeeds, and keeps what the first writer added.
	fresh, err := provider.GetAllSkipRanges(ctx)
	require.NoError(t, err)
	fresh.Metadata.ByLogId[8] = rangesFor(8, 1, 0, 5).ByLogId[8]
	require.NoError(t, provider.UpdateAllSkipRanges(ctx, fresh))

	back, err := provider.GetAllSkipRanges(ctx)
	require.NoError(t, err)
	require.NotNil(t, back.For(7), "the first writer's ranges survived")
	require.NotNil(t, back.For(8))
}

// testSkipRangesFirstWriteRefusesIfSomeoneElseCreatedIt covers the revision-zero case
// separately. A record that did not exist when it was read must still not exist, or the
// second writer silently replaces the first writer's whole record.
func testSkipRangesFirstWriteRefusesIfSomeoneElseCreatedIt(t *testing.T) {
	provider := setupSkipRangeTest(t)
	ctx := context.Background()

	a, err := provider.GetAllSkipRanges(ctx)
	require.NoError(t, err)
	b, err := provider.GetAllSkipRanges(ctx)
	require.NoError(t, err)
	require.Zero(t, a.Revision)
	require.Zero(t, b.Revision)

	a.Metadata = rangesFor(7, 3, 10, 19)
	require.NoError(t, provider.UpdateAllSkipRanges(ctx, a))

	b.Metadata = rangesFor(8, 1, 0, 5)
	require.Error(t, provider.UpdateAllSkipRanges(ctx, b),
		"the record exists now, so this write is no longer a creation")
}

// testSkipRangesOversizedRecordIsRefused bounds the one record. reason is free text, so
// a count limit would not hold; the refusal names the size so an operator knows what to
// remove rather than guessing.
func testSkipRangesOversizedRecordIsRefused(t *testing.T) {
	provider := setupSkipRangeTest(t)
	ctx := context.Background()

	set, err := provider.GetAllSkipRanges(ctx)
	require.NoError(t, err)
	set.Metadata = &proto.AllSkipRanges{ByLogId: map[int64]*proto.LogSkipRanges{}}
	huge := strings.Repeat("x", MaxSkipRangeReasonBytes)
	for logID := int64(0); logID < 4000; logID++ {
		set.Metadata.ByLogId[logID] = &proto.LogSkipRanges{
			BySegmentId: map[int64]*proto.SegmentSkipRanges{
				0: {Ranges: []*proto.SkipRange{{FromEntryId: 0, ToEntryId: 9, Reason: huge}}},
			},
		}
	}

	err = provider.UpdateAllSkipRanges(ctx, set)

	require.Error(t, err)
	require.Contains(t, err.Error(), "over the")
	require.Contains(t, err.Error(), "limit")
}
