package meta

import (
	"context"
	"strings"
	"testing"

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
