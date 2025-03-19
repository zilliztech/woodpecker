package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/woodpecker/mocks/mocks_server/mocks_storage"

	"github.com/zilliztech/woodpecker/server/storage"
)

func setupMockFragment(t *testing.T, id int64, key string, size int64, lastEntryId int64) storage.Fragment {
	fragment := mocks_storage.NewFragment(t)

	// Setup mock methods - using On() instead of EXPECT() to match mockery's pattern
	fragment.On("GetFragmentId").Return(id).Maybe()
	fragment.On("GetFragmentKey").Return(key).Maybe()
	fragment.On("GetSize").Return(size).Maybe()
	fragment.On("GetLastEntryIdDirectly").Return(lastEntryId).Maybe()
	fragment.On("Release").Return(nil).Maybe()

	return fragment
}

func TestFragmentManager_AddFragment(t *testing.T) {
	fm := newFragmentManager(100)

	fragment := setupMockFragment(t, 0, "0.frag", 20, 10)

	err := fm.AddFragment(context.TODO(), fragment)
	assert.NoError(t, err)
	assert.Equal(t, int64(20), fm.GetUsedMemory())
}

func TestFragmentManager_RemoveFragment(t *testing.T) {
	fm := newFragmentManager(100)

	fragment := setupMockFragment(t, 0, "0.frag", 20, 10)

	err := fm.AddFragment(context.TODO(), fragment)
	assert.NoError(t, err)
	err = fm.RemoveFragment(context.TODO(), fragment)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), fm.GetUsedMemory())
}

func TestFragmentManager_EvictFragments(t *testing.T) {
	fm := newFragmentManager(40)

	fragment1 := setupMockFragment(t, 0, "0.frag", 20, 10)
	fragment2 := setupMockFragment(t, 1, "1.frag", 30, 20)

	err := fm.AddFragment(context.TODO(), fragment1)
	assert.NoError(t, err)
	err = fm.AddFragment(context.TODO(), fragment2)
	assert.NoError(t, err)
	assert.Equal(t, int64(50), fm.GetUsedMemory())

	err = fm.EvictFragments()
	assert.NoError(t, err)
	assert.Equal(t, int64(30), fm.GetUsedMemory())
}

func TestFragmentManager(t *testing.T) {
	fm := newFragmentManager(100)

	fragment1 := setupMockFragment(t, 0, "0.frag", 20, 20)
	fragment2 := setupMockFragment(t, 1, "1.frag", 30, 50)
	fragment3 := setupMockFragment(t, 2, "2.frag", 40, 90)

	err := fm.AddFragment(context.TODO(), fragment1)
	assert.NoError(t, err)
	err = fm.AddFragment(context.TODO(), fragment2)
	assert.NoError(t, err)
	err = fm.AddFragment(context.TODO(), fragment3)
	assert.NoError(t, err)

	assert.Equal(t, int64(90), fm.GetUsedMemory())

	// remove manually
	err = fm.RemoveFragment(context.TODO(), fragment2)
	assert.NoError(t, err)
	assert.Equal(t, int64(60), fm.GetUsedMemory())

	// remove automatically
	err = fm.StartEvictionLoop(1 * time.Second)
	assert.NoError(t, err)
	fragment4 := setupMockFragment(t, 3, "3.frag", 80, 170)
	err = fm.AddFragment(context.TODO(), fragment4)
	assert.NoError(t, err)
	time.Sleep(3 * time.Second)
	err = fm.StopEvictionLoop()
	assert.NoError(t, err)
	assert.Equal(t, int64(80), fm.GetUsedMemory())
}
