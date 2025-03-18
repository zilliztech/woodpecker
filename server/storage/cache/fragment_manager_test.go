package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/server/storage/objectstorage"
)

func TestFragmentManager_AddFragment(t *testing.T) {
	fm := newFragmentManager(100)

	data := [][]byte{
		make([]byte, 10),
		make([]byte, 10),
	}
	fragment := objectstorage.NewFragmentObject(nil, "test-bucket", 0, "0.frag", data, 0, true, false, true)

	err := fm.AddFragment(context.TODO(), fragment)
	assert.NoError(t, err)
	assert.Equal(t, int64(20)+2*8, fm.GetUsedMemory())
}

func TestFragmentManager_RemoveFragment(t *testing.T) {
	fm := newFragmentManager(100)

	data := [][]byte{
		make([]byte, 10),
		make([]byte, 10),
	}
	fragment := objectstorage.NewFragmentObject(nil, "test-bucket", 0, "0.frag", data, 0, true, false, true)

	err := fm.AddFragment(context.TODO(), fragment)
	assert.NoError(t, err)
	err = fm.RemoveFragment(context.TODO(), fragment)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), fm.GetUsedMemory())
}

func TestFragmentManager_EvictFragments(t *testing.T) {
	fm := newFragmentManager(50)

	data := [][]byte{
		make([]byte, 10),
		make([]byte, 10),
	}
	fragment1 := objectstorage.NewFragmentObject(nil, "test-bucket", 0, "0.frag", data, 0, true, false, true)
	fragment2 := objectstorage.NewFragmentObject(nil, "test-bucket", 1, "1.frag", data, 20, true, false, true)

	err := fm.AddFragment(context.TODO(), fragment1)
	assert.NoError(t, err)
	err = fm.AddFragment(context.TODO(), fragment2)
	assert.NoError(t, err)
	assert.Equal(t, int64(40)+4*8, fm.GetUsedMemory())

	err = fm.EvictFragments()
	assert.NoError(t, err)
	assert.Equal(t, int64(20)+2*8, fm.GetUsedMemory())
}

func TestFragmentManager(t *testing.T) {
	fm := newFragmentManager(100)

	data20 := [][]byte{
		make([]byte, 10),
		make([]byte, 10),
	}
	data30 := [][]byte{
		make([]byte, 10),
		make([]byte, 10),
		make([]byte, 10),
	}
	data40 := [][]byte{
		make([]byte, 10),
		make([]byte, 10),
		make([]byte, 10),
		make([]byte, 10),
	}
	fragment1 := objectstorage.NewFragmentObject(nil, "test-bucket", 0, "0.frag", data20, 0, true, false, true)
	fragment2 := objectstorage.NewFragmentObject(nil, "test-bucket", 1, "1.frag", data30, 20, true, false, true)
	fragment3 := objectstorage.NewFragmentObject(nil, "test-bucket", 2, "2.frag", data40, 50, true, false, true)

	err := fm.AddFragment(context.TODO(), fragment1)
	assert.NoError(t, err)
	err = fm.AddFragment(context.TODO(), fragment2)
	assert.NoError(t, err)
	err = fm.AddFragment(context.TODO(), fragment3)
	assert.NoError(t, err)

	assert.Equal(t, int64(90)+9*8, fm.GetUsedMemory())

	// remove manually
	err = fm.RemoveFragment(context.TODO(), fragment2)
	assert.NoError(t, err)
	assert.Equal(t, int64(60)+6*8, fm.GetUsedMemory())

	// remove automatically
	err = fm.StartEvictionLoop(1 * time.Second)
	assert.NoError(t, err)
	time.Sleep(3 * time.Second)
	err = fm.StopEvictionLoop()
	assert.NoError(t, err)
	assert.Equal(t, int64(40)+4*8, fm.GetUsedMemory())

}
