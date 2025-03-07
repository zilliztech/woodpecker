package benchmark

//
//func TestNewObjectStorageFragment(t *testing.T) {
//	bucket := "test-bucket"
//	fragmentId := uint64(1)
//	fragmentKey := "test-key"
//	entries := [][]byte{[]byte("data1"), []byte("data2"), []byte("data3")}
//	firstEntryId := int64(1)
//
//	cfg, err := config.NewConfiguration()
//	assert.NoError(t, err)
//	cfg.Minio.BucketName = bucket
//	client, err := minioHandler.NewMinioHandler(context.Background(), cfg)
//
//	assert.NoError(t, err)
//	fragment := objectstorage.NewFragmentObject(client, bucket, fragmentId, fragmentKey, entries, firstEntryId, true, false)
//	assert.NotNil(t, fragment)
//	assert.Equal(t, client, fragment.client)
//	assert.Equal(t, bucket, fragment.bucket)
//	assert.Equal(t, fragmentId, fragment.fragmentId)
//	assert.Equal(t, fragmentKey, fragment.fragmentKey)
//	assert.Equal(t, entries[0], fragment.entriesData[:len(entries[0])])
//	assert.Equal(t, entries[1], fragment.entriesData[len(entries[0]):len(entries[0])+len(entries[1])])
//	assert.Equal(t, entries[2], fragment.entriesData[len(entries[0])+len(entries[1]):])
//	assert.Equal(t, firstEntryId, fragment.firstEntryId)
//	assert.Equal(t, firstEntryId+int64(len(entries))-1, fragment.lastEntryId)
//	assert.True(t, fragment.loaded)
//	assert.False(t, fragment.uploaded)
//}
//
//func TestWriteAndLoad(t *testing.T) {
//	bucket := "test-bucket"
//	fragmentId := uint64(1)
//	fragmentKey := "test-key"
//	entries := [][]byte{[]byte("data1"), []byte("data2"), []byte("data3")}
//	firstEntryId := int64(1)
//
//	cfg, err := config.NewConfiguration()
//	assert.NoError(t, err)
//	cfg.Minio.BucketName = bucket
//	client, err := minioHandler.NewMinioHandler(context.Background(), cfg)
//	assert.NoError(t, err)
//
//	fragment := objectstorage.NewFragmentObject(client, bucket, fragmentId, fragmentKey, entries, firstEntryId, true, false)
//
//	// Test writing when fragment is not loaded
//	fragment.loaded = false
//	err = fragment.Flush(context.Background())
//	assert.Error(t, err)
//	assert.Equal(t, "fragment is empty", err.Error())
//
//	// Test writing when fragment is loaded
//	fragment.loaded = true
//
//	//
//	err = fragment.Flush(context.Background())
//	assert.NoError(t, err)
//	assert.True(t, fragment.uploaded)
//
//	// Test Load
//	fragment2 := &objectstorage.FragmentObject{
//		client:      client,
//		bucket:      bucket,
//		fragmentId:  fragmentId,
//		fragmentKey: fragmentKey,
//		loaded:      false,
//		uploaded:    true,
//	}
//	err = fragment2.Load(context.Background())
//	assert.NoError(t, err)
//	assert.True(t, fragment2.loaded)
//	assert.Equal(t, firstEntryId, fragment2.firstEntryId)
//	assert.Equal(t, firstEntryId+int64(len(entries))-1, fragment.lastEntryId)
//	assert.Equal(t, []byte("data1data2data3"), fragment2.entriesData)
//	assert.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0, 5}, fragment2.indexes[:8])
//	assert.Equal(t, []byte{0, 0, 0, 5, 0, 0, 0, 5}, fragment2.indexes[8:16])
//	assert.Equal(t, []byte{0, 0, 0, 10, 0, 0, 0, 5}, fragment2.indexes[16:24])
//
//	// test release
//	err = fragment.Release()
//	assert.NoError(t, err)
//	assert.Nil(t, fragment.indexes)
//	assert.Nil(t, fragment.entriesData)
//	assert.False(t, fragment.loaded)
//
//	// test GetEntry
//	entry, err := fragment.GetEntry(1)
//	assert.NoError(t, err)
//	assert.Equal(t, []byte("data1"), entry)
//	entry, err = fragment.GetEntry(2)
//	assert.NoError(t, err)
//	assert.Equal(t, []byte("data2"), entry)
//	entry, err = fragment.GetEntry(3)
//	assert.NoError(t, err)
//	assert.Equal(t, []byte("data3"), entry)
//}
