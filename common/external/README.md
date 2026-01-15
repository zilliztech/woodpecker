# Arrow FileSystem C Interface Documentation

This document describes how to use Woodpecker to unify the access of various types of cloud storage via the external dependency milvus-storage.

## Build the C++ Library

First, you need to build the C++ parts of the code:

```bash
cd /path/to/woodpecker
make build-external
```

After building, the library files will be generated in the `external/output/lib/` directory.

## Environment Variable Configuration

Before running Go tests, you need to set up the following environment variables:

```bash
export PKG_CONFIG_PATH=/path/to/woodpecker/external/output/lib/pkgconfig
export CGO_LDFLAGS="-Wl,-rpath,/path/to/woodpecker/external/output/lib"
```

Or set them directly when running tests:

```bash
PKG_CONFIG_PATH=/path/to/woodpecker/external/output/lib/pkgconfig \
CGO_LDFLAGS="-Wl,-rpath,/path/to/woodpecker/external/output/lib" \
go test -v ./common/initcore -run TestStorageV2LocalFS
```

## Go API Usage

### Initializing the File System

#### Local File System

```go
import "github.com/zilliztech/woodpecker/common/initcore"

// Initialize local file system
err := initcore.InitLocalArrowFileSystem("/tmp/storage")
if err != nil {
    log.Fatal(err)
}
defer initcore.CleanFileSystem()
```

#### Remote File System (MinIO/S3)

```go
import (
    "github.com/zilliztech/woodpecker/common/config"
    "github.com/zilliztech/woodpecker/common/initcore"
)

// Load configuration
cfg, err := config.NewConfiguration("config/woodpecker.yaml")
if err != nil {
    log.Fatal(err)
}

// Configure MinIO settings
cfg.Woodpecker.Storage.Type = "minio"
cfg.Minio.Address = "localhost:9000"
cfg.Minio.BucketName = "my-bucket"
cfg.Minio.AccessKeyID = "minioadmin"
cfg.Minio.SecretAccessKey = "minioadmin"

// Initialize
err = initcore.InitStorageV2FileSystem(cfg)
if err != nil {
    log.Fatal(err)
}
defer initcore.CleanFileSystem()
```

### File Operations

#### Write File

```go
data := []byte("Hello, Woodpecker!")
metadata := map[string]string{
    "author": "test_user",
    "version": "1.0",
}

err := initcore.WriteFile("test.txt", data, metadata)
if err != nil {
    log.Fatal(err)
}
```

#### Read File

```go
data, err := initcore.ReadFile("test.txt")
if err != nil {
    log.Fatal(err)
}
fmt.Printf("File content: %s\n", string(data))
```

#### Get File Info

```go
info, err := initcore.GetFileInfo("test.txt")
if err != nil {
    log.Fatal(err)
}
fmt.Printf("File size: %d bytes\n", info.Size)
fmt.Printf("Metadata: %v\n", info.Metadata)
```

#### Get File Size

```go
size, err := initcore.GetFileSize("test.txt")
if err != nil {
    log.Fatal(err)
}
fmt.Printf("File size: %d bytes\n", size)
```

#### Check File Existence

```go
exists, err := initcore.FileExists("test.txt")
if err != nil {
    log.Fatal(err)
}
if exists {
    fmt.Println("File exists")
}
```

#### Delete File

```go
err := initcore.DeleteFile("test.txt")
if err != nil {
    log.Fatal(err)
}
```

## Running Tests

### Local File System Tests

```bash
cd /path/to/woodpecker
PKG_CONFIG_PATH=external/output/lib/pkgconfig \
CGO_LDFLAGS="-Wl,-rpath,$(pwd)/external/output/lib" \
go test -v ./common/initcore -run TestStorageV2LocalFS
```

### Remote File System Tests (MinIO)

First, start the MinIO server:

```bash
# Start MinIO with Docker
docker run -d \
  -p 9000:9000 \
  -p 9001:9001 \
  --name minio \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  quay.io/minio/minio server /data --console-address ":9001"

# Wait for MinIO to start
sleep 5

# Create a test bucket
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio mc mb local/woodpecker-test
```

Then run the test:

```bash
PKG_CONFIG_PATH=external/output/lib/pkgconfig \
CGO_LDFLAGS="-Wl,-rpath,$(pwd)/external/output/lib" \
go test -v ./common/initcore -run TestStorageV2RemoteFS
```

If MinIO is not available, you can skip the remote test:

```bash
SKIP_MINIO_TESTS=1 go test -v ./common/initcore -run TestStorageV2RemoteFS
```

## C Interface Overview

The underlying C interfaces are defined in `external/src/segcore/arrow_fs_c.h`:

- `InitLocalArrowFileSystemSingleton(const char* c_path)` - Initialize local file system
- `InitRemoteArrowFileSystemSingleton(CStorageConfig c_storage_config)` - Initialize remote file system
- `CleanArrowFileSystemSingleton()` - Clean up the file system
- `GetFileInfo(path, out_size, out_keys, out_values, out_count)` - Get file information
- `ReadFileData(path, out_data, out_size)` - Read file data
- `WriteFileData(path, data, data_size, metadata_keys, metadata_values, metadata_count)` - Write file
- `DeleteFile(path)` - Delete file
- `FreeMemory(ptr)` - Free memory

## Notes

1. **Memory Management:** Memory allocated by the C interface will be automatically released. There's no need to manually call `FreeMemory` in the Go code.
2. **Thread Safety:** The file system singleton is thread-safe in multi-threaded environments.
3. **Metadata Support:** The metadata writing feature is reserved and may not take effect in the current version.
4. **Error Handling:** All functions return errors. Make sure to handle them appropriately.
5. **Path Handling:** Paths are relative to the root path specified during file system initialization.

## Troubleshooting

### Build Error: milvus_core.pc not found

Make sure you have run `make build-cpp` and configured the correct `PKG_CONFIG_PATH`.

### Runtime Error: Library not loaded

Make sure you have correctly set `CGO_LDFLAGS` for rpath, or set `DYLD_LIBRARY_PATH` (macOS) / `LD_LIBRARY_PATH` (Linux) as appropriate.

### Test Failure: MinIO connection refused

Confirm that your MinIO server is up and running, and that the address and port in your configuration are correct.
