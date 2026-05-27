# Condition Write Fence Mechanism

## Overview

In embed mode, Woodpecker uses **condition write** as a fence mechanism to prevent split-brain scenarios where a stale primary node resumes writing after being partitioned or crashed. This ensures data consistency and prevents concurrent write conflicts in distributed deployments.

## Supported Object Storage Backends

Most mainstream cloud storage backends natively support condition write (fence) capabilities:

- **MinIO** - Full support for `If-None-Match` header
- **AWS S3** - Supports conditional PUT operations
- **Azure Blob Storage** - Supports conditional writes
- **GCP Cloud Storage** - Supports conditional operations
- **Aliyun OSS** - Supports forbid overwrite feature, Woodpecker wraps it as condition write
- **Tencent COS** - Supports forbid overwrite feature, Woodpecker wraps it as condition write

These backends allow Woodpecker to use optimistic concurrency control without requiring distributed locks.

## Condition Write Modes

For object storage backends that don't support condition write (typically some open-source or self-hosted solutions), Woodpecker can use a **distributed lock** mechanism via explicit configuration.

### Mode Logic

The fallback behavior depends on the `fencePolicy.conditionWrite` configuration:

#### Auto Mode (Default)
- **Legacy Metadata First**: If etcd already stores true or false, Woodpecker follows that cluster decision
- **Strict New Cluster Behavior**: If no metadata exists, Woodpecker verifies condition write support using enable-mode semantics
- **Success Behavior**: Successful verification stores true for the cluster
- **Failure Behavior**: Verification failure fails startup; it does not fall back and does not persist false

#### Enable Mode
- **Fast Path**: Stored true metadata is trusted
- **Ignore Stored False**: Stored false metadata is ignored because the operator explicitly required condition write
- **Strict Requirement**: Condition write verification must succeed within 10 retries
- **Success Behavior**: Successful verification overwrites cluster metadata to true
- **Failure Behavior**: Verification failure fails startup
- **Use Case**: When you're certain your storage backend supports condition write

#### Disable Mode
- **Skip Detection**: Completely skips condition write detection
- **Distributed Lock Mode**: Immediately uses distributed lock mode
- **Use Case**: When you know your storage backend doesn't support condition write

### Detection and Storage Separation

The detection retry logic is separated from etcd storage operations:

1. **Phase 1 - Detection**: Retries storage capability detection up to the configured limit
2. **Phase 2 - Storage**: Independently retries storing the enabled result to etcd (up to 10 times)
3. **Error Handling**: If etcd storage fails after all retries, returns an error rather than silently using local detection result

This separation ensures that etcd failures don't consume detection retry quota, and verification failures are surfaced instead of being persisted as unsupported capability.

## Distributed Lock Implementation

When condition write is explicitly disabled, or when legacy auto metadata stores false, Woodpecker uses distributed locks via etcd to coordinate concurrent writes.

### Lock Mechanism

- **Lock Type**: etcd `concurrency.Mutex` with `concurrency.Session`
- **Session TTL**: 10 seconds (automatically renewed via keep-alive)
- **Lock Acquisition**: Uses `TryLock` for non-blocking lock acquisition
- **Lock Key**: Per-log distributed lock (`/woodpecker/service/lock/{logName}`)

### Failover Behavior

When a primary node fails or becomes partitioned:

1. **Session Expiration**: The etcd session expires after the TTL (10 seconds) if keep-alive fails
2. **Lock Release**: The distributed lock is automatically released when the session expires
3. **New Primary**: A new primary node can acquire the lock after the session expires
4. **Writer Switch**: Applications need to reopen a new writer, which may experience a brief wait time (up to 10 seconds) for the old session to expire

### Session Monitoring

The log writer continuously monitors the etcd session:

- **Session Channel**: Watches `session.Done()` channel for expiration signals
- **Writer Invalidation**: When session expires, the writer is marked as invalid
- **Write Rejection**: Subsequent writes are rejected with `ErrLogWriterLockLost` error
- **Application Handling**: Applications should detect this error and reopen a new writer

### Performance Considerations

**Distributed Lock Mode:**
- **Lock Acquisition Overhead**: Each writer open requires acquiring a distributed lock
- **Failover Latency**: Up to 10 seconds wait time during failover (session TTL)
- **Concurrent Write Limitation**: Only one writer per log can exist at a time

**Condition Write Mode:**
- **No Lock Overhead**: No distributed lock acquisition needed
- **Faster Failover**: Immediate detection of stale writes via condition write failures
- **Better Concurrency**: Multiple writers can attempt writes, but only one succeeds per segment

## Configuration

Configure the fence policy in `woodpecker.yaml`:

```yaml
woodpecker:
  logstore:
    fencePolicy:
      conditionWrite: "auto"  # Options: "auto", "enable", "disable"
```

- **`auto`**: Follow existing cluster metadata if present; otherwise verify condition write support and fail startup if verification fails
- **`enable`**: Require condition write support; ignore stored false metadata, verify support, and fail startup if verification fails
- **`disable`**: Force use of distributed locks, skip detection entirely

## Custom Object Storage Integration

If you're using an open-source or self-hosted object storage that:

1. **Supports prevent-overwrite semantics** (similar to condition write)
2. **Has MinIO-like API compatibility** but doesn't fully support the standard condition write interface

You can submit an issue to discuss integration support. Please include:

- Object storage backend name and version
- API documentation for prevent-overwrite operations
- Compatibility details with MinIO/S3 API
- Test results showing prevent-overwrite behavior

We can work together to add support for your storage backend in Woodpecker.
