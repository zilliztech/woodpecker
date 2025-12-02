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

## Fallback to Distributed Locks

For object storage backends that don't support condition write (typically some open-source or self-hosted solutions), Woodpecker automatically falls back to a **distributed lock** mechanism using etcd.

### Fallback Logic

The fallback behavior depends on the `fencePolicy.conditionWrite` configuration:

#### Auto Mode (Default)
- **Detection Phase**: Attempts to detect condition write support up to 30 times with exponential backoff
- **Optimistic Enable**: If any detection succeeds, condition write is enabled for the entire cluster
- **Pessimistic Fallback**: If all 30 attempts fail, automatically falls back to distributed lock mode
- **Cluster Consensus**: All nodes share a single global result stored in etcd via CAS operation
- **First Detection Wins**: The first successful detection determines the cluster-wide setting

#### Enable Mode
- **Strict Requirement**: Condition write detection must succeed within 10 retries
- **Failure Behavior**: Panics if detection fails after all retries
- **Use Case**: When you're certain your storage backend supports condition write

#### Disable Mode
- **Skip Detection**: Completely skips condition write detection
- **Direct Fallback**: Immediately uses distributed lock mode
- **Use Case**: When you know your storage backend doesn't support condition write

### Detection and Storage Separation

The detection retry logic is separated from etcd storage operations:

1. **Phase 1 - Detection**: Retries storage capability detection up to the configured limit
2. **Phase 2 - Storage**: Independently retries storing the result to etcd (up to 10 times)
3. **Error Handling**: If etcd storage fails after all retries, returns an error rather than silently using local detection result

This separation ensures that etcd failures don't consume detection retry quota, preventing scenarios where storage supports condition write but etcd failures cause incorrect fallback.

## Distributed Lock Implementation

When condition write is not available, Woodpecker uses distributed locks via etcd to coordinate concurrent writes.

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

- **`auto`**: Automatically detect and use condition write if available, fallback to distributed locks otherwise (recommended)
- **`enable`**: Require condition write support, panic if not available
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
