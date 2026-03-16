# Woodpecker Chaos Testing Matrix

This document defines the comprehensive chaos testing plan for Woodpecker.
It covers all failure modes across every component, organized by interaction path and failure category.

## Overview

### Test Environments

| Environment | Location | Scope | Strengths |
|-------------|----------|-------|-----------|
| **Docker-based** | `tests/docker/chaos_extra/` | Full system (all components) | Realistic infrastructure failures: etcd down, MinIO down, network partition between any components |
| **In-process** | `tests/integration/e2e_service_failover_test.go` (methods with `Chaos` keyword) | Woodpecker components + disk + interface-level mock | Fine-grained fault injection: fail on Nth write, inject latency per call, corrupt specific records, disk failure simulation |

### Relationship

```
Docker-based (superset)
  ├── Infrastructure failures (etcd, MinIO, network) — Docker only
  ├── Component interaction failures — Both environments
  └── Fine-grained I/O injection — In-process only

In-process (subset + exclusive) — in e2e_service_failover_test.go with Chaos keyword
  ├── Interface-level fault injection (Writer, MetadataProvider wrappers)
  ├── Precise operation-stage failures (fail during 5th flush, timeout on 2nd upload)
  ├── Disk I/O failures (fsync, partial write, disk full)
  └── Disk failure while node alive (chmod read-only simulation)
```

### Existing Coverage (tests/docker/chaos/)

The following scenarios are already covered in the existing `tests/docker/chaos/` suite:

| # | Test Name | Scenario |
|---|-----------|----------|
| E1 | TestChaos_BasicReadWrite | Smoke test: 1000 writes/reads, healthy cluster |
| E2 | TestChaos_SingleNodeKill_WriteContinues | Kill 1/4 nodes, writes continue via quorum |
| E3 | TestChaos_DoubleNodeKill_WriteBlocksAndRecovers | Kill 2/4 nodes, writes block then recover |
| E4 | TestChaos_NodeRestart_RecoveryMode | Kill and restart node, validate recovery |
| E5 | TestChaos_RollingRestart | Rolling restart of all 4 nodes |
| E6 | TestChaos_FullClusterRestart_DataDurability | Full cluster stop/restart, data persists |
| E7 | TestChaos_NetworkPartition_SingleNode | Disconnect 1 node from network |
| E8 | TestChaos_NetworkPartition_TwoNodes | Disconnect 2 nodes from network |
| E9 | TestChaos_MinIOFailure_WriteBlocksAndRecovers | Stop MinIO, writes fail, restart, recovery |

### Existing Coverage (tests/integration/e2e_service_failover_test.go)

| # | Test Name | Scenario |
|---|-----------|----------|
| F1-F16 | Case1-Case16 | 16 in-process failover tests covering node failure, quorum loss, rolling restart, reader fallback, LAC calculation, data loss scenarios |

#### New In-process Chaos Tests (in e2e_service_failover_test.go)

| # | Test Name | Scenario | Maps to |
|---|-----------|----------|---------|
| CH1 | TestStagedStorageService_Chaos_CompetingWritersFencing | Writer A → B handoff with fencing | F1, C3 |
| CH2 | TestStagedStorageService_Chaos_ConcurrentWriterAttempts | 2 goroutines race for writer lock | C5, M10 |
| CH3 | TestStagedStorageService_Chaos_FencingDuringNodeFailure | Fence with crashed ensemble node | W5, F6 |
| CH4 | TestStagedStorageService_Chaos_DiskFailureNodeAlive | chmod node data dir read-only, writes route to healthy nodes | D7 |
| CH5 | TestStagedStorageService_Chaos_DiskFullSimulation | 2 nodes disk-full via chmod, writes via remaining nodes | D8 |
| CH6 | TestStagedStorageService_Chaos_RecoveryMultiSegmentAfterCrash | 3 segments with crash/recovery | R1 |
| CH7 | TestStagedStorageService_Chaos_ReaderDuringServerCrash | Reader failover mid-read | P1 |
| CH8 | TestStagedStorageService_Chaos_ConcurrentReaders | 3 concurrent readers independence | P4 |
| CH9 | TestStagedStorageService_Chaos_DiskFailureDuringActiveWrite | chmod mid-segment write | D9 |

---

## New Chaos Scenarios

### Naming Convention

```
TestChaosExtra_<Component>_<Operation>_<Failure>
```

Examples:
- `TestChaosExtra_Etcd_SegmentCompletion_Unavailable`
- `TestChaosExtra_Server_WALFlush_CrashDuringSync`

---

## 1. Metadata (etcd) Failure Scenarios

### 1.1 etcd Unavailability

| ID | Scenario | Environment | Priority | Description |
|----|----------|-------------|----------|-------------|
| M1 | etcd down during write | Docker | P0 | Stop etcd while client is appending. Writes should continue (server buffers). Segment completion should fail/retry. Restart etcd, verify completion succeeds. |
| M2 | etcd down during segment completion | Docker | P0 | Stop etcd right when segment rolling triggers. Verify completion retries after etcd recovers. No data loss. |
| M3 | etcd down during log creation | Docker | P1 | Stop etcd, attempt CreateLog. Should fail with retryable error. Restart etcd, retry succeeds. |
| M4 | etcd restart during active session | Docker | P1 | Restart etcd while writer holds session lock. Verify session lease survives or writer detects loss and re-acquires. |
| M5 | etcd network partition from servers | Docker | P0 | Disconnect etcd from woodpecker network. Servers can still serve buffered writes but cannot update metadata. Reconnect, verify consistency. |

### 1.2 Metadata Conflicts (Revision-based)

| ID | Scenario | Environment | Priority | Description |
|----|----------|-------------|----------|-------------|
| M6 | Revision conflict during segment completion | In-process | P0 | Inject stale revision on UpdateSegmentMetadata. Verify CAS fails and operation retries with fresh revision. |
| M7 | Stale metadata read after segment state change | In-process | P1 | Return outdated SegmentMeta from GetSegmentMetadata. Verify caller detects staleness and refreshes. |
| M8 | Concurrent segment metadata updates | In-process | P1 | Two goroutines race to update same segment metadata. Exactly one should succeed; loser retries. |
| M9 | Session lock expired during write | In-process | P0 | Simulate session lock expiry (CheckSessionLockAlive returns false). Writer should stop and client should detect invalidation. |
| M10 | Session lock acquisition race | In-process | P1 | Two writers race to AcquireLogWriterLock. Exactly one wins. Loser gets appropriate error. |

---

## 2. Object Storage (MinIO) Failure Scenarios

### 2.1 MinIO Infrastructure Failures

| ID | Scenario | Environment | Priority | Description |
|----|----------|-------------|----------|-------------|
| S1 | MinIO down during segment compaction | Docker | P0 | Stop MinIO while segment compaction (upload merged block) is in progress. Verify compaction retries after MinIO recovers. |
| S2 | MinIO restart during active writes | Docker | P1 | Restart MinIO. Staged storage writes to local disk should continue. Compaction upload should retry. |
| S3 | MinIO network partition | Docker | P0 | Disconnect MinIO from network. Writes to staged storage local disk continue. Compaction fails. Reconnect, verify eventual upload. |
| S4 | MinIO slow (high latency) | Docker | P2 | Use `tc` to add 500ms+ latency to MinIO. Verify writes don't timeout prematurely and throughput degrades gracefully. |

### 2.2 Object Storage Operation Failures

| ID | Scenario | Environment | Priority | Description |
|----|----------|-------------|----------|-------------|
| S5 | Upload error during block flush | In-process | P0 | ObjectStorage mock returns error on PutObject. Verify retry logic and no silent data loss. |
| S6 | Upload timeout during compaction | In-process | P1 | ObjectStorage mock hangs (context timeout). Verify compaction fails cleanly and can be retried. |
| S7 | Partial upload (incomplete object) | In-process | P1 | ObjectStorage mock returns success but data is incomplete. Verify CRC check catches corruption on read. |
| S8 | GetObject failure during read | In-process | P1 | ObjectStorage mock returns error on read. Reader should retry or fail with clear error. |
| S9 | Condition write conflict | In-process | P1 | StoreOrGetConditionWriteResult returns conflicting result. Verify distributed agreement converges. |

---

## 3. Server (Woodpecker LogStore) Failure Scenarios

### 3.1 Server Crash at Specific Stages

| ID | Scenario | Environment | Priority | Description |
|----|----------|-------------|----------|-------------|
| W1 | Server crash during WAL sync | Docker + In-process | P0 | Kill server while Sync() is in progress. Restart, verify recovery reads partial data correctly. |
| W2 | Server crash during segment finalization | Docker + In-process | P0 | Kill server during Finalize() (writing footer/index). Restart, verify recovery reconstructs index from data blocks. |
| W3 | Server crash during compaction upload | Docker | P0 | Kill server while Compact() is uploading to MinIO. Restart, verify compaction retries and data integrity. |
| W4 | Server crash with buffered entries | Docker + In-process | P1 | Kill server while entries are in write buffer (not yet synced). Verify buffered entries are lost but previously synced data is safe. |
| W5 | Server crash during fence operation | Docker + In-process | P0 | Kill server while processing Fence() RPC. Verify fencing state is consistent after restart. |

### 3.2 Server Graceful Degradation

| ID | Scenario | Environment | Priority | Description |
|----|----------|-------------|----------|-------------|
| W6 | Server process pause (simulates GC pause) | Docker | P1 | Pause server container. Writes timeout. Unpause, verify server recovers and serves pending requests. |
| W7 | Server slow response (overloaded) | Docker | P2 | Pause and unpause server rapidly. Verify client retry handles intermittent slow responses. |
| W8 | Server OOM / resource exhaustion | Docker | P2 | Set memory limit very low. Verify server crashes gracefully and other nodes take over via quorum. |

---

## 4. Disk / Local Storage Failure Scenarios

| ID | Scenario | Environment | Priority | Description |
|----|----------|-------------|----------|-------------|
| D1 | fsync failure during WAL flush | In-process | P0 | Writer wrapper returns error on Sync(). Verify entries are not acknowledged as synced. Client is notified. |
| D2 | Disk full during write | In-process | P0 | Writer wrapper returns "no space left" on WriteDataAsync after N entries. Verify graceful degradation. |
| D3 | Partial write (truncated block) | In-process | P0 | Writer wrapper writes incomplete data then returns error. Recovery should detect and discard partial block. |
| D4 | Corrupted block data | In-process | P1 | Write valid data, then corrupt bytes in stored block. Reader should detect CRC mismatch. |
| D5 | Slow disk I/O | In-process | P2 | Writer wrapper adds latency to each write. Verify timeout handling and no cascading failures. |
| D6 | Write succeeds but fsync fails | In-process | P1 | WriteDataAsync succeeds but subsequent Sync fails. Verify data is not confirmed as durable. |
| D7 | Disk failure while node alive (read-only) | In-process | P0 | chmod node data dir read-only (node still alive). Writer gets ErrStorageNotWritable, writes route to other nodes. Verify data integrity. **Implemented: CH4** |
| D8 | Multiple nodes disk full | In-process | P0 | chmod 2 node data dirs read-only. Writes continue via remaining healthy nodes. Verify quorum still works. **Implemented: CH5** |
| D9 | Disk failure during active write | In-process | P0 | chmod mid-segment write. Already-written entries preserved, subsequent writes route elsewhere. **Implemented: CH9** |

---

## 5. Client Failure Scenarios

| ID | Scenario | Environment | Priority | Description |
|----|----------|-------------|----------|-------------|
| C1 | Client disconnect during append | Docker | P0 | Kill client context mid-write. Verify server-side state is clean (no stuck segments). New client can resume. |
| C2 | Client reconnect after partition | Docker | P1 | Disconnect client network briefly. Writes fail. Reconnect. Verify client can resume writing. |
| C3 | Stale writer continues after fencing | In-process | P0 | Writer A is fenced. Simulate delayed detection — writer A attempts more writes. All should fail with ErrSegmentFenced. |
| C4 | Writer session lost (etcd lease expired) | In-process + Docker | P0 | Simulate session expiry. Writer should be invalidated. New writer can acquire lock. |
| C5 | Concurrent writers on same log | In-process | P0 | Two writers attempt to write to same log simultaneously. Only one should succeed at any time (session lock guarantees). |
| C6 | Client retry storm after transient failure | In-process | P2 | Inject transient errors. Verify exponential backoff prevents retry amplification. |

---

## 6. Fencing & Segment Completion Scenarios

| ID | Scenario | Environment | Priority | Description |
|----|----------|-------------|----------|-------------|
| F1 | Competing writers cause fencing | In-process + Docker | P0 | Writer A is active. Writer B acquires lock (A's session expired). B triggers fence on A's segment. Verify: A's writes fail, B can create new segment. |
| F2 | Fence during active flush | In-process | P0 | Fence arrives while Sync() is in progress. Verify fence takes effect after current sync completes. No partial state. |
| F3 | Fence + complete with stale LAC | In-process | P1 | FenceAndComplete with LAC that doesn't match actual last entry. Verify system uses correct LAC from storage. |
| F4 | Segment completion timeout | In-process + Docker | P1 | CompleteSegment RPC times out on one node. Verify completion retries and eventual consistency. |
| F5 | Concurrent segment completion race | In-process | P1 | Two goroutines race to complete same segment. Exactly one succeeds via etcd CAS. |
| F6 | Fence all quorum nodes, one fails | In-process | P1 | FenceSegment RPC fails on 1 of 3 nodes. Verify fencing still succeeds if quorum achieved. |

---

## 7. Recovery Scenarios

| ID | Scenario | Environment | Priority | Description |
|----|----------|-------------|----------|-------------|
| R1 | Recovery from partially flushed WAL | In-process + Docker | P0 | Server restarts with incomplete last block in WAL file. Recovery mode should reconstruct valid state. |
| R2 | Recovery with missing object storage segment | Docker | P1 | Delete a segment from MinIO. Server restarts. Verify reader skips missing segment or reports clear error. |
| R3 | Recovery from corrupted footer | In-process | P0 | Truncate footer record in WAL file. Recovery scans data blocks to rebuild index. |
| R4 | Recovery after double failure (server + MinIO) | Docker | P1 | Kill server and MinIO. Restart MinIO first, then server. Verify full recovery. |
| R5 | Recovery with conflicting metadata | In-process | P1 | Metadata says segment is Active but storage has finalized file. Recovery should detect and reconcile. |
| R6 | Stale metadata revision during recovery | In-process | P1 | GetSegmentMetadata returns stale revision during recovery. Verify recovery handles gracefully. |

---

## 8. Multi-Component Failure Scenarios

| ID | Scenario | Environment | Priority | Description |
|----|----------|-------------|----------|-------------|
| X1 | Server crash + MinIO down simultaneously | Docker | P0 | Kill server and stop MinIO at same time. Restart MinIO, then server. Verify full recovery and data integrity. |
| X2 | etcd down + server restart | Docker | P0 | Stop etcd. Restart a server. Server should fail to start or wait for etcd. Start etcd. Server initializes. |
| X3 | Network partition: server isolated from etcd + MinIO | Docker | P1 | Server can't reach etcd or MinIO but client can reach server. Writes to local buffer succeed temporarily. |
| X4 | Rolling etcd + server restart | Docker | P2 | Restart etcd while performing rolling restart of servers. Verify cluster recovers. |
| X5 | MinIO slow + server crash | Docker | P1 | Add latency to MinIO. Kill a server during slow compaction. Restart. Verify no stuck segments. |
| X6 | Writer fence + server crash + etcd delay | Docker | P1 | Fence writer, crash server during metadata update, delay etcd response. Verify metadata consistency after recovery. |

---

## 9. Quorum-Specific Scenarios

| ID | Scenario | Environment | Priority | Description |
|----|----------|-------------|----------|-------------|
| Q1 | Write with exactly ack_quorum nodes alive | Docker | P0 | In 4-node cluster (es=3, aq=2), kill nodes until exactly aq nodes remain for segment. Writes should succeed. |
| Q2 | Lose ack_quorum during write batch | Docker | P1 | Start writing, then kill nodes to drop below aq mid-batch. Partial batch should fail, data before failure safe. |
| Q3 | Node rejoins with stale data | Docker | P1 | Kill node, write more data, restart node. Verify recovery mode catches up the rejoined node. |
| Q4 | Quorum selection with all preferred nodes down | In-process | P1 | All nodes in preferred AZ are down. Verify fallback to other AZ works correctly. |
| Q5 | Asymmetric network partition in quorum | Docker | P2 | Node A can reach B but not C; B can reach C but not A. Verify write behavior under split-brain scenario. |

---

## 10. Read Path Chaos Scenarios

| ID | Scenario | Environment | Priority | Description |
|----|----------|-------------|----------|-------------|
| P1 | Reader during server crash | Docker + In-process | P1 | Kill server while tail reader is reading. Verify reader fails over to another replica. |
| P2 | Reader with stale LAC | In-process | P1 | Reader has outdated LAC. Verify UpdateLastAddConfirmed refreshes correctly. |
| P3 | Reader across segment boundary during completion | In-process | P1 | Reader is at end of segment while segment completes. Verify reader transitions to next segment. |
| P4 | Concurrent readers on same segment | In-process | P2 | Multiple readers on same segment. No interference. Each maintains independent state. |
| P5 | Reader after segment truncation | In-process + Docker | P1 | Truncate log to remove old segments. Verify reader positioned at truncated segment gets clear error. |

---

## Chaos Testing Matrix Summary

### By Component

| Component | Docker-only | In-process-only | Both | Total |
|-----------|-------------|-----------------|------|-------|
| Metadata (etcd) | 5 (M1-M5) | 5 (M6-M10) | 0 | 10 |
| Object Storage (MinIO) | 4 (S1-S4) | 5 (S5-S9) | 0 | 9 |
| Server (Woodpecker) | 3 (W6-W8) | 0 | 5 (W1-W5) | 8 |
| Disk / Local Storage | 0 | 9 (D1-D9) | 0 | 9 |
| Client | 2 (C1-C2) | 2 (C5-C6) | 2 (C3-C4) | 6 |
| Fencing & Completion | 0 | 3 (F2,F3,F5,F6) | 2 (F1,F4) | 6 |
| Recovery | 0 | 3 (R3,R5,R6) | 1 (R1) | 7 |
| Multi-Component | 6 (X1-X6) | 0 | 0 | 6 |
| Quorum | 3 (Q1-Q3,Q5) | 1 (Q4) | 0 | 5 |
| Read Path | 0 | 3 (P2-P4) | 2 (P1,P5) | 5 |
| **Total** | **23** | **31** | **12** | **71** |

### By Priority

| Priority | Count | Description |
|----------|-------|-------------|
| P0 (Critical) | 25 | Core durability, fencing, recovery — must work correctly |
| P1 (Important) | 31 | Edge cases that affect reliability under real failures |
| P2 (Nice-to-have) | 10 | Performance degradation, resource exhaustion, rare scenarios |

### Implementation Order

**Phase 1 — Foundation (P0 Docker-based)**
- M1, M2, M5: etcd unavailability during writes and completion
- S1, S3: MinIO failure during compaction
- W1, W2, W3, W5: Server crash at critical stages
- X1, X2: Multi-component failures
- Q1: Quorum boundary

**Phase 2 — In-process Framework + P0 In-process**
- Build fault injection wrappers (Writer, MetadataProvider decorators)
- M6, M9: Revision conflict, session expiry
- S5: Upload error injection
- D1, D2, D3: Disk I/O failures
- C3, C5: Stale writer, concurrent writers
- F1, F2: Fencing races
- R1, R3: Recovery from partial/corrupt state

**Phase 3 — P1 scenarios**
- Remaining Docker and In-process tests

**Phase 4 — P2 scenarios**
- Latency injection, resource exhaustion, rare multi-component combinations

---

## Invariants to Verify

Every chaos test should verify one or more of these system invariants:

1. **Durability**: Acknowledged writes are never lost (data readable after any recoverable failure)
2. **Ordering**: Entry IDs are monotonically increasing within a segment; segment IDs are monotonically increasing within a log
3. **Fencing correctness**: A fenced writer cannot produce new acknowledged writes
4. **Single-writer guarantee**: At most one active writer per log at any time
5. **Metadata consistency**: Segment metadata in etcd matches actual segment state in storage
6. **Recovery completeness**: After recovery, all synced data is readable; only buffered (unsynced) data may be lost
7. **No stuck state**: System does not permanently hang after any recoverable failure sequence
8. **CRC integrity**: All read data passes CRC32 checksum validation

---

## File Organization

```
tests/docker/chaos_extra/                          # Docker-based chaos tests (full system)
├── chaos_matrix.md                                # This document
├── docker-compose.chaos-extra.yaml                # Docker compose override (restart: no)
├── run_chaos_extra_tests.sh                       # Test runner script
├── chaos_extra_cluster.go                         # ChaosExtraCluster helper (extends DockerCluster)
├── chaos_extra_helpers.go                         # Shared test helpers
├── etcd_chaos_test.go                             # M1-M5: etcd failure scenarios
├── minio_chaos_test.go                            # S1-S3: MinIO failure scenarios
├── server_crash_test.go                           # W1-W3,W5,W6: Server crash/degradation scenarios
├── multi_component_chaos_test.go                  # X1-X3,Q1,C1: Multi-component + quorum + client scenarios
├── quorum_chaos_test.go                           # (planned) Q2-Q5: Additional quorum scenarios
├── recovery_chaos_test.go                         # (planned) R1,R2,R4: Recovery Docker scenarios
└── read_path_chaos_test.go                        # (planned) P1,P5: Read path Docker scenarios

tests/integration/e2e_service_failover_test.go     # In-process chaos tests (Chaos keyword in method names)
  ├── TestStagedStorageService_Chaos_CompetingWritersFencing        # F1,C3: Writer handoff + fencing
  ├── TestStagedStorageService_Chaos_ConcurrentWriterAttempts       # C5,M10: Writer lock race
  ├── TestStagedStorageService_Chaos_FencingDuringNodeFailure       # W5,F6: Fence with crashed node
  ├── TestStagedStorageService_Chaos_DiskFailureNodeAlive           # D7: Disk failure, node alive
  ├── TestStagedStorageService_Chaos_DiskFullSimulation             # D8: Multiple nodes disk full
  ├── TestStagedStorageService_Chaos_RecoveryMultiSegmentAfterCrash # R1: Multi-segment recovery
  ├── TestStagedStorageService_Chaos_ReaderDuringServerCrash        # P1: Reader failover
  ├── TestStagedStorageService_Chaos_ConcurrentReaders              # P4: Concurrent reader independence
  └── TestStagedStorageService_Chaos_DiskFailureDuringActiveWrite   # D9: Mid-write disk failure
```
