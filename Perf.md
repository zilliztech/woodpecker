# S3 write test

| Object Size | Metric | 1 Thread | 4 Threads | 8 Threads | 16 Threads | 32 Threads |
|-------------|---------|-----------|------------|------------|-------------|-------------|
| 4MB | Throughput | 27.4MB/s | 113MB/s | 240MB/s | 463MB/s | 932MB/s |
| 4MB | Latency | 136ms | 138.3ms | 135.1ms | 144.95ms | 142.4ms |
| 8MB | Throughput | 33MB/s | 117MB/s | 250MB/s | 500MB/s | 937MB/s |
| 8MB | Latency | 246ms | 261ms | 259ms | 257ms | 244.7ms |
| 16MB | Throughput | 39MB/s | 145MB/s | 287MB/s | 559MB/s | 1042MB/s |
| 16MB | Latency | 407ms | 447ms | 469ms | 421.5ms | 522.6ms |


# local minio standalone test


# Local MinIO Standalone Write Performance Test

| Object Size | Metric | 1 Thread | 2 Threads | 4 Threads | 8 Threads | 16 Threads | 32 Threads |
|-------------|---------|-----------|------------|------------|------------|------------|------------|
| 256 MiB | Throughput | 116.52 MiB/s | 122.80 MiB/s | 129.68 MiB/s | - | - | - |
| 256 MiB | Latency | 2139.6ms | 4178.5ms | 7903.7ms | - | - | - |
| 128 MiB | Throughput | 123.76 MiB/s | 126.47 MiB/s | 128.45 MiB/s | - | - | - |
| 128 MiB | Latency | 1037.5ms | 2048.8ms | 3927.2ms | - | - | - |
| 64 MiB | Throughput | 118.21 MiB/s | 123.64 MiB/s | 120.71 MiB/s | - | - | - |
| 64 MiB | Latency | 541.5ms | 1051.1ms | 2090.7ms | - | - | - |
| 32 MiB | Throughput | 105.14 MiB/s | 121.51 MiB/s | 112.25 MiB/s | 122.86 MiB/s | - | - |
| 32 MiB | Latency | 305.5ms | 527.4ms | 1137.7ms | 1033.8ms | - | - |
| 16 MiB | Throughput | 101.94 MiB/s | 123.95 MiB/s | 121.33 MiB/s | - | - | - |
| 16 MiB | Latency | 156.9ms | 257.8ms | 519.5ms | - | - | - |
| 8 MiB | Throughput | 92.53 MiB/s | 119.76 MiB/s | 116.18 MiB/s | 117.03 MiB/s | - | - |
| 8 MiB | Latency | 87.8ms | 133.1ms | 285.3ms | 276.1ms | - | - |
| 4 MiB | Throughput | 82.12 MiB/s | 108.20 MiB/s | 114.99 MiB/s | 97.76 MiB/s | - | - |
| 4 MiB | Latency | 49.6ms | 72.8ms | 138.7ms | 162.6ms | - | - |
| 2 MiB | Throughput | 70.40 MiB/s | 93.38 MiB/s | 108.45 MiB/s | 82.35 MiB/s | 102.19 MiB/s | - |
| 2 MiB | Latency | 28.4ms | 42.9ms | 73.5ms | 95.7ms | 319.6ms | - |
| 1 MiB | Throughput | 50.60 MiB/s | 67.85 MiB/s | 80.51 MiB/s | 55.78 MiB/s | 87.09 MiB/s | - |
| 1 MiB | Latency | 19.8ms | 29.2ms | 49.1ms | 70.1ms | 183.4ms | - |
| 512 KiB | Throughput | 33.43 MiB/s | 39.74 MiB/s | 51.82 MiB/s | 14.90 MiB/s | 61.32 MiB/s | 46.66 MiB/s |
| 512 KiB | Latency | 15.1ms | 24.4ms | 40.6ms | 31.7ms | 133.3ms | 367.0ms |
| 64 KiB | Throughput | 8.15 MiB/s | 11.02 MiB/s | 14.35 MiB/s | - | 17.54 MiB/s | 19.98 MiB/s |
| 64 KiB | Latency | 7.6ms | 11.9ms | 17.6ms | - | 59.5ms | 98.0ms |
| 1 KiB | Throughput | 0.14 MiB/s | 0.21 MiB/s | - | - | - | - |
| 1 KiB | Latency | 6.8ms | 9.2ms | - | - | - | - |
| 1 B | Throughput | 154.07 obj/s | 227.68 obj/s | - | - | - | - |
| 1 B | Latency | 6.6ms | 8.7ms | - | - | - | - |


# EFS write test
| Write Size | Buffer Size | Flush Latency (ms) | Throughput (MB/s) |
|------------|-------------|-------------------|------------------|
| 2 MB | 2 MB (immediate) | 73.295 | 27.22 |
| 2 MB | 4 MB | 134.198 | 29.68 |
| 2 MB | 8 MB | 171.737 | 46.20 |
| 2 MB | 16 MB | 231.954 | 67.35 |
| 2 MB | 32 MB | 350.589 | 86.42 |
| 4 KB | 4 KB (immediate) | 8.459 | 0.46 |
| 4 KB | 4 MB | 95.306 | 38.57 |
| 2 KB | 2 KB (immediate) | 8.361 | 0.23 |
| 1 KB | 1 KB (immediate) | 8.564 | 0.11 |
| 4 MB | 4 MB (immediate) | 133.931 | 29.71 |
| 8 MB | 8 MB (immediate) | 172.011 | 46.04 |


# SSD write test
| Write Size | Buffer Size | Write Latency (ms) | Throughput (MB/s) |
|------------|-------------|-------------------|------------------|
| 2 MB | 2 MB (immediate) | 8.000 | 247.00 |
| 2 MB | 8 MB | 18.300 | 430.60 |
| 2 MB | 16 MB | 27.700 | 562.98 |
| 2 MB | 32 MB | 54.230 | 558.34 |
| 4 KB | 4 KB (immediate) | 0.146 | 25.37 |
| 4 KB | 4 MB | 4.677 | 634.95 |


# Conclusion
1. 写入延迟对比:
- SSD 延迟最低,2MB数据写入延迟仅8ms,4KB数据写入延迟仅0.146ms
- EFS 延迟较高,2MB数据写入延迟73ms,4KB数据写入延迟8.459ms,约为SSD的10倍
- S3/MinIO 延迟最高,512KB数据写入延迟15-40ms,64KB数据写入延迟7-17ms,1KB数据写入延迟6-9ms

2. 吞吐量对比:
- SSD吞吐量最高,2MB数据写入吞吐量可达560MB/s,4KB数据写入吞吐量可达634MB/s
- EFS吞吐量中等,2MB数据写入吞吐量最高86MB/s,4KB数据写入吞吐量最高38MB/s
- S3/MinIO吞吐量较低,512KB数据写入吞吐量33-61MB/s,64KB数据写入吞吐量8-20MB/s

3. Buffer大小对性能的影响:
- SSD: Buffer从2MB增大到16MB,吞吐量从247MB/s提升到562MB/s,提升明显
- EFS: Buffer从2MB增大到32MB,吞吐量从27MB/s提升到86MB/s,提升明显
- S3/MinIO: 随着并发数增加,吞吐量有一定提升,但提升幅度不如SSD和EFS明显

4. 数据块大小对性能的影响:
- 三种存储介质都表现出随数据块增大,单次操作延迟增加,但总体吞吐量提升的特点
- SSD对小数据块处理性能最好,EFS和S3/MinIO对小数据块处理性能相对较差



