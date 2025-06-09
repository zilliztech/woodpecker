![woodpecker logo](./docs/images/logo.png)
# Woodpecker
*A Cloud-Native WAL Storage Implementation*

> **Status**: Woodpecker is currently under active development and is being continuously improved. Stay tuned for future updates and releases!

## 🌟 Introduction
**Woodpecker** is a cloud-native **Write-Ahead Log (WAL) storage** Implementation that leverages cloud object storage for **low-cost, high-throughput, and reliable** logging. Unlike traditional on-premises WAL storage or custom-built distributed logging solutions, Woodpecker is designed to **fully utilize cloud-native infrastructure** for scalability and durability.

With its **high-throughput writes, ordered durability guarantees, and efficient reads**, Woodpecker provides a lightweight yet powerful solution for cloud-native logging.

### **Use Cases**
Although Woodpecker is initially built for vectordatabase use case [Milvus](https://milvus.io/) and [Zilliz Cloud](https://zilliz.com/), we believe it has strong potential to serve multiple different workloads on cloud. Woodpecker is suitable for **high-throughput, ordered, and durable log storage** scenarios, including:

- **Distributed WAL for databases** – Ensuring write-ahead logging with strict ordering and persistence.
- **Streaming and event sourcing** – Providing a durable event log for stream processing frameworks.
- **Consensus protocols** – Serving as a persistent log for distributed consensus algorithms (e.g., Raft, Paxos).
- **Transaction logs** – Storing ordered, durable logs for financial or critical business applications.

---

### 🚀 Key Features
✅ **Cloud-Native WAL** – Uses cloud object storage as the durable storage layer, ensuring scalability and cost-effectiveness.  
✅ **High-Throughput Writes** – Optimized for cloud storage with specialized write strategies to maximize sequential write throughput.  
✅ **Efficient Log Reads** – Utilizes memory management and prefetching strategies to optimize sequential log access.  
✅ **Ordered Durability** – Guarantees strict sequential ordering for log persistence.  
✅ **Flexible Deployment** – Can be deployed as a standalone service or integrated as an embedded library in your application.  
✅ **Resilient & Fault-Tolerant** – Leverages cloud reliability features for strong durability guarantees.  
✅ **One Write, Multiple Reads** – Adopts a "data-embedded metadata" design, enabling multiple read operations without the need for tight synchronization with the writer's metadata.

---

## 🏗 Architecture Overview

### **Embedded mode Architecture**
In the embedded mode, **Woodpecker** is designed as a lightweight library with minimal dependencies. It does not require complex services but relies on **etcd** for metadata and coordination. This design ensures efficient operation while keeping the system simple and flexible.

Woodpecker supports integration with a variety of cloud-native object storage backends, including those that are compatible with the **MinIO API**, as well as major cloud providers like **AWS, GCP, Azure and Aliyun**. This enables seamless deployment in multi-cloud environments, making it highly adaptable for different storage needs while leveraging the scalability and reliability of cloud object storage.

![embedded mode architecture diagram](./docs/images/embedded_architecture.png)

### **Service mode Architecture**

In the service mode, the WAL read/write operations and caching logic are decoupled into a dedicated **LogStore** component cluster service, acting as a caching layer for the object storage. This architecture further improves throughput and reduces latency by employing data prefetching and read/write caching strategies.

![service mode architecture diagram](./docs/images/service_architecture.png)


### **Core Components**

- **Client** – Read/write protocol layer.
- **LogStore** – Handles high-speed log writes, batching, and cloud storage uploads.
- **ObjectStorage Backend** – Uses cloud object storage (e.g., S3, GCS, OSS) as a scalable, low-cost WAL backend.
- **ETCD** – Uses etcd to store metadata and coordination .
- **EmbeddedClient** – Client with LogStore component in it.

### **Design Benefits**
- **Lightweight deployment**, making it easy to integrate with various systems.
- **Decouples compute & storage**, reducing operational complexity.
- **Auto-scaling storage**, eliminating capacity planning overhead.
- **Reduces local disk dependency**, making it ideal for cloud-native workloads.

---

## 🏎 Benchmarks


| Test Scenario                      | Description                                             | Throughput | Latency |
|------------------------------------|---------------------------------------------------------|------------|---------|
| **embedded mode one client write** | Concurrent async append log entries to S3               | 729.4 MB/s | T+100ms |
| **embedded mode one client read**  | Reading batches of log entries from S3                  | 768.0 MB/s | 245ms   |
| **embedded mode one client write** | Concurrent async append log entries to standalone minio | 103.04MB/s | T+19ms  |
| **embedded mode one client read**  | Reading batches of log entries from standalone minio    | 141.9MB/s  | 14.1ms  |
| **embedded mode one client write** | Concurrent async append log entries to local SSD        | 562.9MB/s  | T+27.ms |
| **embedded mode one client read**  | Reading batches of log entries from local SSD           |            |         |

> **Note**: The performance benchmarks are based on a single ECS instance writing to S3. While there are traffic limitations on a single ECS instance, the upper throughput limits can be higher with proper configuration or scaling. Additionally, **Woodpecker** is still under active development, and performance improvements are continually being made to enhance throughput and reduce latency.
> **Note**: Test 2MB-16MB per entry, record the max throughput and latency. ”T“ represents the time interval for flush buffer, which is configurable.
---

## 🎯 **Usage Examples**

### **1. Install Woodpecker**
```bash
go install github.com/zilliztech/woodpecker@latest
```

### **2. Custom Config**
```yaml
# woodpecker.yaml
woodpecker:
  meta:
    type: etcd # Type of the configuration, currently only etcd is supported.
    prefix: woodpecker # Root prefix of the key to where Woodpecker stores data in etcd.
  client:
    segmentAppend:
      queueSize: 10000 # Maximum number of queued segment append requests, default is 10000
      maxRetries: 3 # Maximum number of retries for segment append operations
    segmentRollingPolicy:
      maxSize: 2000000000 # Maximum entries count of a segment, default is 2GB
      maxInterval: 600 # Maximum interval between two segments in seconds, default is 10 minutes
    auditor:
      maxInterval: 10 # Maximum interval between two auditing operations in seconds, default is 10 seconds
  logstore:
    segmentSyncPolicy:
      maxInterval: 1000 # Maximum interval between two sync operations in milliseconds
      maxEntries: 100000 # Maximum entries number of write buffer
      maxBytes: 64000000 # Maximum size of write buffer in bytes
      maxFlushRetries: 5 # Maximum number of retries for sync operations
      retryInterval: 1000 # Maximum interval between two retries in milliseconds
      maxFlushSize: 8000000 # Maximum size of a fragment in bytes to flush, default is 8M
      maxFlushThreads: 4 # Maximum number of threads to flush data
```

### **3. Use Woodpecker for write**
**3.1 open client writer**
```
// open client writer
cfg, _ := config.NewConfiguration()
client, _ := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
_ = client.CreateLog(context.Background(), "test_log_single")
logHandle, _ := client.OpenLog(context.Background(), "test_log_single")
logWriter, _ := logHandle.OpenLogWriter(context.Background())
```
**3.2 sync/async write**

sync write 
```
// Sync Write
writeResult := logWriter.Write(context.Background(),
    &log.WriterMessage{
        Payload: []byte("hello world"),
        Properties: map[string]string{
            "key": fmt.Sprintf("value"),
        },
    }, 
)
```
or async write
```
// Async Write multi messages as a batch
resultChan := make([]<-chan *log.WriteResult, count)
for i := 0; i < count; i++ {
	writeResultChan := logWriter.WriteAsync(context.Background(),
		&log.WriterMessage{
			Payload: []byte(fmt.Sprintf("hello world %d", i)),
			Properties: map[string]string{
				"key": fmt.Sprintf("value%d", i),
			},
		},
	)
	resultChan[i] = writeResultChan
}
// wait for async result for this batch
for _, resultChan := range resultChan {
	r := <-resultChan
	// biz ...
}
// Other biz logic ...
```

### **4. Use Woodpecker for read**
```
logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), start)
	if openReaderErr != nil {
		fmt.Printf("Open reader failed, err:%v\n", openReaderErr)
		panic(openReaderErr)
	}

	// iterate through all data logReader.ReadNext(context.Background())
	for {
		msg, err := logReader.ReadNext(context.Background())
		if err == nil {
			// read success, do biz logic...
		} else {
			// read fail, do biz logic...
                } 
	}
	
// Other biz logic...
```


## 📜 **License**

Woodpecker is licensed under different open source licenses depending on the component:

- **Server components** (`server/` directory): Dual-licensed under your choice of either:
  - [GNU Affero General Public License v3.0 (AGPLv3)](server/LICENSE)
  - [Server Side Public License v1 (SSPLv1)](server/LICENSE)

- **All other components**: Licensed under [Apache License 2.0](APACHE-LICENSE-2.0.txt)

See [LICENSE.txt](LICENSE.txt) for detailed license information and [NOTICE](NOTICE) for copyright and contribution details.

