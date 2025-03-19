# Woodpecker 系统设计文档

## 1. 简介

Woodpecker 是一个高性能、分布式的日志存储系统，专注于提供可靠的数据持久化、高效的数据读写以及灵活的扩展能力。系统采用模块化设计，支持对象存储后端，并实现了高效的缓存管理机制。

## 2. 系统架构

### 2.1 核心组件

**系统层级关系**

| 客户端层 (Client Layer)                                    | 服务层 (Server Layer)                                              | 存储层 (Storage Layer)                                     |
|--------------------------------------------------------|-----------------------------------------------------------------|---------------------------------------------------------|
| Client  <br/> Log Writer/Reader<br/>   Segment Handler | Log Store  <br/>  Segment Processor <br/> LogFile Writer/Reader | Object Storage <br/>Local File System<br/> Cache System |

**配置与监控 (Config & Monitor)**
- Metrics
- Logging
- Configuration

### 2.2 主要模块

1. **客户端层 (Client Layer)**
   - **客户端 (Client)**: 提供日志数据的读写操作API
   - **日志读写器 (Log Writer/Reader)**: 处理日志数据的读写操作句柄
   - **段处理器 (Segment Handler)**: 在客户端层管理数据段中数据的读写协议操作

2. **服务层 (Server Layer)**
   - **日志存储 (Log Store)**: 管理日志数据的存储和检索的存储服务
   - **段处理器 (Segment Processor)**: 服务端管理数据段的相应操作
   - **日志文件读写器 (LogFile Writer/Reader)**: 负责日志文件级别的读写操作

3. **存储层 (Storage Layer)**
   - **对象存储 (Object Storage)**: 基于 MinIO 实现的持久化存储
   - **本地文件系统 (Local File System)**: 提供本地数据存储能力
   - **缓存系统 (Cache System)**: 提供内存缓存以提高性能

4. **配置与监控 (Config & Monitor)**
   - **指标 (Metrics)**: 系统性能指标收集
   - **日志 (Logging)**: 系统运行日志
   - **配置 (Configuration)**: 系统配置管理

## 3. 核心概念

### 3.1 数据模型

- **日志 (Log)**: 表示一个连续的数据流
- **段 (Segment)**: 日志的逻辑分区
- **日志文件 (LogFile)**: 段中的物理存储单元
- **片段 (Fragment)**: 日志文件的分片，是数据的基本存储单位

### 3.2 关键流程

1. **写入流程**:
   - 客户端调用 AppendAsync 向日志添加数据
   - 数据首先写入内存缓冲区
   - 当达到特定条件(大小、数量、时间)时，触发 Sync 操作
   - Sync 将数据持久化到存储系统作为日志文件片段
   - 向客户端顺序应答数据写入成功的ack

2. **读取流程**:
   - 客户端创建特定范围的 Reader
   - Reader 通过段管理器定位相应段
   - 从段中获取 LogFile，并从片段中读取数据
   - 数据可能从缓存或对象存储中获取

3. **缓存管理**:
   - 片段加载到内存缓存中
   - 以LRU基础上，结合日志顺序读写的特点进行缓存策略调整优化，进行缓存淘汰
   - 自动或手动触发淘汰机制控制内存使用

## 4. 迭代开发计划

### 4.1 阶段一: 基础存储层

**目标**: 实现基本的对象存储接口和片段管理

**关键模块**:
- Fragment: 实现片段的基本操作
- LogFile: 实现日志文件的读写
- 对象存储适配器: 支持 MinIO 等存储后端

**接口设计**:
```go

// Fragment 接口定义读写操作
type Fragment interface {
    GetFragmentId() int64
    GetFragmentKey() string
    Flush(ctx context.Context) error
    Load(ctx context.Context) error
    GetLastEntryId() (int64, error)
    GetEntry(entryId int64) ([]byte, error)
    GetSize() int64
    Release() error
}

// LogFile 接口定义日志文件操作
type LogFile interface {
    GetId() int64
    AppendAsync(ctx context.Context, entryId int64, data []byte) (int64, <-chan int64, error)
    NewReader(ctx context.Context, opt ReaderOpt) (Reader, error)
    Sync(ctx context.Context) error
    Close() error
}
```

### 4.2 阶段二: 缓存系统

**目标**: 实现高效的内存缓存管理

**关键模块**:
- FragmentManager: 管理片段缓存
- 缓存淘汰策略: 实现 LRU 等淘汰算法
- 内存使用监控: 实现内存使用监控和限制

**接口设计**:
```go
// FragmentManager 负责管理片段的内存使用
type FragmentManager interface {
    GetMaxMemory() int64
    GetUsedMemory() int64
    GetFragment(ctx context.Context, fragmentKey string) (Fragment, bool)
    AddFragment(ctx context.Context, fragment Fragment) error
    RemoveFragment(ctx context.Context, fragment Fragment) error
    EvictFragments() error
    StartEvictionLoop(interval time.Duration) error
    StopEvictionLoop() error
}
```

### 4.3 阶段三: 服务层实现

**目标**: 实现段管理和日志存储服务

**关键模块**:
- LogStore: 管理日志存储
- SegmentProcessor: 处理段操作
- LogFile: 实际存储系统的日志文件操作对象

**接口设计**:
```go
// LogFile 定义文件操作
type LogFile interface{
    GetId() int64
	Append(ctx context.Context, data []byte) error
	AppendAsync(ctx context.Context, entryId int64, data []byte) (int64, <-chan int64, error)
	NewReader(ctx context.Context, opt ReaderOpt) (Reader, error)
	LastFragmentId() uint64
	GetLastEntryId() (int64, error)
	Sync(ctx context.Context) error
	Merge(ctx context.Context) ([]Fragment, []int32, []int32, error)
	Load(ctx context.Context) (int64, Fragment, error)
}

// Segment 定义段操作
type Segment interface {
    GetId() int64
    Append(ctx context.Context, data []byte) (int64, error)
    ReadEntry(ctx context.Context, entryId int64) ([]byte, error)
    GetFirstEntryId() (int64, error)
    GetLastEntryId() (int64, error)
}

// LogStore 定义日志存储操作
type LogStore interface {
    CreateSegment(ctx context.Context) (int64, error)
    GetSegment(ctx context.Context, segmentId int64) (Segment, error)
    ListSegments(ctx context.Context) ([]int64, error)
    DeleteSegment(ctx context.Context, segmentId int64) error
}
```

### 4.4 阶段四: 客户端接口

**目标**: 提供易用的客户端接口

**关键模块**:
- LogReader: 实现日志读取
- LogWriter: 实现日志写入
- SegmentHandle: 处理段操作

**接口设计**:
```go
// LogReader 定义日志读取接口
type LogReader interface {
    ReadNext(ctx context.Context) (*LogEntry, error)
    HasNext() bool
    Close() error
}

// LogWriter 定义日志写入接口
type LogWriter interface {
    Append(ctx context.Context, data []byte) (int64, error)
    Flush() error
    Close() error
}
```

### 4.5 阶段五: 性能优化与可靠性

**目标**: 优化系统性能，提高可靠性

**重点领域**:
- 并发控制: 优化锁机制，减少竞争
- 批处理: 实现批量操作以提高吞吐量
- 错误处理: 完善错误处理和恢复机制
- 指标监控: 增强系统监控能力

## 5. 子模块详细设计

### 5.1 片段管理 (Fragment Management)

#### 5.1.1 设计目标
- 高效管理片段的生命周期
- 优化片段的加载和持久化
- 控制内存使用，防止 OOM

#### 5.1.2 核心组件
- **FragmentObject**: 实现片段的基本操作
- **FragmentManager**: 管理片段缓存
- **缓存策略**: 实现基于 LRU 的缓存淘汰

#### 5.1.3 优化方向
- 异步加载: 支持片段的异步预加载
- 压缩算法: 实现片段数据压缩
- 并发控制: 优化片段并发访问性能

### 5.2 日志文件 (LogFile)

#### 5.2.1 设计目标
- 提供高性能的日志读写
- 确保数据持久性和一致性
- 支持灵活的配置和优化

#### 5.2.2 核心组件
- **缓冲区管理**: 管理内存缓冲区
- **同步策略**: 控制数据同步到存储层
- **读取优化**: 优化数据读取性能

#### 5.2.3 优化方向
- 写入合并: 合并小写入以提高效率
- 预读策略: 实现智能预读以提高读性能
- 并发控制: 优化并发读写性能

### 5.3 缓存系统 (Cache System)

#### 5.3.1 设计目标
- 减少对象存储访问，提高性能
- 控制内存使用，防止内存溢出
- 提供灵活的缓存策略

#### 5.3.2 核心组件
- **缓存项管理**: 管理缓存项的生命周期
- **淘汰算法**: 实现 LRU 等淘汰算法
- **内存监控**: 监控和控制内存使用

#### 5.3.3 优化方向
- 分级缓存: 实现多级缓存架构
- 自适应策略: 根据负载自动调整缓存策略
- 并发优化: 减少缓存操作中的锁竞争

## 6. 线程安全与并发控制

### 6.1 当前实现

目前系统使用以下机制确保线程安全:
- 使用互斥锁 (mutex) 保护共享资源
- 使用原子操作处理简单的计数器
- 实现特定的锁定模式以避免死锁

### 6.2 已知问题

- 在高并发场景下，锁竞争可能成为性能瓶颈
- 某些操作中存在潜在的竞态条件
- 嵌套锁可能导致死锁风险

### 6.3 改进方向

- 细化锁的粒度，减少锁定范围
- 实现读写锁，提高并发读取性能
- 采用无锁数据结构，减少锁竞争
- 系统性地审核所有并发控制点

## 7. 配置与调优

### 7.1 关键配置参数

- **MaxEntries**: 缓冲区最大条目数
- **MaxBytes**: 缓冲区最大字节数
- **MaxInterval**: 最大同步间隔 (毫秒)
- **MaxFlushThreads**: 最大刷新线程数
- **MaxFlushSize**: 单个片段最大大小
- **MaxMemory**: 缓存最大内存使用

### 7.2 性能调优建议

- 根据工作负载调整缓冲区大小
- 优化刷新策略以平衡吞吐量和延迟
- 调整缓存大小以匹配可用内存
- 并发参数调优以适应不同的硬件配置

### 7.3 监控指标

- 读写延迟和吞吐量
- 缓存命中率和内存使用
- 同步操作频率和延迟
- 对象存储操作耗时

## 8. 扩展与未来发展

### 8.1 短期目标

- 完善测试覆盖率，提高系统稳定性
- 优化内存使用，减少资源消耗
- 增强监控和诊断能力

### 8.2 中期目标

- 支持更多存储后端 (除 MinIO 外)
- 实现更高级的缓存策略
- 提供更丰富的客户端接口和工具

### 8.3 长期愿景

- 分布式部署支持
- 高级数据分析和查询能力
- 与更多生态系统集成

## 9. 对接指南

### 9.1 开发新功能

1. 确定目标功能属于哪个模块和阶段
2. 查阅相关模块的接口设计和实现细节
3. 编写设计文档，描述新功能的实现方式
4. 实现并测试功能，确保与现有系统无缝集成
5. 更新文档，反映新功能的使用方法和配置选项

### 9.2 优化现有模块

1. 收集性能数据和用户反馈
2. 确定瓶颈和改进点
3. 设计优化方案，评估影响范围
4. 实现并测试优化，确保兼容性
5. 更新文档，记录优化效果和新的最佳实践

### 9.3 跨模块协作

1. 确定涉及的模块和责任边界
2. 明确模块间的接口和数据流
3. 协调开发计划和测试策略
4. 实现各自部分，并进行集成测试
5. 共同维护文档，确保系统整体一致性

## 10. 参考文档

- [Fragment 接口定义](server/storage/fragment.go)
- [LogFile 实现](server/storage/objectstorage/log_file_impl.go)
- [FragmentManager 实现](server/storage/cache/fragment_manager.go)
- [Woodpecker 配置说明](common/config/config.go)

*注意: 本设计文档应随着项目的演进而更新，确保其反映最新的设计决策和实现细节。*
