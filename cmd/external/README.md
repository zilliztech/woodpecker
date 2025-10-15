# External Configuration Parser

This package provides a compatibility layer for external configuration files to override Woodpecker's internal configuration. It primarily supports parsing external configurations like Milvus `user.yaml` files.

## Overview

The `external` package enables Woodpecker to accept configuration overrides from external YAML files that may use different format conventions. This is particularly useful when:

1. Integrating Woodpecker with existing systems (like Milvus) that use their own configuration formats
2. Providing user-friendly configuration with human-readable units (e.g., "256M", "10s")
3. Allowing external systems to override Woodpecker's default settings without modifying internal configuration files

**Key Feature**: This package extends support for Milvus-style `user.yaml` configuration parsing.

## Design Philosophy

### Reuse Internal Structures

To minimize code duplication and maintain consistency, we **reuse internal configuration structures** from `common/config` wherever possible:

- ✅ **Etcd**: Directly uses `config.EtcdConfig`
- ✅ **Minio**: Directly uses `config.MinioConfig`
- ✅ **Log**: Directly uses `config.LogConfig`
- ✅ **Trace**: Directly uses `config.TraceConfig`
- ✅ **Meta**: Directly uses `config.MetaConfig`

### Custom Structures Only Where Needed

We only define custom structures for parts that have **format differences** in external configurations:

- 🔧 **Client**: Custom `UserClientConfig` because quorum configuration format differs
- 🔧 **Logstore**: Custom `UserLogstoreConfig` because it uses human-readable units (e.g., "256M", "200ms")
- 🔧 **Storage**: Custom `UserStorageConfig` for format flexibility

## Features

- **Configuration Override**: External configurations override Woodpecker's internal default settings
- **Milvus Compatibility**: Supports Milvus `user.yaml` format for seamless integration
- **Unit Parsing**: Human-readable units like "256M", "2G" for sizes and "10s", "5m" for durations
- **Structure Reuse**: Leverages existing internal config structures where formats are compatible
- **Safe Merging**: Non-empty external values override defaults without destructive changes

## Supported Configuration Sections

### 1. Woodpecker Configuration
```yaml
woodpecker:
  meta:           # ✅ Reuses config.MetaConfig
    type: etcd
    prefix: woodpecker
  
  client:         # 🔧 Custom (quorum format differs)
    segmentAppend:
      queueSize: 10000
    segmentRollingPolicy:
      maxSize: 256M      # Human-readable units
      maxInterval: 10m
    quorum:
      quorumBufferPools:  # External format
        - name: pool-1
          seeds: []
  
  logstore:       # 🔧 Custom (uses human-readable units)
    segmentSyncPolicy:
      maxInterval: 200ms  # Milliseconds
      maxBytes: 256M
  
  storage:        # 🔧 Custom
    type: minio
    rootPath: /var/lib/woodpecker
```

### 2. System Configuration (Direct Reuse)
```yaml
log:              # ✅ config.LogConfig
  level: info
  format: text

trace:            # ✅ config.TraceConfig
  exporter: noop
  sampleFraction: 1

etcd:             # ✅ config.EtcdConfig
  endpoints:
    - localhost:2379
  rootPath: woodpecker

minio:            # ✅ config.MinioConfig
  address: localhost
  port: 9000
  bucketName: a-bucket
```

### 3. Additional Sections
```yaml
localStorage:     # Local storage path
  path: /var/lib/milvus/data/

mq:               # Message queue type
  type: default
```

## Usage

### Basic Usage

```go
import (
    "github.com/zilliztech/woodpecker/cmd/external"
    "github.com/zilliztech/woodpecker/common/config"
)

// Load external configuration
userConfig, err := external.LoadUserConfig("user_config.yaml")
if err != nil {
    log.Fatalf("Failed to load user config: %v", err)
}

// Load base Woodpecker configuration
wpConfig, err := config.NewConfiguration("woodpecker.yaml")
if err != nil {
    log.Fatalf("Failed to load woodpecker config: %v", err)
}

// Apply user configuration overrides
if err := userConfig.ApplyToConfig(wpConfig); err != nil {
    log.Fatalf("Failed to apply user config: %v", err)
}

// Now wpConfig has merged settings from both files
```

### Configuration Priority

When using both configuration files:
1. Default Woodpecker configuration is loaded first
2. External user configuration is applied as overrides
3. Only non-empty values from user configuration override defaults

## Configuration Format Differences

### Size Format

**External Format** (user config):
```yaml
woodpecker:
  client:
    segmentRollingPolicy:
      maxSize: 256M  # Human-readable format
```

**Internal Format** (woodpecker config):
```yaml
woodpecker:
  client:
    segmentRollingPolicy:
      maxSize: 268435456  # Bytes
```

### Duration Format

**External Format** (user config):
```yaml
woodpecker:
  client:
    segmentRollingPolicy:
      maxInterval: 10m  # Human-readable format (minutes)
```

**Internal Format** (woodpecker config):
```yaml
woodpecker:
  client:
    segmentRollingPolicy:
      maxInterval: 600  # Seconds
```

### Millisecond Duration Format

**External Format** (user config):
```yaml
woodpecker:
  logstore:
    segmentSyncPolicy:
      maxInterval: 200ms  # Milliseconds
```

**Internal Format** (woodpecker config):
```yaml
woodpecker:
  logstore:
    segmentSyncPolicy:
      maxInterval: 200  # Milliseconds
```

## Unit Formats

### Size Units
- `K` or `k`: Kilobytes (1024 bytes)
- `M` or `m`: Megabytes (1024 * 1024 bytes)
- `G` or `g`: Gigabytes (1024 * 1024 * 1024 bytes)
- `T` or `t`: Terabytes (1024 * 1024 * 1024 * 1024 bytes)
- No unit: Raw bytes

Examples: `256M`, `2G`, `1024`, `512K`

### Duration Units
- `ms`: Milliseconds
- `s`: Seconds
- `m`: Minutes
- `h`: Hours
- No unit: Raw value (depends on context)

Examples: `200ms`, `10s`, `5m`, `1h`

## Error Handling

The parser validates:
- Unit format correctness
- Numeric value validity
- Configuration structure

Invalid formats will return descriptive errors indicating what went wrong.

## Examples

See `user_simple_example.yaml` for a complete example of external configuration format that can be used with this parser.

## Integration with main.go

To integrate with Woodpecker's main.go:

```go
// Add command-line flag for user config
userConfigFile := flag.String("user-config", "", "External user configuration file")

// Load configurations
cfg, err := config.NewConfiguration(*configFile)
if err != nil {
    log.Fatalf("Failed to load configuration: %v", err)
}

// Apply user config overrides if provided
if *userConfigFile != "" {
    userCfg, err := external.LoadUserConfig(*userConfigFile)
    if err != nil {
        log.Fatalf("Failed to load user config: %v", err)
    }
    if err := userCfg.ApplyToConfig(cfg); err != nil {
        log.Fatalf("Failed to apply user config: %v", err)
    }
    log.Printf("Applied user configuration from: %s", *userConfigFile)
}

// Continue with server initialization using merged cfg
```

## Benefits

1. **External Override**: Allows external configurations (like Milvus `user.yaml`) to override Woodpecker defaults
2. **Milvus Integration**: Extends Woodpecker to understand Milvus configuration format
3. **User-Friendly**: Human-readable units (256M vs. 268435456)
4. **Code Reuse**: Leverages existing internal structures where possible
5. **Safe**: Non-destructive merging preserves existing config
6. **Validated**: Comprehensive test coverage

## Testing

Run tests:
```bash
go test ./cmd/external/...
```

The test suite covers:
- Unit parsing functions (size, duration, milliseconds)
- Configuration loading from files
- Configuration merging and overrides
- Quorum configuration
- Logstore configuration
