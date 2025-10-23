# External User Configuration

This package is primarily used by **external users** to override Woodpecker's default configuration through configuration files.

## Design Purpose

1. **Compatibility**: Supports configuration file formats similar to Milvus (including additional fields like `localStorage`)
2. **Ease of Use**: External users only need to specify the configuration items they want to modify, keeping defaults for others
3. **Maintainability**: Uses **explicit field assignment** instead of reflection, making the code clear, readable, and easy to maintain

## Supported Configuration Modules

- **Woodpecker**: Core Woodpecker configuration (meta, client, logstore, storage)
- **Etcd**: Metadata storage configuration
- **Minio**: Object storage configuration
- **Log**: Logging configuration
- **Trace**: Distributed tracing configuration
- **LocalStorage**: Local storage path (for Milvus compatibility, used as woodpecker's basepath when necessary)

## Quick Start

```go
import (
    "github.com/zilliztech/woodpecker/common/config"
    "github.com/zilliztech/woodpecker/cmd/external"
)

// 1. Load default configuration
cfg, _ := config.NewConfiguration("config/woodpecker.yaml")

// 2. Load and apply user configuration
userConfig, _ := external.LoadUserConfig("user_config.yaml")
userConfig.ApplyToConfig(cfg)

// cfg now contains the merged configuration
```
