# Woodpecker Build System

This directory contains the build system for Woodpecker, supporting cross-platform binary compilation and Docker image building.

## 📁 Directory Structure

```
build/
├── README.md              # This documentation
├── build_bin.sh           # Cross-architecture binary compilation
├── build_image.sh          # Cross-platform Docker image building
└── docker/
    ├── scripts/            # Shared Docker scripts
    │   ├── start-woodpecker.sh
    │   └── health-check.sh
    ├── ubuntu20.04/        # Ubuntu 20.04 Dockerfile
    ├── ubuntu22.04/        # Ubuntu 22.04 Dockerfile
    │   └── Dockerfile
    ├── amazonlinux2023/    # Amazon Linux 2023 Dockerfile
    └── rockylinux8/        # Rocky Linux 8 Dockerfile
```

## 🚀 Quick Start

### Build Binary for Current Architecture
```bash
./build/build_bin.sh
```

### Build Docker Image for Current Architecture
```bash
./build/build_image.sh
```

## 📖 Binary Building (`build_bin.sh`)

### Overview
The `build_bin.sh` script compiles Woodpecker binaries for different CPU architectures.

### Usage
```bash
./build/build_bin.sh [ARCH]
```

### Parameters
- `ARCH`: Target architecture (optional)
  - `auto` (default): Auto-detect current system architecture
  - `amd64`: Build for AMD64/x86-64
  - `arm64`: Build for ARM64/AArch64

### Examples

#### Auto-detect Architecture
```bash
# Build for current system architecture
./build/build_bin.sh
```

#### Specific Architecture
```bash
# Build for AMD64/x86-64
./build/build_bin.sh amd64

# Build for ARM64
./build/build_bin.sh arm64
```

### Output
- Binary location: `bin/woodpecker`
- The script will display the binary's architecture and size
- Exit code 0 on success, non-zero on failure

## 🐳 Docker Image Building (`build_image.sh`)

### Overview
The `build_image.sh` script builds Woodpecker Docker images for different operating systems and architectures.

### Usage
```bash
./build/build_image.sh [OS] [ARCH] [OPTIONS]
```

### Parameters

#### OS (Operating System)
- `ubuntu22.04` (default): Ubuntu 22.04 LTS
- `ubuntu20.04`: Ubuntu 20.04 LTS
- `amazonlinux2023`: Amazon Linux 2023
- `rockylinux8`: Rocky Linux 8

#### ARCH (Architecture)
- `auto` (default): Auto-detect current system architecture
- `amd64`: Build for AMD64/x86-64
- `arm64`: Build for ARM64/AArch64
- `multiarch`: Build for multiple architectures using Docker Buildx

#### Options
- `-t, --tag TAG`: Custom image tag (default: `woodpecker:latest`)
- `-m, --multiarch`: Enable multi-architecture build
- `--no-cache`: Build without using Docker cache
- `--push`: Push to registry (requires multiarch build)
- `-h, --help`: Show help message

### Examples

#### Basic Usage
```bash
# Build Ubuntu 22.04 image for current architecture
./build/build_image.sh

# Build Ubuntu 20.04 image for current architecture
./build/build_image.sh ubuntu20.04
```

#### Architecture-Specific Builds
```bash
# Build Ubuntu 22.04 for ARM64
./build/build_image.sh ubuntu22.04 arm64

# Build Amazon Linux 2023 for AMD64
./build/build_image.sh amazonlinux2023 amd64
```

#### Custom Tags
```bash
# Build with custom tag
./build/build_image.sh ubuntu22.04 auto -t myregistry/woodpecker:v1.0.0

# Build Rocky Linux 8 with version tag
./build/build_image.sh rockylinux8 arm64 -t woodpecker:rocky8-arm64
```

#### Multi-Architecture Builds
```bash
# Build for both AMD64 and ARM64
./build/build_image.sh ubuntu22.04 multiarch -t woodpecker:latest

# Build multi-arch and push to registry
./build/build_image.sh ubuntu22.04 multiarch -t myregistry/woodpecker:v1.0.0 --push
```

#### Cache Control
```bash
# Build without cache
./build/build_image.sh ubuntu22.04 amd64 --no-cache

# Build Ubuntu 20.04 without cache and custom tag
./build/build_image.sh ubuntu20.04 arm64 --no-cache -t woodpecker:clean-build
```

## 🔧 Prerequisites

### For Binary Building
- Go 1.19+ installed
- Make utility
- Access to the project's Makefile

### For Docker Image Building
- Docker Engine installed and running
- Docker Buildx (for multi-architecture builds)
- Appropriate permissions to build and tag Docker images

### Multi-Architecture Building
For multi-architecture builds, ensure Docker Buildx is properly configured:

```bash
# Check if buildx is available
docker buildx version

# Create a new builder if needed
docker buildx create --name woodpecker-builder --use

# Verify builder supports multiple platforms
docker buildx inspect --bootstrap
```

## 📋 Build Process

### Binary Build Process
1. **Architecture Detection**: Auto-detect or validate specified architecture
2. **Environment Setup**: Configure Go build environment (GOOS, GOARCH)
3. **Compilation**: Execute appropriate Make target
4. **Verification**: Check binary exists and display metadata
5. **Output**: Report build success with binary information

### Docker Build Process
1. **Validation**: Validate OS and architecture parameters
2. **Dockerfile Location**: Locate appropriate Dockerfile for target OS
3. **Binary Preparation**: Ensure binary exists for target architecture
4. **Image Building**: Execute Docker build with appropriate platform flags
5. **Verification**: Confirm image was created successfully
6. **Tagging**: Apply specified tags to the built image

## 🔍 Troubleshooting

### Common Issues

#### Binary Build Failures
```bash
# Error: Make target not found
# Solution: Ensure you're in the project root and Makefile exists
ls -la Makefile

# Error: Go not installed
# Solution: Install Go 1.19+
go version
```

#### Docker Build Failures
```bash
# Error: Dockerfile not found
# Solution: Check if Dockerfile exists for the specified OS
ls -la build/docker/ubuntu22.04/Dockerfile

# Error: Binary not found during Docker build
# Solution: Build binary first
./build/build_bin.sh

# Error: Docker Buildx not available
# Solution: Install Docker Buildx or use single-arch build
docker buildx version
```

#### Multi-Architecture Build Issues
```bash
# Error: Platform not supported
# Solution: Check available platforms
docker buildx inspect --bootstrap

# Error: Cannot push without registry
# Solution: Tag with registry prefix
./build/build_image.sh ubuntu22.04 multiarch -t registry.example.com/woodpecker:latest --push
```

## 🎯 Best Practices

### Development Workflow
1. **Local Development**: Use auto-detection for quick builds
   ```bash
   ./build/build_bin.sh
   ./build/build_image.sh
   ```

2. **Cross-Platform Testing**: Build for different architectures
   ```bash
   ./build/build_bin.sh amd64
   ./build/build_bin.sh arm64
   ```

3. **Release Preparation**: Use multi-architecture builds
   ```bash
   ./build/build_image.sh ubuntu22.04 multiarch -t woodpecker:v1.0.0
   ```

### Performance Tips
- **Use Cache**: Avoid `--no-cache` unless necessary for clean builds
- **Parallel Builds**: Build different OS variants in parallel
- **Registry Proximity**: Use local or nearby container registries

### Security Considerations
- **Minimal Base Images**: Choose appropriate base OS for security
- **Regular Updates**: Keep base images updated
- **Secret Management**: Never embed secrets in images

## 📚 Additional Resources

- **Docker Multi-Platform**: [Docker Buildx Documentation](https://docs.docker.com/buildx/)
- **Go Cross-Compilation**: [Go Cross Compilation Guide](https://golang.org/doc/install/source#environment)
- **Container Security**: [Docker Security Best Practices](https://docs.docker.com/develop/security-best-practices/)

## 🤝 Contributing

When adding new OS support:
1. Create new directory under `build/docker/[os-name]/`
2. Add Dockerfile with consistent structure
3. Update this README with new OS option
4. Test build process on target platform

For build script improvements:
1. Follow existing error handling patterns
2. Maintain backward compatibility
3. Add appropriate validation
4. Update documentation
