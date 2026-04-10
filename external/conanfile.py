from conans import ConanFile


class MilvusConan(ConanFile):
    keep_imports = True
    settings = "os", "compiler", "build_type", "arch"
    # Versions/options aligned to milvus-io/milvus-storage@b86636f cpp/conanfile.py.
    # We keep extra packages that woodpecker's external/src/ uses (nlohmann_json, etc.)
    # but bump anything that overlaps with upstream so the linker sees one consistent ABI.
    requires = (
        "boost/1.83.0@",
        "onetbb/2021.9.0#4a223ff1b4025d02f31b65aedf5e7f4a",
        "nlohmann_json/3.11.3#ffb9e9236619f1c883e36662f944345d",
        "zstd/1.5.5#34e9debe03bf0964834a09dfbc31a5dd",
        "lzo/2.10#9517fc1bcc4d4cc229a79806003a1baa",
        "arrow/17.0.0@milvus/dev-2.6#7af258a853e20887f9969f713110aac8",
        "aws-sdk-cpp/1.11.692@milvus/dev#1e17deac19383217d291a01c23147b33",
        "googleapis/cci.20221108#65604e1b3b9a6b363044da625b201a2a",
        "benchmark/1.8.3",
        "gtest/1.15.0",
        "rapidxml/1.13#10c11a4bfe073e131ed399d5c4f2e075",
        "yaml-cpp/0.7.0#9c87b3998de893cf2e5a08ad09a7a6e0",
        "marisa/0.2.6#68446854f5a420672d21f21191f8e5af",
        "libcurl/8.10.1",
        "glog/0.7.1",
        "fmt/11.0.2",
        "gflags/2.2.2#b15c28c567c7ade7449cf994168a559f",
        "double-conversion/3.2.1#640e35791a4bac95b0545e2f54b7aceb",
        "libevent/2.1.12#4fd19d10d3bed63b3a8952c923454bc0",
        "libdwarf/20191104#7f56c6c7ccda5fadf5f28351d35d7c01",
        "libiberty/9.1.0#3060045a116b0fff6d4937b0fc9cfc0e",
        "libsodium/cci.20220430#7429a9e5351cc67bea3537229921714d",
        "xz_utils/5.4.5",
        "prometheus-cpp/1.1.0#ea9b101cb785943adb40ad82eda7856c",
        "re2/20230301#f8efaf45f98d0193cd0b2ea08b6b4060",
        "folly/2024.08.12.00@milvus/dev#e09fc71826ce6b4568441910665f0889",
        "google-cloud-cpp/2.28.0@milvus/dev#25e69d743269d6c9ae5bf676af2174dc",
        "roaring/3.0.0#25a703f80eda0764a31ef939229e202d",
        "rapidjson/cci.20230929#624c0094d741e6a3749d2e44d834b96c",
        "simde/0.8.2#5e1edfd5cba92f25d79bf6ef4616b972",
        "xxhash/0.8.3#199e63ab9800302c232d030b27accec0",
        "unordered_dense/4.4.0#6a855c992618cc4c63019109a2e47298",
        "mongo-cxx-driver/3.11.0#ae206de0e90fb8cb2fb95465fb8b2f01",
        "geos/3.12.0#0b177c90c25a8ca210578fb9e2899c37",
        "libavrocpp/1.12.1.1@milvus/dev#a77043b1b435c3abef7b45710d05b300",
        # opentelemetry-cpp dropped: pulls protobuf 3.21 transitively, conflicts with
        # the protobuf 5.27 force-override below; woodpecker external/src/ doesn't use it.
    )
    generators = ("cmake", "cmake_find_package")
    default_options = {
        "libevent:shared": True,
        "double-conversion:shared": True,
        "folly:shared": True,
        "arrow:filesystem_layer": True,
        "arrow:parquet": True,
        "arrow:compute": True,
        "arrow:with_re2": True,
        "arrow:with_zstd": True,
        "arrow:with_boost": True,
        "arrow:with_thrift": True,
        "arrow:with_jemalloc": True,
        "arrow:with_openssl": True,
        "arrow:shared": False,
        "arrow:with_azure": True,
        "arrow:with_s3": True,
        "arrow:encryption": True,
        "arrow:dataset_modules": True,
        # Added to match upstream cpp/conanfile.py.
        "arrow:with_snappy": True,
        "arrow:with_lz4": True,
        "aws-sdk-cpp:config": True,
        "aws-sdk-cpp:text-to-speech": False,
        "aws-sdk-cpp:transfer": False,
        "aws-sdk-cpp:s3-crt": True,
        "gtest:build_gmock": True,
        "boost:without_locale": False,
        "boost:without_test": True,
        # Added to match upstream cpp/conanfile.py.
        "boost:without_stacktrace": True,
        "glog:with_gflags": True,
        "glog:shared": True,
        # Added to match upstream cpp/conanfile.py.
        "gflags:shared": True,
        "openssl:shared": True,
        # xz_utils must be shared because glog (shared) depends on liblzma; if static,
        # symbol resolution fails at link time. Comment copied from upstream.
        "xz_utils:shared": True,
        "prometheus-cpp:with_pull": False,
        # Upstream uses header_only=False; we follow.
        "fmt:header_only": False,
        "onetbb:tbbmalloc": False,
        "onetbb:tbbproxy": False,
        "gdal:shared": True,
        "gdal:fPIC": True,
    }

    def configure(self):
        if self.settings.arch not in ("x86_64", "x86"):
            del self.options["folly"].use_sse4_2
        if self.settings.os == "Macos":
            # By default abseil uses static link but is not compatible with macos x86
            self.options["abseil"].shared = True
            self.options["arrow"].with_jemalloc = False

    def requirements(self):
        # Force-override transitive deps to align with milvus-storage upstream b86636f
        # cpp/conanfile.py. Some dep recipes (e.g. folly, libavrocpp) pin older versions
        # of these packages; without override=True, conan errors with version conflicts.
        self.requires("protobuf/5.27.0@milvus/dev#6fff8583e2fe32babef04a9097f1d581", override=True)
        self.requires("grpc/1.67.1@milvus/dev#5aa62c51bced448b83d7db9e5b3a13c7", override=True)
        self.requires("abseil/20250127.0", override=True)
        self.requires("snappy/1.2.1", override=True)
        self.requires("lz4/1.9.4", override=True)
        self.requires("openssl/3.3.2", override=True)
        self.requires("zlib/1.3.1", override=True)
        if self.settings.os != "Macos":
            self.requires("libunwind/1.8.1")

    def imports(self):
        self.copy("*.dylib", "../lib", "lib")
        self.copy("*.dll", "../lib", "lib")
        self.copy("*.so*", "../lib", "lib")
        self.copy("*", "../bin", "bin")
        self.copy("*.proto", "../include", "include")
