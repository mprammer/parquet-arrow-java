# Provenance of vendored / adapted code

| Local path | Upstream | Version/ref | License | Purpose |
|---|---|---|---|---|
| `parquet-arrow-core/src/main/java/org/apache/hadoop/**` (28 files) | [strategicblue/parquet-floor](https://github.com/strategicblue/parquet-floor) | master @ vendored 2026-07-11 | Apache-2.0 | Minimal empty/stub `org.apache.hadoop.*` classes (conf, fs, io.compress, mapreduce) so parquet-java 1.17.1's `ParquetReadOptions`/`ParquetFileReader` class-load without the Hadoop dependency tree. Empty stubs — no Hadoop behavior is invoked on the local-file path. |
| `VectorWritePlan`, `CoreParquetArrowReader`, and related composite node code | [Apache Iceberg](https://github.com/apache/iceberg) | 1.11.0 patterns; see `ParquetValueWriters` and `ParquetValueReaders` | Apache-2.0 | Adapted visitor/composite structure for primitive, struct, list, and map values. parquet-arrow-java carries Arrow vector indices and does not copy Iceberg's table model or row model. |
| `ParquetArrowWriter` and direct file/page lifecycle | [Apache parquet-java](https://github.com/apache/parquet-java) | 1.17.1; `MessageColumnIO`, `ParquetFileWriter`, `ColumnChunkPageWriteStore`, `LocalInputFile`, and `LocalOutputFile` | Apache-2.0 | Adapted orchestration around the public low-level writer and reader contracts. The dependency is consumed as parquet-java; no parquet-java source is vendored here. |
| `ParquetArrowReader`, `ArrowSchemaMetadataCodec`, and example consumer code | [Apache Arrow Java](https://github.com/apache/arrow-java) | 19.0.0; `ArrowReader` and IPC `MessageSerializer` | Apache-2.0 | Uses the Arrow reader lifecycle and schema-message encoding contracts. No Arrow Java source is copied into this repository. |

The three upstream projects above are also called out in the root
[`NOTICE`](../NOTICE). Where this repository adapts an upstream pattern rather
than copying source, the local files are identified by role and the upstream
class or surface is named so the relationship remains auditable.

## Pinned dependency set

The core build uses Gradle dependency locking. The 0.1.0 release pins the
direct runtime set below; the committed lock state is the authority for the
resolved graph used by the core module.

| Dependency | Pinned version | Role |
|---|---:|---|
| Apache Arrow Java (`arrow-vector`, `arrow-memory-core`, `arrow-memory-netty`) | 19.0.0 | Public vectors and allocator/runtime |
| Apache parquet-java (`parquet-hadoop`) | 1.17.1 | Parquet format/file/page implementation |
| `com.github.luben:zstd-jni` | 1.5.7-6 | Zstandard codec |
| `org.xerial.snappy:snappy-java` | 1.1.10.7 | Snappy codec |
| `org.slf4j:slf4j-nop` | 2.0.18 | Silent runtime binding |

`parquet-hadoop`'s Hadoop dependencies are excluded at the Gradle declaration
and are replaced only by the minimal source shims described above. The core
Hadoop-free check also inspects runtime JAR contents for `org/apache/hadoop/**`.
