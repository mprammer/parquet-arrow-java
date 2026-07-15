# Changelog

All notable changes to parquet-arrow-java are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2]

### Fixed

- Documentation only: the README still advertised the `0.1.0` dependency
  coordinate (Gradle + Maven snippets and the "cut" note) after 0.1.1 shipped.
  Bumped every version reference to match the released artifact. No code change.

## [0.1.1]

### Fixed

- Reader rejected valid Parquet files containing an **empty-named column** (`""`,
  e.g. an unnamed index column) with `InvalidParquetFileException`. The write side
  already produced valid files for these; the default all-columns projection built
  a `FieldPath` from the empty name and `FieldPath.of` banned empty segments.
  Empty-string segments are now accepted (they are valid Arrow/Parquet field
  names); only null segments are rejected.

## [0.1.0]

Initial public release: a pure-JVM, Hadoop-free bidirectional bridge between
in-memory Apache Arrow and standard Apache Parquet.

### Added

- `ParquetArrow.writer(schema)` / `ParquetArrow.reader(allocator)` — write an
  Arrow `VectorSchemaRoot` to standard Parquet and read Parquet back into
  reusable Arrow vectors, streaming a batch at a time.
- Full type coverage: booleans; signed/unsigned integers; float32/float64;
  UTF-8 and binary (incl. large-offset and view variants); fixed-size binary;
  decimal128/decimal256; dates, times, timestamps; duration; structs; canonical
  three-level lists and maps; and Arrow dictionary inputs (translated as values).
- Hadoop-free operation via vendored minimal `org.apache.hadoop.*` shims
  (from parquet-floor); no Hadoop dependency artifact is required at runtime.
- Configurable compression (`UNCOMPRESSED`, `ZSTD`, `SNAPPY`, `GZIP`) via a
  core-owned codec factory, data-page V1/V2 selection, and row-group controls.
- `ARROW:schema` footer metadata is consumed as an advisory hint only; the
  Parquet physical/logical types define the readable value domain.
- Typed rejection (`ParquetArrowException` subtypes) for constructs standard
  Parquet cannot represent (Float16, unions, run-end encoding, list views,
  non-canonical legacy nested encodings, duplicate sibling field names).

### Notes

- Arrow SECOND-precision times and timestamps are promoted to Parquet MILLIS
  (values scale, the instant is preserved), since Parquet has no
  second-precision temporal type.
- Publishing is local-only (`publishToMavenLocal`); there is no remote Maven
  repository.

[0.1.2]: https://github.com/mprammer/parquet-arrow-java/releases/tag/v0.1.2
[0.1.1]: https://github.com/mprammer/parquet-arrow-java/releases/tag/v0.1.1
[0.1.0]: https://github.com/mprammer/parquet-arrow-java/releases/tag/v0.1.0
