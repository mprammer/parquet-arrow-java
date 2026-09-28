# Changelog

All notable changes to parquet-arrow-java are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Parquet `VARIANT`: a struct carrying the Arrow canonical extension
  `arrow.parquet.variant` is written as a `VARIANT`-annotated group (parquet-java
  1.17.1's `VariantLogicalTypeAnnotation`, specification version 1), and the
  reader maps a `VARIANT` group to that struct with the extension in its field
  metadata, where it used to reject the file ("unrecognized group annotation
  VARIANT"). The Parquet logical type outranks the advisory `ARROW:schema`
  hint's metadata for that field. A storage struct that is not a `VARIANT`
  group's shape (a binary `metadata`, a binary `value` and/or `typed_value`) is
  rejected on either side.

### Changed

- The writer names list and map levels as parquet-format LogicalTypes.md
  requires (`list`/`element`, `key_value`/`key`/`value`) whatever the Arrow
  names are; it used to write the Arrow element, key and value names. The
  Arrow names are restored from `ARROW:schema` on read, so an Arrow schema
  still round-trips exactly. `ListEncodingResolver.Reason` loses the codes of
  the shapes now read (`TWO_LEVEL`, `REPEATED_PRIMITIVE`, `CHILD_NAME`,
  `LEGACY_*`, ...) and `MapEncodingResolver.Reason` loses `MAP_KEY_VALUE_OUTER`.

### Fixed

- Reader rejected valid Parquet lists whose element is not named `element` or
  `item` (`UnsupportedNestedEncodingException: CHILD_NAME`), including files the
  bridge itself wrote from an Arrow list whose element had another name, and
  every 2-level or legacy list. It now reads lists and maps as LogicalTypes.md
  directs: level names are not enforced, 2-level/legacy lists resolve by the
  five backward-compatibility rules, and a `MAP_KEY_VALUE` group outside a
  `MAP` reads as a map. An unannotated repeated field, which the reader used
  to read as one value per row (dropping the rest), is now rejected with
  `UNANNOTATED_REPEATED`.
- Reader failed on a valid file whose batch ended in **null struct rows over a
  variable-width child** ("invalid Parquet page encoding", caused by an
  `IndexOutOfBoundsException`): a null struct row never started the struct's
  converter, so its children had no slot for that row, and the batch's
  variable-width byte accounting read a child offset past its buffer. A null
  struct row now indexes its children as null too.

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
