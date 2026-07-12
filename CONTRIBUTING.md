# Contributing to parquet-arrow-java

Thanks for your interest in parquet-arrow-java — a pure-JVM, Hadoop-free bridge
between Apache Arrow and Apache Parquet. This guide covers how to build, test,
and submit changes. For the public contract and supported types, see
[`README.md`](README.md).

## Setting up

You need a **JDK 17** toolchain. Every module builds on 17; the Gradle wrapper
is checked in, so no separate Gradle install is required.

```bash
git clone git@github.com:mprammer/parquet-arrow-java.git
cd parquet-arrow-java
./gradlew check
```

## Before you open a PR

The gate CI runs is:

```bash
./gradlew check          # compiles both modules and runs the full test suite
```

The real-corpus conformance test (`RaincloudArrowToParquetTest`) is opt-in and
skips unless `PA_ARROW_CORPUS` points at a directory of `*.arrow.zstd` files, so
`check` is fully hermetic without it. If you have the corpus locally:

```bash
PA_ARROW_CORPUS=/path/to/arrow/corpus ./gradlew :parquet-arrow-conformance:test
```

## Conventions

- **Correctness is proven, not asserted.** New behaviour needs a test that fails
  without the change. The conformance module carries a semantic Arrow oracle
  (`ArrowLogicalComparator`) and physical-trace oracles — prefer routing new
  coverage through them over shallow equality checks.
- **Standard Parquet only.** The contract is Arrow-in-memory ⇆ standard Parquet.
  Types Parquet cannot represent are rejected with a typed
  `ParquetArrowException`, not silently coerced. See the supported-types section
  of the README before adding a mapping.
- **Stay Hadoop-free.** The core must not gain a real `org.apache.hadoop`
  dependency; the vendored parquet-floor shims exist precisely to avoid it. The
  build asserts a Hadoop-free runtime classpath — don't defeat that check.
- **SPDX headers.** Every source file carries the Apache-2.0 SPDX header; match
  the surrounding style.

## Publishing locally

There is no remote Maven repository. To consume a local build:

```bash
./gradlew publishToMavenLocal
# then depend on dev.spiraldb.parquet.arrow:parquet-arrow-core:<version>
```

## License

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE).
