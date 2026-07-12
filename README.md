# parquet-arrow-java

> **Note:** This repository is effectively entirely AI-generated — its design,
> implementation, tests, and documentation were produced by LLM agents working
> under human direction and review. It builds cleanly and its test suite passes,
> but read the code before you depend on it.

`parquet-arrow-java` is the missing pure-JVM, Hadoop-free bridge between Apache Arrow
and Apache Parquet. It writes an Arrow `VectorSchemaRoot` to a Parquet file and
reads Parquet back into reusable Arrow vectors, without requiring a Hadoop
dependency JAR in a consumer's runtime.

The core module uses parquet-java 1.17.1 under the hood. Its dependency on the
`parquet-hadoop` artifact is paired with the minimal `org.apache.hadoop.*`
shims needed by parquet-java's class-loading paths; Hadoop itself is not a
runtime dependency.

The API targets JDK 17 and uses Apache Arrow Java 19.0.0. The current artifact
coordinates are:

```text
dev.spiraldb.parquet.arrow:parquet-arrow-core:0.1.0
```

Maven consumers use:

```xml
<dependency>
  <groupId>dev.spiraldb.parquet.arrow</groupId>
  <artifactId>parquet-arrow-core</artifactId>
  <version>0.1.0</version>
</dependency>
```

## Quick start

The following is the complete write/read shape. `Path` overloads own the file
handle; the reader's `VectorSchemaRoot` is borrowed and its contents are
invalidated and refilled on the next `loadNextBatch()` call.

```java
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import dev.spiraldb.parquet.arrow.ParquetArrow;
import dev.spiraldb.parquet.arrow.ParquetArrowReader;
import dev.spiraldb.parquet.arrow.ParquetArrowWriter;

Schema schema = new Schema(java.util.List.of(
    new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), java.util.List.of()),
    new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), java.util.List.of())));
Path file = Path.of("example.parquet");

try (RootAllocator allocator = new RootAllocator();
     VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
  root.allocateNew();
  ((IntVector) root.getVector("id")).setSafe(0, 1);
  ((VarCharVector) root.getVector("name"))
      .setSafe(0, "alpha".getBytes(StandardCharsets.UTF_8));
  root.setRowCount(1);

  try (ParquetArrowWriter writer = ParquetArrow.writer(schema).build(file)) {
    writer.writeBatch(root);
    writer.finish();
  }

  try (ParquetArrowReader reader = ParquetArrow.reader(allocator).build(file)) {
    while (reader.loadNextBatch()) {
      VectorSchemaRoot batch = reader.getVectorSchemaRoot();
      System.out.println(((IntVector) batch.getVector("id")).get(0));
      System.out.println(((VarCharVector) batch.getVector("name")).getObject(0));
    }
  }
}
```

Run the same flow from the checked-in example with JDK 17:

```bash
./gradlew :examples:run --args='build/example.parquet'
```

The example configures the required JVM opening for Arrow's Java NIO memory
access. For a manually assembled classpath, use
`--add-opens java.base/java.nio=ALL-UNNAMED` (and the Arrow/Netty runtime
dependencies) when launching Java 17.

## v1 supported types and lifecycle

v1 translates Arrow values to standard Parquet and standard Parquet values back
to Arrow. It supports booleans; signed and unsigned integers; float32/float64;
UTF-8 and binary values (including large-offset variants); fixed-size binary;
decimal128/decimal256; dates, times, timestamps; duration; structs; canonical
three-level lists and maps; and Arrow dictionary inputs. Float16, intervals,
unions, run-end encoding, list views, and non-canonical legacy nested encodings
are rejected with a typed `ParquetArrowException`.

Parquet has no second-precision temporal type, so Arrow `SECOND`-unit times and
timestamps are promoted to Parquet `MILLIS` on write: the values scale and the
instant is preserved, but the round-tripped Arrow unit is `MILLISECOND`.
`MILLI`/`MICRO`/`NANO` units round-trip unchanged.

The writer consumes one `VectorSchemaRoot` at a time and `finish()` writes the
footer. `close()` finishes a healthy writer and always releases the parquet
writer and its output stream. The reader returns one borrowed, reusable root;
the next `loadNextBatch()` invalidates its previous contents. `Path` endpoints
own and close their file channels. A `SeekableByteChannel` built with
`CloseMode.LEAVE_OPEN` remains owned by the caller; `CloseMode.CLOSE` transfers
that ownership to parquet-arrow-java. `InputFile` and `OutputFile` retain their own
endpoint ownership policy while the streams opened for Parquet are always
closed. Reader input-byte accounting is unavailable in the parquet-java lane:
`bytesRead()` returns `-1` and `ReadMetrics` deliberately contains no byte
counter.

`ARROW:schema` footer metadata is advisory. Parquet physical/logical types
define the readable value domain; an absent, malformed, unsupported, or stale
hint never rejects valid values. The reader uses only safe representation
refinements such as large UTF-8/binary offsets and a timestamp timezone name
that agrees with Parquet's adjusted-to-UTC bit.

Arrow dictionaries are decoded at the write boundary. Dictionary columns read
back as ordinary value vectors, not dictionary/index vectors. Duplicate sibling
field names are rejected before Parquet compilation with a typed
`UnsupportedParquetTypeException`, at every nesting level (including MAP
entries).

The artifact intentionally ships minimal classes in `org.apache.hadoop.*` to
satisfy parquet-java without a Hadoop dependency jar. Do **not** co-deploy it
with real `hadoop-common` (including Spark/Hadoop runtimes): those classes share
names and classpath coexistence is unsupported.

## Modules and build

* `parquet-arrow-core` is the public Arrow⇆Parquet bridge.
* `parquet-arrow-conformance` contains the logical oracle, fixtures, and benchmarks.
* `examples` is a clean Java-17 downstream consumer smoke project.

Every module builds on JDK 17.

This README is the v1 public contract. Dependency versions and the
reproducible-build policy are recorded in
[`docs/provenance.md`](docs/provenance.md).

This is the `0.1.0` cut. Publishing is intentionally local-only: there is no
remote Maven repository, and none is promised — consume it from source. With
access to the repository, a consumer publishes the artifacts to its local Maven
cache and depends on them by coordinate:

```bash
./gradlew publishToMavenLocal
# then, in the consumer: implementation("dev.spiraldb.parquet.arrow:parquet-arrow-core:0.1.0")
```

The `parquet-arrow-core` dependency set is pinned in
[`parquet-arrow-core/gradle.lockfile`](parquet-arrow-core/gradle.lockfile).
