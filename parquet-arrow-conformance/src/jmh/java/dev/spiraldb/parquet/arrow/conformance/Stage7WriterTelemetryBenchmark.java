// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow.conformance;

import dev.spiraldb.parquet.arrow.Compression;
import dev.spiraldb.parquet.arrow.ParquetArrow;
import dev.spiraldb.parquet.arrow.ParquetArrowReader;
import dev.spiraldb.parquet.arrow.ParquetArrowWriter;
import dev.spiraldb.parquet.arrow.WriteOptions;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Stage-7 telemetry only. Run with {@code -prof gc} to obtain JMH's allocation bytes/op, then
 * divide by the emitted {@link Telemetry#rows} and {@link Telemetry#inputBytes} counters.  The
 * Group writer is deliberately retained as the inherited baseline, not as product code.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class Stage7WriterTelemetryBenchmark {
  private static final byte[] LOW_CARDINALITY = "dictionary-value".getBytes(StandardCharsets.UTF_8);
  private static final MessageType FLAT_GROUP = Types.buildMessage()
      .optional(PrimitiveType.PrimitiveTypeName.INT32).named("id")
      .optional(PrimitiveType.PrimitiveTypeName.BINARY).as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType()).named("text")
      .named("arrow");

  @Param({"flat-fixed", "string-dictionary", "nullable", "nested-list-map"})
  public String workload;
  @Param({"1024"}) public int rows;

  private RootAllocator allocator;
  private Schema schema;
  private MessageType groupSchema;
  private VectorSchemaRoot root;
  private Path direct;
  private Path group;
  private long logicalInputBytes;

  @Setup(Level.Trial) public void setup() throws Exception {
    allocator = new RootAllocator();
    if ("nested-list-map".equals(workload)) {
      Field element = new Field("element", FieldType.nullable(new ArrowType.Int(32, true)), List.of());
      Field key = new Field("key", new FieldType(false, ArrowType.Utf8.INSTANCE, null, null), List.of());
      Field value = new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), List.of());
      Field entry = new Field("entries", new FieldType(false, ArrowType.Struct.INSTANCE, null, null), List.of(key, value));
      schema = new Schema(List.of(new Field("xs", FieldType.nullable(ArrowType.List.INSTANCE), List.of(element)),
          new Field("attrs", FieldType.nullable(new ArrowType.Map(false)), List.of(entry))));
      groupSchema = org.apache.parquet.schema.MessageTypeParser.parseMessageType("""
          message arrow {
            optional group xs (LIST) { repeated group list { optional int32 element; } }
            optional group attrs (MAP) { repeated group key_value { required binary key (STRING); optional int32 value; } }
          }
          """);
    } else {
      schema = new Schema(List.of(
          new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), List.of()),
          new Field("text", FieldType.nullable(ArrowType.Utf8.INSTANCE), List.of())));
      groupSchema = FLAT_GROUP;
    }
    root = VectorSchemaRoot.create(schema, allocator); root.allocateNew();
    if ("nested-list-map".equals(workload)) { populateNested(); assertCorrectnessBeforeTiming(); return; }
    IntVector ids = (IntVector) root.getVector("id");
    VarCharVector text = (VarCharVector) root.getVector("text");
    for (int i = 0; i < rows; i++) {
      if ("nullable".equals(workload) && i % 7 == 0) { ids.setNull(i); text.setNull(i); continue; }
      ids.setSafe(i, i);
      byte[] value = "string-dictionary".equals(workload) ? LOW_CARDINALITY
          : ("flat-fixed".equals(workload) ? "fixed-width-telemetry".getBytes(StandardCharsets.UTF_8) : LOW_CARDINALITY);
      text.setSafe(i, value);
      logicalInputBytes += Integer.BYTES + value.length;
    }
    root.setRowCount(rows);
    assertCorrectnessBeforeTiming();
  }

  @Setup(Level.Invocation) public void files() throws Exception {
    direct = Files.createTempFile("stage7-direct", ".parquet"); Files.delete(direct);
    group = Files.createTempFile("stage7-group", ".parquet"); Files.delete(group);
  }
  @TearDown(Level.Invocation) public void cleanupFiles() throws Exception { Files.deleteIfExists(direct); Files.deleteIfExists(group); }
  @TearDown(Level.Trial) public void cleanup() { root.close(); allocator.close(); }

  @Benchmark public long vectorPlan(Telemetry telemetry) throws Exception {
    try (ParquetArrowWriter writer = ParquetArrow.writer(schema).options(options()).build(direct)) {
      writer.writeBatch(root); long written = writer.finish().rowCount();
      telemetry.record(rows, logicalInputBytes, Files.size(direct)); return written;
    }
  }

  @Benchmark public long groupBaseline(Telemetry telemetry) throws Exception {
    if ("nested-list-map".equals(workload)) return nestedGroupBaseline(telemetry);
    IntVector ids = (IntVector) root.getVector("id"); VarCharVector text = (VarCharVector) root.getVector("text");
    SimpleGroupFactory factory = new SimpleGroupFactory(groupSchema);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new BenchmarkOutputFile(group)).withType(groupSchema)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
      for (int i = 0; i < rows; i++) {
        Group value = factory.newGroup();
        if (!ids.isNull(i)) value.append("id", ids.get(i));
        if (!text.isNull(i)) value.append("text", new String(text.get(i), StandardCharsets.UTF_8));
        writer.write(value);
      }
    }
    telemetry.record(rows, logicalInputBytes, Files.size(group)); return rows;
  }

  private long nestedGroupBaseline(Telemetry telemetry) throws Exception {
    SimpleGroupFactory factory = new SimpleGroupFactory(groupSchema);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new BenchmarkOutputFile(group)).withType(groupSchema)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
      for (int i = 0; i < rows; i++) {
        Group row = factory.newGroup();
        switch (i % 3) {
          case 1 -> { row.addGroup("xs"); row.addGroup("attrs"); }
          case 2 -> {
            Group xs = row.addGroup("xs"); xs.addGroup("list"); xs.addGroup("list").append("element", i);
            Group attrs = row.addGroup("attrs"); attrs.addGroup("key_value").append("key", "a");
            attrs.addGroup("key_value").append("key", "b").append("value", i);
          }
          default -> { /* null list and map */ }
        }
        writer.write(row);
      }
    }
    telemetry.record(rows, logicalInputBytes, Files.size(group)); return rows;
  }

  private void populateNested() {
    ListVector lists = (ListVector) root.getVector("xs");
    IntVector elements = (IntVector) lists.getDataVector();
    MapVector maps = (MapVector) root.getVector("attrs");
    StructVector entries = (StructVector) maps.getDataVector();
    VarCharVector keys = (VarCharVector) entries.getChild("key");
    IntVector values = (IntVector) entries.getChild("value");
    for (int row = 0; row < rows; row++) {
      switch (row % 3) {
        case 0 -> { lists.setNull(row); maps.setNull(row); }
        case 1 -> { lists.startNewValue(row); lists.endValue(row, 0); maps.startNewValue(row); maps.endValue(row, 0); }
        default -> {
          int listStart = lists.startNewValue(row); elements.setNull(listStart); elements.setSafe(listStart + 1, row); lists.endValue(row, 2);
          int mapStart = maps.startNewValue(row);
          keys.setSafe(mapStart, "a".getBytes(StandardCharsets.UTF_8)); values.setNull(mapStart);
          keys.setSafe(mapStart + 1, "b".getBytes(StandardCharsets.UTF_8)); values.setSafe(mapStart + 1, row);
          maps.endValue(row, 2);
          logicalInputBytes += Integer.BYTES + 2;
        }
      }
    }
    root.setRowCount(rows);
  }

  private WriteOptions options() {
    return WriteOptions.builder().compression(Compression.UNCOMPRESSED)
        .parquetDictionaryEnabled("string-dictionary".equals(workload)).build();
  }

  private void assertCorrectnessBeforeTiming() throws Exception {
    Path file = Files.createTempFile("stage7-correctness", ".parquet"); Files.delete(file);
    try (ParquetArrowWriter writer = ParquetArrow.writer(schema).options(options()).build(file)) {
      writer.writeBatch(root); writer.finish();
    }
    try (ParquetArrowReader reader = ParquetArrow.reader(allocator).build(file)) {
      if (!reader.loadNextBatch()) throw new IllegalStateException("correctness check returned no batch");
      new ArrowLogicalComparator().compare(root, reader.getVectorSchemaRoot()).requireMatch();
    } finally { Files.deleteIfExists(file); }
  }

  /** JMH prints these as secondary event counters; allocations are supplied by {@code -prof gc}. */
  @AuxCounters(AuxCounters.Type.EVENTS)
  @State(Scope.Thread)
  public static class Telemetry {
    public long rows;
    public long inputBytes;
    public long outputBytes;
    public void record(long rowCount, long input, long output) { rows = rowCount; inputBytes = input; outputBytes = output; }
  }
}
