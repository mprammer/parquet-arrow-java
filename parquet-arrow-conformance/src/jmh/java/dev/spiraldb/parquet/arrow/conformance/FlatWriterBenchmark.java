// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow.conformance;

import dev.spiraldb.parquet.arrow.Compression;
import dev.spiraldb.parquet.arrow.ParquetArrow;
import dev.spiraldb.parquet.arrow.ParquetArrowWriter;
import dev.spiraldb.parquet.arrow.WriteOptions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
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
 * Telemetry only: records allocation and throughput of direct vector writes against parquet-java's
 * inherited Group path.  Stage 7 owns statistical thresholds; this harness deliberately has none.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class FlatWriterBenchmark {
  private static final Schema ARROW = new Schema(List.of(
      new Field("id", FieldType.notNullable(new ArrowType.Int(32, true)), List.of()),
      new Field("text", FieldType.notNullable(ArrowType.Utf8.INSTANCE), List.of())));
  private static final MessageType GROUP = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).named("id")
      .required(PrimitiveType.PrimitiveTypeName.BINARY).as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType()).named("text").named("arrow");
  @Param({"1024", "8192"}) public int rows;
  private RootAllocator allocator; private VectorSchemaRoot root; private Path direct; private Path group;

  @Setup(Level.Trial) public void setup() {
    allocator = new RootAllocator(); root = VectorSchemaRoot.create(ARROW, allocator); root.allocateNew();
    IntVector ids = (IntVector) root.getVector("id"); VarCharVector text = (VarCharVector) root.getVector("text");
    for (int i = 0; i < rows; i++) { ids.setSafe(i, i); text.setSafe(i, "compressible-flat-value".getBytes(java.nio.charset.StandardCharsets.UTF_8)); }
    root.setRowCount(rows);
  }
  @Setup(Level.Invocation) public void files() throws Exception { direct = Files.createTempFile("stage4-direct", ".parquet"); group = Files.createTempFile("stage4-group", ".parquet"); Files.delete(direct); Files.delete(group); }
  @TearDown(Level.Invocation) public void cleanupFiles() throws Exception { Files.deleteIfExists(direct); Files.deleteIfExists(group); }
  @TearDown(Level.Trial) public void cleanup() { root.close(); allocator.close(); }

  @Benchmark public long vectorPlan() throws Exception {
    try (ParquetArrowWriter writer = ParquetArrow.writer(ARROW).options(WriteOptions.builder().compression(Compression.UNCOMPRESSED).parquetDictionaryEnabled(false).build()).build(direct)) { writer.writeBatch(root); return writer.finish().rowCount(); }
  }
  @Benchmark public long groupBaseline() throws Exception {
    SimpleGroupFactory factory = new SimpleGroupFactory(GROUP);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new BenchmarkOutputFile(group)).withType(GROUP).withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
      for (int i = 0; i < rows; i++) writer.write(factory.newGroup().append("id", i).append("text", "compressible-flat-value"));
    }
    return rows;
  }
}

/** Minimal Hadoop-free OutputFile for the inherited Group baseline. */
final class BenchmarkOutputFile implements OutputFile {
  private final Path path;
  BenchmarkOutputFile(Path path) { this.path = path; }
  @Override public PositionOutputStream create(long hint) throws java.io.IOException { return new Output(FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)); }
  @Override public PositionOutputStream createOrOverwrite(long hint) throws java.io.IOException { return new Output(FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)); }
  @Override public boolean supportsBlockSize() { return false; }
  @Override public long defaultBlockSize() { return 0; }
  private static final class Output extends PositionOutputStream {
    private final FileChannel channel; Output(FileChannel channel) { this.channel = channel; }
    @Override public long getPos() throws java.io.IOException { return channel.position(); }
    @Override public void write(int value) throws java.io.IOException { write(new byte[] {(byte) value}); }
    @Override public void write(byte[] value, int offset, int length) throws java.io.IOException { ByteBuffer bytes = ByteBuffer.wrap(value, offset, length); while (bytes.hasRemaining()) channel.write(bytes); }
    @Override public void close() throws java.io.IOException { channel.close(); }
  }
}
