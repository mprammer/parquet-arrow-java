// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow.conformance;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.spiraldb.parquet.arrow.Compression;
import dev.spiraldb.parquet.arrow.DataPageVersion;
import dev.spiraldb.parquet.arrow.ParquetArrow;
import dev.spiraldb.parquet.arrow.ParquetArrowWriter;
import dev.spiraldb.parquet.arrow.WriteOptions;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

/** Exercises the product lifecycle and validates emitted pages without using its reader. */
class Stage4FlatWriterPhysicalTest {
  @TempDir Path temporary;
  private static final Schema SCHEMA = new Schema(List.of(
      new Field("i", FieldType.nullable(new ArrowType.Int(32, true)), List.of()),
      new Field("s", FieldType.nullable(ArrowType.Utf8.INSTANCE), List.of()),
      new Field("b", FieldType.nullable(ArrowType.Bool.INSTANCE), List.of())));

  @Test void allCodecsAndBothPageVersionsProduceIndependentFlatTraces() throws Exception {
    for (DataPageVersion version : DataPageVersion.values()) for (Compression codec : Compression.values()) {
      Path file = temporary.resolve(version + "-" + codec + ".parquet");
      try (RootAllocator allocator = new RootAllocator(); VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
        root.allocateNew();
        ((IntVector) root.getVector("i")).setSafe(0, 7);
        ((VarCharVector) root.getVector("s")).setSafe(0, "one".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        ((BitVector) root.getVector("b")).setSafe(0, 1);
        ((VarCharVector) root.getVector("s")).setSafe(1, "two".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        ((BitVector) root.getVector("b")).setNull(1);
        ((IntVector) root.getVector("i")).setSafe(2, 9);
        ((BitVector) root.getVector("b")).setSafe(2, 0);
        root.setRowCount(3);
        try (ParquetArrowWriter writer = ParquetArrow.writer(SCHEMA).options(WriteOptions.builder()
            .compression(codec).dataPageVersion(version).parquetDictionaryEnabled(false).build()).build(file)) {
          writer.writeBatch(root);
          writer.finish();
        }
      }
      String header = version == DataPageVersion.V1 ? "V1" : "V2";
      PhysicalTraceOracle.assertTrace(new PhysicalTrace("i", 0, 1, List.of(header), List.of(
          new PhysicalEvent(true, 0, 1, true, 7), new PhysicalEvent(true, 0, 0, false, null),
          new PhysicalEvent(true, 0, 1, true, 9))), PhysicalTraceOracle.decode(new LocalInputFile(file)).get("i"));
      assertEquals(3, PhysicalTraceOracle.decode(new LocalInputFile(file)).get("s").events().size());
    }
  }

  private record LocalInputFile(Path path) implements InputFile {
    @Override public long getLength() throws IOException { return Files.size(path); }
    @Override public SeekableInputStream newStream() throws IOException { return new Input(FileChannel.open(path, StandardOpenOption.READ)); }
  }
  private static final class Input extends SeekableInputStream {
    private final FileChannel channel; Input(FileChannel channel) { this.channel = channel; }
    @Override public long getPos() throws IOException { return channel.position(); }
    @Override public void seek(long position) throws IOException { channel.position(position); }
    @Override public int read() throws IOException { ByteBuffer one = ByteBuffer.allocate(1); return read(one) < 0 ? -1 : one.get(0) & 0xff; }
    @Override public int read(byte[] bytes, int offset, int length) throws IOException { return read(ByteBuffer.wrap(bytes, offset, length)); }
    @Override public int read(ByteBuffer bytes) throws IOException { return channel.read(bytes); }
    @Override public void readFully(byte[] bytes) throws IOException { readFully(bytes, 0, bytes.length); }
    @Override public void readFully(byte[] bytes, int offset, int length) throws IOException { readFully(ByteBuffer.wrap(bytes, offset, length)); }
    @Override public void readFully(ByteBuffer bytes) throws IOException { while (bytes.hasRemaining()) if (read(bytes) < 0) throw new EOFException(); }
    @Override public void close() throws IOException { channel.close(); }
  }
}
