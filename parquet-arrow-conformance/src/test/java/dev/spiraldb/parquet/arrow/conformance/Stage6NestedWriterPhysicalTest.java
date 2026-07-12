// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow.conformance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.spiraldb.parquet.arrow.Compression;
import dev.spiraldb.parquet.arrow.DataPageVersion;
import dev.spiraldb.parquet.arrow.ParquetArrow;
import dev.spiraldb.parquet.arrow.ParquetArrowReader;
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
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Writer-side executable §7.3 contract; page decoding is intentionally the first assertion. */
class Stage6NestedWriterPhysicalTest {
  @TempDir Path temporary;
  private static final Schema SCHEMA = new Schema(List.of(new Field("xs", FieldType.nullable(ArrowType.List.INSTANCE), List.of(
      new Field("element", FieldType.nullable(new ArrowType.Int(32, true)), List.of())))));

  @Test void nullableListNullEmptyAndNullElementTraceAcrossPageFormatsAndCodecs() throws Exception {
    for (DataPageVersion page : DataPageVersion.values()) for (Compression codec : Compression.values()) {
      Path file = temporary.resolve(page + "-" + codec + ".parquet");
      try (RootAllocator allocator = new RootAllocator(); VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
        root.allocateNew();
        ListVector list = (ListVector) root.getVector("xs");
        IntVector values = (IntVector) list.getDataVector();
        list.setNull(0);                         // null outer list
        list.startNewValue(1); list.endValue(1, 0); // present, empty list
        int start = list.startNewValue(2);
        values.setNull(start); values.setSafe(start + 1, 7);
        list.endValue(2, 2);
        root.setRowCount(3);
        try (ParquetArrowWriter writer = ParquetArrow.writer(SCHEMA).options(WriteOptions.builder()
            .compression(codec).dataPageVersion(page).parquetDictionaryEnabled(false).build()).build(file)) {
          writer.writeBatch(root); writer.finish();
        }

        String header = page == DataPageVersion.V1 ? "V1" : "V2";
        PhysicalTraceOracle.assertTrace(new PhysicalTrace("xs.list.element", 1, 3, List.of(header), List.of(
            new PhysicalEvent(true, 0, 0, false, null),
            new PhysicalEvent(true, 0, 1, false, null),
            new PhysicalEvent(true, 0, 2, false, null),
            new PhysicalEvent(false, 1, 3, true, 7))),
            PhysicalTraceOracle.decode(new LocalInputFile(file)).get("xs.list.element"));

        try (ParquetArrowReader reader = ParquetArrow.reader(allocator).build(file)) {
          assertTrue(reader.loadNextBatch());
          new ArrowLogicalComparator().compare(root, reader.getVectorSchemaRoot()).requireMatch();
        }
      }
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
    @Override public int read() throws IOException { ByteBuffer one = ByteBuffer.allocate(1); return read(one) < 0 ? -1 : one.get(0) & 255; }
    @Override public int read(byte[] bytes, int off, int len) throws IOException { return read(ByteBuffer.wrap(bytes, off, len)); }
    @Override public int read(ByteBuffer bytes) throws IOException { return channel.read(bytes); }
    @Override public void readFully(byte[] bytes) throws IOException { readFully(bytes, 0, bytes.length); }
    @Override public void readFully(byte[] bytes, int off, int len) throws IOException { readFully(ByteBuffer.wrap(bytes, off, len)); }
    @Override public void readFully(ByteBuffer bytes) throws IOException { while (bytes.hasRemaining()) if (read(bytes) < 0) throw new EOFException(); }
    @Override public void close() throws IOException { channel.close(); }
  }
}
