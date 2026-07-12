// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow.conformance;

import static org.junit.jupiter.api.Assertions.*;

import dev.spiraldb.parquet.arrow.ColumnProjection;
import dev.spiraldb.parquet.arrow.FieldPath;
import dev.spiraldb.parquet.arrow.ParquetArrow;
import dev.spiraldb.parquet.arrow.ParquetArrowReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;

/** Goldens here are deliberately written through parquet-java's Group API, not our writer. */
class Stage5NestedReaderTest {
  private static final MessageType NESTED = MessageTypeParser.parseMessageType("""
      message external {
        optional group s { optional int32 x; }
        optional group xs (LIST) { repeated group list { optional int32 item; } }
        optional group attrs (MAP) { repeated group different_entry_name { required binary strange_key (STRING); optional int32 strange_value; } }
      }
      """);

  @Test void readsCanonicalNestedGoldensAndKeepsPresentAllNullStruct() throws Exception {
    Path file = Files.createTempFile("stage5-nested", ".parquet");
    try {
      writeGolden(file);
      try (RootAllocator allocator = new RootAllocator(); ParquetArrowReader reader = ParquetArrow.reader(allocator).build(file)) {
        assertTrue(reader.loadNextBatch());
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        assertEquals(3, root.getRowCount());
        StructVector struct = (StructVector) root.getVector("s");
        assertTrue(struct.isNull(0));
        assertFalse(struct.isNull(1), "a present group with only null descendants remains present");
        assertEquals(9, ((IntVector) struct.getChild("x")).get(2));
        ListVector list = (ListVector) root.getVector("xs");
        assertTrue(list.isNull(0));
        assertEquals(2, list.getDataVector().getValueCount());
        assertEquals(2, ((IntVector) list.getDataVector()).get(1));
        assertFalse(reader.loadNextBatch());
        assertEquals(0, reader.getVectorSchemaRoot().getRowCount());
        assertFalse(reader.loadNextBatch(), "EOF is stable and keeps an empty root");
      }
    } finally { Files.deleteIfExists(file); }
  }

  @Test void physicalProjectionReturnsOnlyRequestedTopLevelField() throws Exception {
    Path file = Files.createTempFile("stage5-projection", ".parquet");
    try {
      writeGolden(file);
      try (RootAllocator allocator = new RootAllocator(); ParquetArrowReader reader = ParquetArrow.reader(allocator)
          .projection(ColumnProjection.paths(FieldPath.of("s"))).build(file)) {
        assertTrue(reader.loadNextBatch());
        assertEquals(1, reader.getVectorSchemaRoot().getFieldVectors().size());
        assertNotNull(reader.getVectorSchemaRoot().getVector("s"));
      }
    } finally { Files.deleteIfExists(file); }
  }

  private static void writeGolden(Path file) throws Exception {
    Files.deleteIfExists(file); // createTempFile pre-creates the path; parquet writer wants create-new
    SimpleGroupFactory groups = new SimpleGroupFactory(NESTED);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file)).withType(NESTED).build()) {
      writer.write(groups.newGroup());
      Group allNullStructAndEmptyList = groups.newGroup();
      allNullStructAndEmptyList.addGroup("s");
      allNullStructAndEmptyList.addGroup("xs");
      allNullStructAndEmptyList.addGroup("attrs");
      writer.write(allNullStructAndEmptyList);
      Group values = groups.newGroup();
      values.addGroup("s").append("x", 9);
      Group xs = values.addGroup("xs");           // one LIST field, two repeated `list` elements
      xs.addGroup("list");                         // element 0: null
      xs.addGroup("list").append("item", 2);       // element 1: 2
      Group attrs = values.addGroup("attrs");      // one MAP field, two key_value entries
      attrs.addGroup("different_entry_name").append("strange_key", "a");                            // a -> null
      attrs.addGroup("different_entry_name").append("strange_key", "b").append("strange_value", 7); // b -> 7
      writer.write(values);
    }
  }

  private record LocalOutputFile(Path path) implements OutputFile {
    @Override public PositionOutputStream create(long hint) throws IOException { return new ChannelOutput(FileChannel.open(path, java.nio.file.StandardOpenOption.CREATE_NEW, java.nio.file.StandardOpenOption.WRITE)); }
    @Override public PositionOutputStream createOrOverwrite(long hint) throws IOException { return new ChannelOutput(FileChannel.open(path, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING, java.nio.file.StandardOpenOption.WRITE)); }
    @Override public boolean supportsBlockSize() { return false; }
    @Override public long defaultBlockSize() { return 0; }
  }
  private static final class ChannelOutput extends PositionOutputStream {
    private final FileChannel channel;
    ChannelOutput(FileChannel channel) { this.channel = channel; }
    @Override public long getPos() throws IOException { return channel.position(); }
    @Override public void write(int value) throws IOException { write(new byte[] { (byte) value }); }
    @Override public void write(byte[] value, int offset, int length) throws IOException { ByteBuffer bytes = ByteBuffer.wrap(value, offset, length); while (bytes.hasRemaining()) channel.write(bytes); }
    @Override public void close() throws IOException { channel.close(); }
  }
}
