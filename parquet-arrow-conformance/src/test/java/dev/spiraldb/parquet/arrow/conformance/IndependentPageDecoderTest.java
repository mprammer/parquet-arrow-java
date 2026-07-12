// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow.conformance;

import static org.junit.jupiter.api.Assertions.*;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Uses parquet-java's Group writer, never the product writer, to prove the decoder reads real pages. */
class IndependentPageDecoderTest {
  @TempDir Path temporary;

  @Test void decodesAnExternalGroupWriterV1Page() throws Exception {
    MessageType schema = Types.buildMessage().optional(PrimitiveType.PrimitiveTypeName.INT32).named("value").named("group_fixture");
    Path file = temporary.resolve("external-group.parquet");
    SimpleGroupFactory groups = new SimpleGroupFactory(schema);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file)).withType(schema)
        .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0).build()) {
      writer.write(groups.newGroup().append("value", 7));
      writer.write(groups.newGroup()); // optional leaf produces the structural absent event
    }
    Map<String, PhysicalTrace> decoded = PhysicalTraceOracle.decode(new LocalInputFile(file));
    PhysicalTrace trace = decoded.get("value");
    assertNotNull(trace);
    assertEquals(List.of("V1"), trace.pageHeaders());
    PhysicalTraceOracle.assertTrace(new PhysicalTrace("value", 0, 1, List.of("V1"), List.of(
        new PhysicalEvent(true, 0, 1, true, 7), new PhysicalEvent(true, 0, 0, false, null))), trace);
  }

  @Test void decodesAnExternalGroupWriterV2Page() throws Exception {
    MessageType schema = Types.buildMessage().optional(PrimitiveType.PrimitiveTypeName.INT32).named("value").named("group_fixture");
    Path file = temporary.resolve("external-group-v2.parquet");
    SimpleGroupFactory groups = new SimpleGroupFactory(schema);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file)).withType(schema)
        .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0).build()) {
      writer.write(groups.newGroup().append("value", 7));
      writer.write(groups.newGroup());
    }
    PhysicalTrace trace = PhysicalTraceOracle.decode(new LocalInputFile(file)).get("value");
    assertNotNull(trace);
    assertEquals(List.of("V2"), trace.pageHeaders());
    PhysicalTraceOracle.assertTrace(new PhysicalTrace("value", 0, 1, List.of("V2"), List.of(
        new PhysicalEvent(true, 0, 1, true, 7), new PhysicalEvent(true, 0, 0, false, null))), trace);
  }

  private record LocalOutputFile(Path path) implements OutputFile {
    @Override public PositionOutputStream create(long hint) throws IOException { return new ChannelOutput(FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)); }
    @Override public PositionOutputStream createOrOverwrite(long hint) throws IOException { return new ChannelOutput(FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)); }
    @Override public boolean supportsBlockSize() { return false; }
    @Override public long defaultBlockSize() { return 0; }
  }
  private static final class ChannelOutput extends PositionOutputStream {
    private final FileChannel channel;
    ChannelOutput(FileChannel channel) { this.channel = channel; }
    @Override public long getPos() throws IOException { return channel.position(); }
    @Override public void write(int value) throws IOException { write(new byte[] {(byte) value}); }
    @Override public void write(byte[] values, int offset, int length) throws IOException { ByteBuffer bytes = ByteBuffer.wrap(values, offset, length); while (bytes.hasRemaining()) channel.write(bytes); }
    @Override public void close() throws IOException { channel.close(); }
  }
  private record LocalInputFile(Path path) implements InputFile {
    @Override public long getLength() throws IOException { return Files.size(path); }
    @Override public SeekableInputStream newStream() throws IOException { return new ChannelInput(FileChannel.open(path, StandardOpenOption.READ)); }
  }
  private static final class ChannelInput extends SeekableInputStream {
    private final FileChannel channel;
    ChannelInput(FileChannel channel) { this.channel = channel; }
    @Override public long getPos() throws IOException { return channel.position(); }
    @Override public void seek(long position) throws IOException { channel.position(position); }
    @Override public int read() throws IOException { ByteBuffer b = ByteBuffer.allocate(1); return read(b) < 0 ? -1 : b.get(0) & 0xff; }
    @Override public int read(byte[] values, int offset, int length) throws IOException { return read(ByteBuffer.wrap(values, offset, length)); }
    @Override public int read(ByteBuffer bytes) throws IOException { return channel.read(bytes); }
    @Override public void readFully(byte[] values) throws IOException { readFully(values, 0, values.length); }
    @Override public void readFully(byte[] values, int offset, int length) throws IOException { readFully(ByteBuffer.wrap(values, offset, length)); }
    @Override public void readFully(ByteBuffer bytes) throws IOException { while (bytes.hasRemaining()) if (read(bytes) < 0) throw new EOFException(); }
    @Override public void close() throws IOException { channel.close(); }
  }
}
