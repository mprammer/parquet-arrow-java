// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow.conformance;

import static org.junit.jupiter.api.Assertions.*;

import dev.spiraldb.parquet.arrow.ParquetArrow;
import dev.spiraldb.parquet.arrow.ParquetArrowReader;
import dev.spiraldb.parquet.arrow.ParquetArrowWriter;
import dev.spiraldb.parquet.arrow.UnsupportedNestedEncodingException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.function.Consumer;
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
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * parquet-format LogicalTypes.md, "Lists" and "Maps": the list and map level names "should not be
 * enforced as errors when reading", 2-level and legacy lists follow the backward-compatibility
 * rules, and the writer emits the spec's names while an Arrow schema still round-trips exactly.
 * External goldens are written through parquet-java's Group API, not our writer.
 */
class SpecNestedNamingTest {
  @TempDir Path temporary;
  private static final ArrowType I32 = new ArrowType.Int(32, true);

  /** One row [1, null, 3] under a 3-level LIST whose element is named {@code name}. */
  private void threeLevel(String wrapper, String name) throws Exception {
    Path file = golden("message m { optional group xs (LIST) { repeated group " + wrapper + " { optional int32 " + name + "; } } }", g -> {
      Group xs = g.addGroup("xs");
      xs.addGroup(wrapper).append(name, 1); xs.addGroup(wrapper); xs.addGroup(wrapper).append(name, 3);
    });
    assertRead(file, new Schema(List.of(list("xs", true, field(name, I32, true)))), "xs\n[1,null,3]\n");
  }
  @Test void threeLevelListReadsUnderAnyElementName() throws Exception {
    threeLevel("list", "element");
    threeLevel("list", "item");
    threeLevel("list", "l"); // a non-standard element name, as arrow-rs writes it for a list field named "l"
    threeLevel("bag", "array_element"); // rule 5 does not constrain the repeated group's name either
  }

  @Test void backwardCompatibilityRulesOneToFive() throws Exception {
    // Rule 1: a repeated primitive is the element, and elements are required.
    assertRead(golden("message m { optional group my_list (LIST) { repeated int32 element; } }",
        g -> g.addGroup("my_list").append("element", 4).append("element", 5)),
        new Schema(List.of(list("my_list", true, field("element", I32, false)))), "my_list\n[4,5]\n");
    // Rule 2: a repeated group with several fields is the (required) element.
    assertRead(golden("message m { optional group my_list (LIST) { repeated group element { required binary str (STRING); required int32 num; } } }",
        g -> g.addGroup("my_list").addGroup("element").append("str", "a").append("num", 1)),
        new Schema(List.of(list("my_list", true, new Field("element", new FieldType(false, ArrowType.Struct.INSTANCE, null),
            List.of(field("str", ArrowType.Utf8.INSTANCE, false), field("num", I32, false)))))), "my_list\n[{\"str\":\"a\",\"num\":1}]\n");
    // Rule 3: a repeated group whose one field is repeated is the element: a nested 2-level list.
    assertRead(golden("message m { optional group my_list (LIST) { repeated group array (LIST) { repeated int32 array; } } }", g -> {
      Group outer = g.addGroup("my_list");
      outer.addGroup("array").append("array", 1).append("array", 2); outer.addGroup("array").append("array", 3);
    }), new Schema(List.of(list("my_list", true, list("array", false, field("array", I32, false))))), "my_list\n[[1,2],[3]]\n");
    // Rule 4: a one-field repeated group named `array` or `<list>_tuple` is the element (a one-tuple).
    for (String tuple : List.of("array", "my_list_tuple")) {
      assertRead(golden("message m { optional group my_list (LIST) { repeated group " + tuple + " { required binary str (STRING); } } }",
          g -> g.addGroup("my_list").addGroup(tuple).append("str", "s")),
          new Schema(List.of(list("my_list", true, new Field(tuple, new FieldType(false, ArrowType.Struct.INSTANCE, null),
              List.of(field("str", ArrowType.Utf8.INSTANCE, false)))))), "my_list\n[{\"str\":\"s\"}]\n");
    }
    // Rule 5: otherwise the repeated group's one field is the element, with its own repetition.
    assertRead(golden("message m { optional group my_list (LIST) { repeated group element { optional binary str (STRING); } } }", g -> {
      Group xs = g.addGroup("my_list"); xs.addGroup("element").append("str", "x"); xs.addGroup("element");
    }), new Schema(List.of(list("my_list", true, field("str", ArrowType.Utf8.INSTANCE, true)))), "my_list\n[\"x\",null]\n");
  }

  @Test void mapNamesArePositionalAndMapKeyValueReadsAsMap() throws Exception {
    // "key and value can be identified by their position in case of misnaming", and "a group
    // annotated with MAP_KEY_VALUE that is not contained by a MAP-annotated group should be
    // handled as a MAP-annotated group" (Maps, Backward-compatibility rules).
    for (String annotation : List.of("MAP", "MAP_KEY_VALUE")) {
      Path file = golden("message m { optional group my_map (" + annotation + ") { repeated group map { required binary str (STRING); optional int32 num; } } }", g -> {
        Group map = g.addGroup("my_map"); map.addGroup("map").append("str", "a"); map.addGroup("map").append("str", "b").append("num", 2);
      });
      assertRead(file, new Schema(List.of(map("my_map", "entries", field("str", ArrowType.Utf8.INSTANCE, false), field("num", I32, true)))),
          "my_map\n[{\"str\":\"a\"},{\"str\":\"b\",\"num\":2}]\n");
    }
  }

  @Test void nestedListsAndMapsUnderForeignNames() throws Exception {
    Path file = golden("""
        message m {
          optional group ll (LIST) { repeated group list { optional group l (LIST) { repeated group list { required int32 l; } } } }
          optional group lm (LIST) { repeated group list { optional group item (MAP) { repeated group key_value { required binary k (STRING); optional group v (LIST) { repeated int32 v; } } } } }
        }
        """, g -> {
      Group ll = g.addGroup("ll");
      ll.addGroup("list").addGroup("l").addGroup("list").append("l", 1); ll.addGroup("list");
      Group entry = g.addGroup("lm").addGroup("list").addGroup("item").addGroup("key_value").append("k", "a");
      entry.addGroup("v").append("v", 7).append("v", 8);
    });
    assertRead(file, new Schema(List.of(
        list("ll", true, list("l", true, field("l", I32, false))),
        list("lm", true, map("item", "entries", field("k", ArrowType.Utf8.INSTANCE, false), list("v", true, field("v", I32, false)))))),
        "ll\tlm\n[[1],null]\t[[{\"k\":\"a\",\"v\":[7,8]}]]\n");
  }

  @Test void unannotatedRepeatedFieldIsRejectedNotTruncated() throws Exception {
    Path file = golden("message m { repeated int32 num; }", g -> g.append("num", 1).append("num", 2));
    try (RootAllocator allocator = new RootAllocator()) {
      UnsupportedNestedEncodingException error = assertThrows(UnsupportedNestedEncodingException.class,
          () -> ParquetArrow.reader(allocator).build(file).close());
      assertEquals("UNANNOTATED_REPEATED at num", error.getMessage());
    }
  }

  /**
   * The writer names the levels "list"/"element" and "key_value"/"key"/"value" as the spec
   * requires; the Arrow names come back from ARROW:schema, so the Arrow schema is exact.
   */
  @Test void arrowNamesRoundTripThroughSpecNamedLevels() throws Exception {
    Field inner = list("l", true, field("l", I32, true));
    Field keys = field("keys", ArrowType.Utf8.INSTANCE, false), values = list("values", true, field("item", I32, true));
    Schema schema = new Schema(List.of(list("xs", true, inner), map("m", "key_value", keys, values), list("e", true, field("element", I32, true))));
    Path file = temporary.resolve("roundtrip.parquet");
    try (RootAllocator allocator = new RootAllocator(); VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      root.allocateNew();
      ListVector xs = (ListVector) root.getVector("xs"); ListVector xsInner = (ListVector) xs.getDataVector();
      IntVector xsValues = (IntVector) xsInner.getDataVector();
      int outer = xs.startNewValue(0), at = xsInner.startNewValue(outer);
      xsValues.setSafe(at, 1); xsValues.setNull(at + 1); xsInner.endValue(outer, 2); xsInner.setNull(outer + 1); xs.endValue(0, 2);
      MapVector m = (MapVector) root.getVector("m"); StructVector entries = (StructVector) m.getDataVector();
      ListVector mValues = (ListVector) entries.getChild("values"); IntVector mInts = (IntVector) mValues.getDataVector();
      int entry = m.startNewValue(0); entries.setIndexDefined(entry);
      ((VarCharVector) entries.getChild("keys")).setSafe(entry, "k".getBytes(java.nio.charset.StandardCharsets.UTF_8));
      int value = mValues.startNewValue(entry); mInts.setSafe(value, 9); mValues.endValue(entry, 1); m.endValue(0, 1);
      ListVector e = (ListVector) root.getVector("e"); int ev = e.startNewValue(0); ((IntVector) e.getDataVector()).setSafe(ev, 5); e.endValue(0, 1);
      root.setRowCount(1);
      try (ParquetArrowWriter writer = ParquetArrow.writer(schema).build(file)) { writer.writeBatch(root); writer.finish(); }
      String expected = root.contentToTSVString();

      MessageType physical;
      try (ParquetFileReader footer = ParquetFileReader.open(new LocalInputFile(file))) { physical = footer.getFileMetaData().getSchema(); }
      assertEquals(MessageTypeParser.parseMessageType("""
          message arrow {
            optional group xs (LIST) { repeated group list { optional group element (LIST) { repeated group list { optional int32 element (INTEGER(32,true)); } } } }
            optional group m (MAP) { repeated group key_value { required binary key (STRING); optional group value (LIST) { repeated group list { optional int32 element (INTEGER(32,true)); } } } }
            optional group e (LIST) { repeated group list { optional int32 element (INTEGER(32,true)); } }
          }
          """), physical);
      assertRead(file, schema, expected);
    }
  }

  private void assertRead(Path file, Schema schema, String tsv) throws Exception {
    try (RootAllocator allocator = new RootAllocator(); ParquetArrowReader reader = ParquetArrow.reader(allocator).build(file)) {
      assertTrue(reader.loadNextBatch());
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      assertEquals(schema, new Schema(root.getSchema().getFields()));
      assertEquals(tsv, root.contentToTSVString());
      assertFalse(reader.loadNextBatch());
    }
  }

  private Path golden(String schema, Consumer<Group> row) throws Exception {
    MessageType type = MessageTypeParser.parseMessageType(schema);
    Path file = Files.createTempFile(temporary, "golden", ".parquet");
    Files.delete(file); // the Parquet writer creates the file
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file)).withType(type).build()) {
      Group group = new SimpleGroupFactory(type).newGroup(); row.accept(group); writer.write(group);
    }
    return file;
  }
  private static Field field(String name, ArrowType type, boolean nullable) { return new Field(name, new FieldType(nullable, type, null), List.of()); }
  private static Field list(String name, boolean nullable, Field element) { return new Field(name, new FieldType(nullable, ArrowType.List.INSTANCE, null), List.of(element)); }
  private static Field map(String name, String entries, Field key, Field value) {
    return new Field(name, FieldType.nullable(new ArrowType.Map(false)), List.of(new Field(entries, new FieldType(false, ArrowType.Struct.INSTANCE, null), List.of(key, value))));
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
    @Override public void write(int value) throws IOException { write(new byte[] { (byte) value }); }
    @Override public void write(byte[] value, int offset, int length) throws IOException { ByteBuffer bytes = ByteBuffer.wrap(value, offset, length); while (bytes.hasRemaining()) channel.write(bytes); }
    @Override public void close() throws IOException { channel.close(); }
  }
  private record LocalInputFile(Path path) implements InputFile {
    @Override public long getLength() throws IOException { return Files.size(path); }
    @Override public SeekableInputStream newStream() throws IOException { return new Input(FileChannel.open(path, StandardOpenOption.READ)); }
  }
  private static final class Input extends SeekableInputStream {
    private final FileChannel channel;
    Input(FileChannel channel) { this.channel = channel; }
    @Override public long getPos() throws IOException { return channel.position(); }
    @Override public void seek(long position) throws IOException { channel.position(position); }
    @Override public int read() throws IOException { byte[] one = new byte[1]; return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff; }
    @Override public int read(byte[] bytes, int offset, int length) throws IOException { return channel.read(ByteBuffer.wrap(bytes, offset, length)); }
    @Override public int read(ByteBuffer buffer) throws IOException { return channel.read(buffer); }
    @Override public void readFully(byte[] bytes) throws IOException { readFully(bytes, 0, bytes.length); }
    @Override public void readFully(byte[] bytes, int offset, int length) throws IOException { readFully(ByteBuffer.wrap(bytes, offset, length)); }
    @Override public void readFully(ByteBuffer buffer) throws IOException { while (buffer.hasRemaining()) if (channel.read(buffer) < 0) throw new java.io.EOFException(); }
    @Override public void close() throws IOException { channel.close(); }
  }
}
