// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/**
 * Arrow dictionary indices are resolved to their values at the write boundary and read back as
 * ordinary Arrow values. Parquet's own page dictionary encoding remains available underneath.
 */
class DictionaryAsValuesTest {
  @Test
  void dictionaryRoundTripsAsPlainValues() throws Exception {
    Path file = Files.createTempFile("pa-dict", ".parquet");
    Files.delete(file);
    try (RootAllocator allocator = new RootAllocator()) {
      // dictionary values + a plain color column referencing them (with a null)
      VarCharVector dictValues = new VarCharVector("dict", allocator);
      dictValues.allocateNew();
      dictValues.setSafe(0, "red".getBytes(StandardCharsets.UTF_8));
      dictValues.setSafe(1, "green".getBytes(StandardCharsets.UTF_8));
      dictValues.setSafe(2, "blue".getBytes(StandardCharsets.UTF_8));
      dictValues.setValueCount(3);
      DictionaryEncoding encoding = new DictionaryEncoding(1L, false, new ArrowType.Int(32, true));
      Dictionary dictionary = new Dictionary(dictValues, encoding);

      // Standard Arrow representation a real caller produces: DictionaryEncoder.encode yields the
      // index vector (its field carries the index type); the value type lives in the provider.
      // B1 requires the library to accept exactly this.
      VarCharVector plain = new VarCharVector("color", allocator);
      plain.allocateNew();
      plain.setSafe(0, "red".getBytes(StandardCharsets.UTF_8));
      plain.setSafe(1, "blue".getBytes(StandardCharsets.UTF_8));
      plain.setSafe(2, "red".getBytes(StandardCharsets.UTF_8));
      plain.setNull(3);
      plain.setValueCount(4);
      try (FieldVector encoded = (FieldVector) DictionaryEncoder.encode(plain, dictionary);
           MapDictionaryProvider provider = new MapDictionaryProvider(dictionary);
           VectorSchemaRoot root = new VectorSchemaRoot(List.of(encoded))) {
        root.setRowCount(4);
        Schema schema = root.getSchema();
        try (ParquetArrowWriter writer = ParquetArrow.writer(schema).build(file)) {
          writer.writeBatch(root, provider);
          writer.finish();
        }
      }
      plain.close();
      dictValues.close();

      // Re-scoped contract: a dictionary column translates to/from its VALUES. The writer resolves
      // indices through the provider and writes the decoded values; the reader returns a plain value
      // vector. (Parquet's own page dictionary still compresses it.)
      try (RootAllocator readAllocator = new RootAllocator();
           ParquetArrowReader reader = ParquetArrow.reader(readAllocator).build(file)) {
        assertTrue(reader.loadNextBatch());
        VectorSchemaRoot out = reader.getVectorSchemaRoot();
        assertEquals(4, out.getRowCount());
        VarCharVector color = (VarCharVector) out.getVector("color");
        assertEquals("red", color.getObject(0).toString());
        assertEquals("blue", color.getObject(1).toString());
        assertEquals("red", color.getObject(2).toString());
        assertTrue(color.isNull(3), "null value must round-trip as null");
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  void zeroBatchStreamUsesDictionaryProviderForTopLevelAndNestedValueTypes() throws Exception {
    Path top = Files.createTempFile("pa-zero-dict-top", ".parquet"); Files.delete(top);
    Path nested = Files.createTempFile("pa-zero-dict-nested", ".parquet"); Files.delete(nested);
    try (RootAllocator allocator = new RootAllocator()) {
      VarCharVector values = new VarCharVector("dictionary", allocator); values.allocateNew(); values.setSafe(0, "red".getBytes(StandardCharsets.UTF_8)); values.setValueCount(1);
      DictionaryEncoding encoding = new DictionaryEncoding(9L, false, new ArrowType.Int(8, true));
      Dictionary dictionary = new Dictionary(values, encoding);
      try (MapDictionaryProvider provider = new MapDictionaryProvider(dictionary)) {
        FieldVectorSchema topSchema = dictionarySchema("color", encoding);
        try (ParquetArrowWriter writer = ParquetArrow.writer(topSchema.schema).build(top)) { writer.writeAll(empty(topSchema.schema, provider)); writer.finish(); }
        assertZeroRowValueType(top, allocator, ArrowType.Utf8.INSTANCE);

        Path arrowReaderTop = Files.createTempFile("pa-zero-dict-reader", ".parquet"); Files.delete(arrowReaderTop);
        try {
          try (ParquetArrowWriter writer = ParquetArrow.writer(topSchema.schema).build(arrowReaderTop);
               ArrowReader source = new EmptyArrowReader(allocator, topSchema.schema, provider)) { writer.writeAll(source); writer.finish(); }
          assertZeroRowValueType(arrowReaderTop, allocator, ArrowType.Utf8.INSTANCE);
        } finally { Files.deleteIfExists(arrowReaderTop); }

        Field child = dictionarySchema("element", encoding).field;
        Field list = new Field("colors", FieldType.nullable(ArrowType.List.INSTANCE), List.of(child));
        Schema nestedSchema = new Schema(List.of(list));
        try (ParquetArrowWriter writer = ParquetArrow.writer(nestedSchema).build(nested)) { writer.writeAll(empty(nestedSchema, provider)); writer.finish(); }
        try (ParquetArrowReader reader = ParquetArrow.reader(allocator).build(nested)) {
          assertTrue(!reader.loadNextBatch());
          assertEquals(ArrowType.Utf8.INSTANCE, reader.getVectorSchemaRoot().getSchema().getFields().get(0).getChildren().get(0).getType());
        }
      } finally { values.close(); }
    } finally { Files.deleteIfExists(top); Files.deleteIfExists(nested); }
  }

  private record FieldVectorSchema(Schema schema, org.apache.arrow.vector.types.pojo.Field field) { }
  private static FieldVectorSchema dictionarySchema(String name, DictionaryEncoding encoding) {
    org.apache.arrow.vector.types.pojo.Field field = new org.apache.arrow.vector.types.pojo.Field(name,
        new org.apache.arrow.vector.types.pojo.FieldType(true, encoding.getIndexType(), encoding, null), List.of());
    return new FieldVectorSchema(new Schema(List.of(field)), field);
  }
  private static ArrowBatchStream empty(Schema schema, MapDictionaryProvider dictionaries) {
    return new ArrowBatchStream() {
      @Override public Schema schema() { return schema; }
      @Override public boolean loadNextBatch() { return false; }
      @Override public VectorSchemaRoot getVectorSchemaRoot() { return null; }
      @Override public MapDictionaryProvider dictionaries() { return dictionaries; }
      @Override public void close() { }
    };
  }
  private static void assertZeroRowValueType(Path file, RootAllocator allocator, ArrowType expected) throws Exception {
    try (ParquetArrowReader reader = ParquetArrow.reader(allocator).build(file)) {
      assertTrue(!reader.loadNextBatch());
      assertEquals(expected, reader.getVectorSchemaRoot().getSchema().getFields().get(0).getType());
    }
  }
  private static final class EmptyArrowReader extends ArrowReader {
    private final Schema schema; private final MapDictionaryProvider dictionaries;
    EmptyArrowReader(RootAllocator allocator, Schema schema, MapDictionaryProvider dictionaries) { super(allocator); this.schema = schema; this.dictionaries = dictionaries; }
    @Override public boolean loadNextBatch() { return false; }
    @Override public long bytesRead() { return -1; }
    @Override public Dictionary lookup(long id) { return dictionaries.lookup(id); }
    @Override protected void closeReadSource() { }
    @Override protected Schema readSchema() { return schema; }
  }
}
