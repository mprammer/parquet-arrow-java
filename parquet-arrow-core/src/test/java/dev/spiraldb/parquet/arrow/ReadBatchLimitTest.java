// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

class ReadBatchLimitTest {
  private static final Schema SCHEMA = new Schema(List.of(new Field("s", FieldType.nullable(ArrowType.Utf8.INSTANCE), List.of())));

  @Test void maxVariableBytesSplitsAfterCompletedRowsAndAllowsOneOversizeRow() throws Exception {
    Path file = Files.createTempFile("pa-variable-limit", ".parquet"); Files.delete(file);
    byte[] oversized = new byte[128]; java.util.Arrays.fill(oversized, (byte) 'x');
    try (RootAllocator allocator = new RootAllocator(); VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
      root.allocateNew(); VarCharVector values = (VarCharVector) root.getVector(0);
      values.setSafe(0, "one".getBytes(StandardCharsets.UTF_8)); values.setSafe(1, "two".getBytes(StandardCharsets.UTF_8)); values.setSafe(2, oversized); root.setRowCount(3);
      try (ParquetArrowWriter writer = ParquetArrow.writer(SCHEMA).build(file)) { writer.writeBatch(root); writer.finish(); }
      try (ParquetArrowReader reader = ParquetArrow.reader(allocator).options(ReadOptions.builder().batchRows(99).maxVariableBytes(1).build()).build(file)) {
        assertTrue(reader.loadNextBatch()); assertEquals(1, reader.getVectorSchemaRoot().getRowCount());
        assertTrue(reader.loadNextBatch()); assertEquals(1, reader.getVectorSchemaRoot().getRowCount());
        assertTrue(reader.loadNextBatch()); assertEquals(1, reader.getVectorSchemaRoot().getRowCount());
        assertEquals(128, ((VarCharVector) reader.getVectorSchemaRoot().getVector(0)).get(0).length);
      }
    } finally { Files.deleteIfExists(file); }
  }

  @Test void allocatorExhaustionRetainsArrowOutOfMemoryIdentity() throws Exception {
    Path file = Files.createTempFile("pa-reader-oom", ".parquet"); Files.delete(file);
    byte[] value = new byte[16 * 1024];
    try (RootAllocator writerAllocator = new RootAllocator(); VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, writerAllocator)) {
      root.allocateNew(); ((VarCharVector) root.getVector(0)).setSafe(0, value); root.setRowCount(1);
      try (ParquetArrowWriter writer = ParquetArrow.writer(SCHEMA).build(file)) { writer.writeBatch(root); writer.finish(); }
    }
    try (RootAllocator limited = new RootAllocator(1024); ParquetArrowReader reader = ParquetArrow.reader(limited).build(file)) {
      assertThrows(OutOfMemoryException.class, reader::loadNextBatch);
    } finally { Files.deleteIfExists(file); }
  }

  @Test void nestedVariableOutputAlsoHonorsTheSoftLimit() throws Exception {
    Field element = new Field("element", FieldType.nullable(ArrowType.Utf8.INSTANCE), List.of());
    Schema schema = new Schema(List.of(new Field("xs", FieldType.nullable(ArrowType.List.INSTANCE), List.of(element))));
    Path file = Files.createTempFile("pa-nested-variable-limit", ".parquet"); Files.delete(file);
    try (RootAllocator allocator = new RootAllocator(); VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      root.allocateNew(); ListVector lists = (ListVector) root.getVector(0); VarCharVector values = (VarCharVector) lists.getDataVector();
      int first = lists.startNewValue(0); values.setSafe(first, "first".getBytes(StandardCharsets.UTF_8)); lists.endValue(0, 1);
      int second = lists.startNewValue(1); values.setSafe(second, "second".getBytes(StandardCharsets.UTF_8)); lists.endValue(1, 1); root.setRowCount(2);
      try (ParquetArrowWriter writer = ParquetArrow.writer(schema).build(file)) { writer.writeBatch(root); writer.finish(); }
      try (ParquetArrowReader reader = ParquetArrow.reader(allocator).options(ReadOptions.builder().batchRows(99).maxVariableBytes(1).build()).build(file)) {
        assertTrue(reader.loadNextBatch()); assertEquals(1, reader.getVectorSchemaRoot().getRowCount());
        assertTrue(reader.loadNextBatch()); assertEquals(1, reader.getVectorSchemaRoot().getRowCount());
      }
    } finally { Files.deleteIfExists(file); }
  }
}
