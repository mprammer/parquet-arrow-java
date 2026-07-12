// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow.examples;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import dev.spiraldb.parquet.arrow.ParquetArrow;
import dev.spiraldb.parquet.arrow.ParquetArrowReader;
import dev.spiraldb.parquet.arrow.ParquetArrowWriter;

/** Exercises the public core API from a separate downstream project. */
class ConsumerSmokeTest {
  @Test
  void externalConsumerCanRoundTrip() throws Exception {
    Schema schema = new Schema(List.of(
        new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), List.of()),
        new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), List.of())));
    Path file = Files.createTempFile("parquet-arrow-consumer-", ".parquet");
    Files.delete(file); // the writer is create-new by default; start from a non-existent path
    try (RootAllocator allocator = new RootAllocator();
         VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      root.allocateNew();
      ((IntVector) root.getVector("id")).setSafe(0, 41);
      ((VarCharVector) root.getVector("name")).setSafe(
          0, "consumer".getBytes(StandardCharsets.UTF_8));
      root.setRowCount(1);

      try (ParquetArrowWriter writer = ParquetArrow.writer(schema).build(file)) {
        writer.writeBatch(root);
        writer.finish();
      }

      try (ParquetArrowReader reader = ParquetArrow.reader(allocator).build(file)) {
        assertTrue(reader.loadNextBatch());
        VectorSchemaRoot batch = reader.getVectorSchemaRoot();
        assertEquals(1, batch.getRowCount());
        assertEquals(41, ((IntVector) batch.getVector("id")).get(0));
        assertEquals("consumer", ((VarCharVector) batch.getVector("name")).getObject(0).toString());
        assertTrue(!reader.loadNextBatch());
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }
}
