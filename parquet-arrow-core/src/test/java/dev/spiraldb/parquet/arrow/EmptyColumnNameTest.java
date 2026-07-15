// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0
package dev.spiraldb.parquet.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/**
 * Arrow and Parquet both permit an empty ("") column name — e.g. an unnamed index column. The
 * writer already produced valid Parquet for these; the reader used to reject its own valid output
 * because the default all-columns projection built a {@link FieldPath} from the empty name and
 * {@code FieldPath.of} banned empty segments. Both directions must round-trip.
 */
class EmptyColumnNameTest {
  @Test void emptyNamedColumnRoundTrips() throws Exception {
    Path file = Files.createTempFile("pa-emptyname", ".parquet");
    Files.delete(file);
    try (RootAllocator allocator = new RootAllocator()) {
      Schema schema = new Schema(List.of(
          new Field("", FieldType.nullable(new ArrowType.Int(32, true)), List.of()),
          new Field("b", FieldType.nullable(new ArrowType.Int(32, true)), List.of())));
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        ((IntVector) root.getVector(0)).setSafe(0, 7);
        ((IntVector) root.getVector("b")).setSafe(0, 42);
        root.setRowCount(1);
        try (ParquetArrowWriter writer = ParquetArrow.writer(schema).build(file)) {
          writer.writeBatch(root);
          writer.finish();
        }
      }
      try (RootAllocator readAllocator = new RootAllocator();
           ParquetArrowReader reader = ParquetArrow.reader(readAllocator).build(file)) {
        assertTrue(reader.loadNextBatch());
        VectorSchemaRoot out = reader.getVectorSchemaRoot();
        assertEquals("", out.getVector(0).getField().getName(), "empty column name preserved");
        assertEquals(7, ((IntVector) out.getVector(0)).get(0));
        assertEquals(42, ((IntVector) out.getVector("b")).get(0));
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }
}
