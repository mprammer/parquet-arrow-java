// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0
package dev.spiraldb.parquet.arrow;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/**
 * A null struct row never starts the struct's converter, so the reader used to leave its
 * children without a slot for that row; the batch's variable-width byte accounting then read a
 * binary child's offset past its buffer ("invalid Parquet page encoding" caused by an
 * IndexOutOfBoundsException) on a valid file whose last rows were null structs.
 */
class NullStructChildTest {
  @Test void aTrailingNullStructOverABinaryChildReadsBack() throws Exception {
    Path file = Files.createTempFile("pa-nullstruct", ".parquet");
    Files.delete(file);
    Field child = new Field("b", FieldType.nullable(ArrowType.Binary.INSTANCE), List.of());
    Schema schema = new Schema(List.of(new Field("s", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(child))));
    try (RootAllocator allocator = new RootAllocator()) {
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        StructVector s = (StructVector) root.getVector("s");
        s.setIndexDefined(0);
        ((VarBinaryVector) s.getChild("b")).setSafe(0, new byte[] {7});
        s.setNull(1);
        root.setRowCount(2);
        try (ParquetArrowWriter writer = ParquetArrow.writer(schema).build(file)) {
          writer.writeBatch(root);
          writer.finish();
        }
      }
      try (ParquetArrowReader reader = ParquetArrow.reader(allocator).build(file)) {
        assertTrue(reader.loadNextBatch());
        VectorSchemaRoot out = reader.getVectorSchemaRoot();
        assertEquals(2, out.getRowCount());
        StructVector s = (StructVector) out.getVector("s");
        assertArrayEquals(new byte[] {7}, ((VarBinaryVector) s.getChild("b")).get(0));
        assertTrue(s.isNull(1));
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }
}
