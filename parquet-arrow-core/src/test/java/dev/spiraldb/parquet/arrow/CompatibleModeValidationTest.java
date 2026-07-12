// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/**
 * COMPATIBLE input-schema mode (the default) accepts a batch whose field is nullable where the
 * writer schema declares it required, as long as no <em>reachable</em> value violates. A required
 * child of an ABSENT nullable struct occupies an unreachable, null child slot that carries no
 * logical value; it must not be treated as a required-field violation. A reachable null still is.
 */
class CompatibleModeValidationTest {
  private static Schema schema(boolean childNullable) {
    Field child = new Field("v", new FieldType(childNullable, new ArrowType.Int(32, true), null), List.of());
    Field struct = new Field("s", FieldType.nullable(new ArrowType.Struct()), List.of(child));
    return new Schema(List.of(struct));
  }

  @Test void requiredChildUnderAbsentStructIsNotAViolation() throws Exception {
    Path file = Files.createTempFile("pa-compat-ok", ".parquet");
    Files.delete(file);
    try (RootAllocator allocator = new RootAllocator();
         VectorSchemaRoot root = VectorSchemaRoot.create(schema(true), allocator)) {
      StructVector s = (StructVector) root.getVector("s");
      IntVector v = (IntVector) s.getChild("v");
      s.setNull(0);                                  // struct absent: child slot 0 is an unreachable null
      s.setIndexDefined(1); v.setSafe(1, 42);        // struct present with a value
      v.setValueCount(2); s.setValueCount(2); root.setRowCount(2);
      // writer declares the child REQUIRED; the batch has it nullable -> COMPATIBLE narrowing.
      try (ParquetArrowWriter writer = ParquetArrow.writer(schema(false)).build(file)) {
        writer.writeBatch(root);
        writer.finish();
      }
      try (RootAllocator readAllocator = new RootAllocator();
           ParquetArrowReader reader = ParquetArrow.reader(readAllocator).build(file)) {
        assertTrue(reader.loadNextBatch());
        VectorSchemaRoot out = reader.getVectorSchemaRoot();
        StructVector s2 = (StructVector) out.getVector("s");
        assertTrue(s2.isNull(0), "absent struct round-trips as null");
        assertTrue(!s2.isNull(1), "present struct round-trips as present");
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test void reachableNullInRequiredChildIsStillRejected() throws Exception {
    Path file = Files.createTempFile("pa-compat-bad", ".parquet");
    Files.delete(file);
    try (RootAllocator allocator = new RootAllocator();
         VectorSchemaRoot root = VectorSchemaRoot.create(schema(true), allocator)) {
      StructVector s = (StructVector) root.getVector("s");
      IntVector v = (IntVector) s.getChild("v");
      s.setIndexDefined(0); v.setNull(0);            // struct PRESENT but required child is null -> reachable violation
      v.setValueCount(1); s.setValueCount(1); root.setRowCount(1);
      try (ParquetArrowWriter writer = ParquetArrow.writer(schema(false)).build(file)) {
        assertThrows(InvalidArrowValueException.class, () -> writer.writeBatch(root));
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }
}
