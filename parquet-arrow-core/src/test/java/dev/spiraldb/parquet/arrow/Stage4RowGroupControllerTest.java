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

/** Exact-row-cap and buffer-lifetime contract for the Stage-4 controller. */
class Stage4RowGroupControllerTest {
  @Test void rowCapSplitsOnlyBetweenMessagesAndReleasesCurrentGroupBuffers() throws Exception {
    Schema schema = new Schema(List.of(new Field("i", FieldType.notNullable(new ArrowType.Int(32, true)), List.of())));
    Path file = Files.createTempFile("stage4-row-cap", ".parquet"); Files.delete(file);
    try (RootAllocator allocator = new RootAllocator(); VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      root.allocateNew(); IntVector values = (IntVector) root.getVector(0); for (int i = 0; i < 5; i++) values.setSafe(i, i); root.setRowCount(5);
      try (ParquetArrowWriter writer = ParquetArrow.writer(schema).options(WriteOptions.builder().compression(Compression.UNCOMPRESSED).maxRowGroupRows(2).build()).build(file)) {
        writer.writeBatch(root);
        assertEquals(2, writer.metrics().rowGroupsFinished());
        assertTrue(writer.metrics().peakBufferedBytes() >= 0);
        writer.finish();
        assertEquals(3, writer.metrics().rowGroupsFinished());
        assertEquals(0, writer.metrics().currentBufferedBytes());
      }
    } finally { Files.deleteIfExists(file); }
  }
}
