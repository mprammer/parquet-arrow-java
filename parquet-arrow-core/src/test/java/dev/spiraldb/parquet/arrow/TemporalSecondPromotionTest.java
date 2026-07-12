// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/**
 * Parquet has no second-precision temporal type, so the writer promotes Arrow SECOND times and
 * timestamps to Parquet MILLIS by scaling the value x1000. This proves the scaling is real (not a
 * relabel), that the promoted unit reads back as MILLISECOND, that nulls survive, and that an
 * unrepresentable overflow is rejected rather than silently truncated.
 */
class TemporalSecondPromotionTest {
  @Test void timestampSecondsScaleToMillisOnRoundTrip() throws Exception {
    Path file = Files.createTempFile("pa-ts-sec", ".parquet");
    Files.delete(file);
    long[] seconds = {0L, 1_700_000_000L, -62_135_596_800L}; // epoch, a recent instant, year 1 UTC
    try (RootAllocator allocator = new RootAllocator()) {
      Field field = new Field("t", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, null)), List.of());
      Schema schema = new Schema(List.of(field));
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        TimeStampSecVector v = (TimeStampSecVector) root.getVector("t");
        for (int i = 0; i < seconds.length; i++) v.setSafe(i, seconds[i]);
        v.setNull(seconds.length);
        root.setRowCount(seconds.length + 1);
        try (ParquetArrowWriter writer = ParquetArrow.writer(schema).build(file)) { writer.writeBatch(root); writer.finish(); }
      }
      try (RootAllocator readAllocator = new RootAllocator();
           ParquetArrowReader reader = ParquetArrow.reader(readAllocator).build(file)) {
        assertTrue(reader.loadNextBatch());
        VectorSchemaRoot out = reader.getVectorSchemaRoot();
        ArrowType.Timestamp type = (ArrowType.Timestamp) out.getVector("t").getField().getType();
        assertEquals(TimeUnit.MILLISECOND, type.getUnit(), "SECOND must be promoted to MILLISECOND");
        TimeStampMilliVector v = (TimeStampMilliVector) out.getVector("t");
        for (int i = 0; i < seconds.length; i++) assertEquals(seconds[i] * 1_000L, v.get(i), "value must scale x1000, not relabel");
        assertTrue(v.isNull(seconds.length), "null must round-trip as null");
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test void time32SecondsScaleToMillisOnRoundTrip() throws Exception {
    Path file = Files.createTempFile("pa-time-sec", ".parquet");
    Files.delete(file);
    int[] seconds = {0, 3_600, 86_399}; // midnight, 01:00, end of day
    try (RootAllocator allocator = new RootAllocator()) {
      Field field = new Field("clock", FieldType.nullable(new ArrowType.Time(TimeUnit.SECOND, 32)), List.of());
      Schema schema = new Schema(List.of(field));
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        TimeSecVector v = (TimeSecVector) root.getVector("clock");
        for (int i = 0; i < seconds.length; i++) v.setSafe(i, seconds[i]);
        v.setNull(seconds.length);
        root.setRowCount(seconds.length + 1);
        try (ParquetArrowWriter writer = ParquetArrow.writer(schema).build(file)) { writer.writeBatch(root); writer.finish(); }
      }
      try (RootAllocator readAllocator = new RootAllocator();
           ParquetArrowReader reader = ParquetArrow.reader(readAllocator).build(file)) {
        assertTrue(reader.loadNextBatch());
        VectorSchemaRoot out = reader.getVectorSchemaRoot();
        ArrowType.Time type = (ArrowType.Time) out.getVector("clock").getField().getType();
        assertEquals(TimeUnit.MILLISECOND, type.getUnit(), "SECOND must be promoted to MILLISECOND");
        TimeMilliVector v = (TimeMilliVector) out.getVector("clock");
        for (int i = 0; i < seconds.length; i++) assertEquals(seconds[i] * 1_000, v.get(i), "value must scale x1000, not relabel");
        assertTrue(v.isNull(seconds.length), "null must round-trip as null");
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test void namedTimezoneSurvivesSecondToMillisPromotion() throws Exception {
    Path file = Files.createTempFile("pa-ts-sec-tz", ".parquet");
    Files.delete(file);
    try (RootAllocator allocator = new RootAllocator()) {
      Field field = new Field("t", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, "America/New_York")), List.of());
      Schema schema = new Schema(List.of(field));
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        org.apache.arrow.vector.TimeStampSecTZVector v = (org.apache.arrow.vector.TimeStampSecTZVector) root.getVector("t");
        v.setSafe(0, 1_700_000_000L);
        root.setRowCount(1);
        try (ParquetArrowWriter writer = ParquetArrow.writer(schema).build(file)) { writer.writeBatch(root); writer.finish(); }
      }
      try (RootAllocator readAllocator = new RootAllocator();
           ParquetArrowReader reader = ParquetArrow.reader(readAllocator).build(file)) {
        assertTrue(reader.loadNextBatch());
        ArrowType.Timestamp type = (ArrowType.Timestamp) reader.getVectorSchemaRoot().getVector("t").getField().getType();
        assertEquals(TimeUnit.MILLISECOND, type.getUnit(), "unit is promoted to MILLISECOND");
        assertEquals("America/New_York", type.getTimezone(), "the named zone must survive the promotion via the advisory hint");
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test void timestampSecondOverflowIsRejected() throws Exception {
    Path file = Files.createTempFile("pa-ts-overflow", ".parquet");
    Files.delete(file);
    try (RootAllocator allocator = new RootAllocator()) {
      Field field = new Field("t", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, null)), List.of());
      Schema schema = new Schema(List.of(field));
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        TimeStampSecVector v = (TimeStampSecVector) root.getVector("t");
        v.setSafe(0, Long.MAX_VALUE / 1_000L + 1L); // x1000 overflows a long
        root.setRowCount(1);
        try (ParquetArrowWriter writer = ParquetArrow.writer(schema).build(file)) {
          assertThrows(InvalidArrowValueException.class, () -> writer.writeBatch(root));
        }
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }
}
