// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;

/** Schema/compiler audit for every scalar and container family admitted by §5. */
class Stage7TypeMatrixAuditTest {
  @Test void everySupportedFamilyCompilesToPhysicalSchemaAndBackToAnArrowFamily() throws Exception {
    Field element = field("element", new ArrowType.Int(32, true));
    Field key = new Field("key", new FieldType(false, ArrowType.Utf8.INSTANCE, null, null), List.of());
    Field value = field("value", ArrowType.Binary.INSTANCE);
    Field entry = new Field("entries", new FieldType(false, ArrowType.Struct.INSTANCE, null, null), List.of(key, value));
    Schema schema = new Schema(List.of(
        field("null", ArrowType.Null.INSTANCE), field("bool", ArrowType.Bool.INSTANCE),
        field("i8", new ArrowType.Int(8, true)), field("u8", new ArrowType.Int(8, false)),
        field("i16", new ArrowType.Int(16, true)), field("u16", new ArrowType.Int(16, false)),
        field("i32", new ArrowType.Int(32, true)), field("u32", new ArrowType.Int(32, false)),
        field("i64", new ArrowType.Int(64, true)), field("u64", new ArrowType.Int(64, false)),
        field("f32", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
        field("f64", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        field("utf8", ArrowType.Utf8.INSTANCE), field("largeUtf8", ArrowType.LargeUtf8.INSTANCE), field("utf8View", ArrowType.Utf8View.INSTANCE),
        field("binary", ArrowType.Binary.INSTANCE), field("largeBinary", ArrowType.LargeBinary.INSTANCE), field("binaryView", ArrowType.BinaryView.INSTANCE),
        field("fixed", new ArrowType.FixedSizeBinary(4)),
        field("decimal128", new ArrowType.Decimal(38, 2, 128)), field("decimal256", new ArrowType.Decimal(76, 2, 256)),
        field("date32", new ArrowType.Date(DateUnit.DAY)), field("date64", new ArrowType.Date(DateUnit.MILLISECOND)),
        field("time32s", new ArrowType.Time(TimeUnit.SECOND, 32)), field("time32ms", new ArrowType.Time(TimeUnit.MILLISECOND, 32)),
        field("time64us", new ArrowType.Time(TimeUnit.MICROSECOND, 64)), field("time64ns", new ArrowType.Time(TimeUnit.NANOSECOND, 64)),
        field("timestampS", new ArrowType.Timestamp(TimeUnit.SECOND, null)), field("timestampNs", new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC")),
        field("duration", new ArrowType.Duration(TimeUnit.NANOSECOND)),
        new Field("struct", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(field("member", ArrowType.Utf8.INSTANCE))),
        new Field("list", FieldType.nullable(ArrowType.List.INSTANCE), List.of(element)),
        new Field("largeList", FieldType.nullable(ArrowType.LargeList.INSTANCE), List.of(element)),
        new Field("fixedList", FieldType.nullable(new ArrowType.FixedSizeList(2)), List.of(element)),
        new Field("map", FieldType.nullable(new ArrowType.Map(false)), List.of(entry))));

    MessageType physical = ArrowSchemaToParquet.toParquet(schema);
    assertEquals(schema.getFields().size(), physical.getFieldCount());
    assertEquals(schema.getFields().size(), ParquetToArrow.fromParquet(physical).getFields().size());
    assertDoesNotThrow(() -> VectorWritePlan.compile(schema));
  }

  @Test void documentedRejectionsFailAtSchemaConstruction() {
    assertUnsupported(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF));
    assertUnsupported(new ArrowType.Interval(IntervalUnit.YEAR_MONTH));
    assertUnsupported(ArrowType.ListView.INSTANCE);
    assertUnsupported(ArrowType.LargeListView.INSTANCE);
    assertUnsupported(new ArrowType.Union(UnionMode.Sparse, new int[0]));
    assertUnsupported(ArrowType.RunEndEncoded.INSTANCE);
    assertThrows(UnsupportedParquetTypeException.class, () -> ArrowSchemaToParquet.toParquet(new Schema(List.of(
        new Field("empty", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of())))));
  }

  private static void assertUnsupported(ArrowType type) {
    assertThrows(UnsupportedParquetTypeException.class,
        () -> ArrowSchemaToParquet.toParquet(new Schema(List.of(field("rejected", type)))));
  }
  private static Field field(String name, ArrowType type) { return new Field(name, FieldType.nullable(type), List.of()); }
}
