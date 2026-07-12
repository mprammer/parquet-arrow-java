// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;

class Stage2SchemaTest {
  @Test void primitiveMatrixAndAnnotationPairs() throws Exception {
    Schema schema = new Schema(List.of(
        f("null", ArrowType.Null.INSTANCE), f("u8", new ArrowType.Int(8, false)), f("i64", new ArrowType.Int(64, true)),
        f("text", ArrowType.Utf8.INSTANCE), f("date", new ArrowType.Date(DateUnit.DAY)),
        f("time", new ArrowType.Time(TimeUnit.MICROSECOND, 64)), f("timestamp", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null))));
    MessageType parquet = ArrowSchemaToParquet.toParquet(schema);
    assertEquals(PrimitiveType.PrimitiveTypeName.INT32, parquet.getType("u8").asPrimitiveType().getPrimitiveTypeName());
    assertTrue(parquet.getType("text").getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation);
    assertNotNull(parquet.getType("text").getOriginalType());
    assertTrue(parquet.getType("date").getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation);
    assertTrue(parquet.getType("time").getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation);
    assertTrue(parquet.getType("timestamp").getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation);
    assertEquals(schema, ParquetToArrow.fromParquet(parquet));
  }

  @Test void decimalBoundariesAndDecimal256Width() throws Exception {
    Schema schema = new Schema(List.of(f("d9", new ArrowType.Decimal(9, 0, 128)), f("d10", new ArrowType.Decimal(10, 0, 128)),
        f("d19", new ArrowType.Decimal(19, 0, 128)), f("d256", new ArrowType.Decimal(76, 3, 256))));
    MessageType parquet = ArrowSchemaToParquet.toParquet(schema);
    assertEquals(PrimitiveType.PrimitiveTypeName.INT32, parquet.getType("d9").asPrimitiveType().getPrimitiveTypeName());
    assertEquals(PrimitiveType.PrimitiveTypeName.INT64, parquet.getType("d10").asPrimitiveType().getPrimitiveTypeName());
    assertEquals(9, parquet.getType("d19").asPrimitiveType().getTypeLength());
    assertEquals(32, parquet.getType("d256").asPrimitiveType().getTypeLength());
    assertThrows(UnsupportedParquetTypeException.class, () -> ArrowSchemaToParquet.toParquet(new Schema(List.of(f("bad", new ArrowType.Decimal(39, 0, 128))))));
    assertThrows(UnsupportedParquetTypeException.class, () -> ArrowSchemaToParquet.toParquet(new Schema(List.of(f("bad", new ArrowType.Decimal(10, -1, 128))))));
  }

  @Test void nestedCanonicalShapesAndRejects() throws Exception {
    Field element = f("element", new ArrowType.Int(32, true));
    Field list = new Field("xs", FieldType.nullable(ArrowType.List.INSTANCE), List.of(element));
    Field key = new Field("key", new FieldType(false, ArrowType.Utf8.INSTANCE, null, null), List.of());
    Field value = f("value", ArrowType.Binary.INSTANCE);
    Field entries = new Field("entries", new FieldType(false, ArrowType.Struct.INSTANCE, null, null), List.of(key, value));
    Field map = new Field("attrs", FieldType.nullable(new ArrowType.Map(false)), List.of(entries));
    MessageType physical = ArrowSchemaToParquet.toParquet(new Schema(List.of(list, map)));
    assertTrue(physical.getType("xs").getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation);
    assertTrue(physical.getType("attrs").getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation);
    assertEquals(new Schema(List.of(list, map)), ParquetToArrow.fromParquet(physical));
    assertThrows(UnsupportedParquetTypeException.class, () -> ArrowSchemaToParquet.toParquet(new Schema(List.of(new Field("empty", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of())))));
    assertThrows(UnsupportedParquetTypeException.class, () -> ArrowSchemaToParquet.toParquet(new Schema(List.of(new Field("bad", FieldType.nullable(ArrowType.ListView.INSTANCE), List.of())))));
  }

  @Test void foreignNestedReasonCodesAreStable() throws Exception {
    Type primitive = new PrimitiveType(Type.Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.INT32, "element");
    Type outer = org.apache.parquet.schema.Types.buildGroup(Type.Repetition.OPTIONAL).as(LogicalTypeAnnotation.listType()).addField(primitive).named("xs");
    UnsupportedNestedEncodingException error = assertThrows(UnsupportedNestedEncodingException.class, () -> new ListEncodingResolver().resolve(outer, "xs"));
    assertTrue(error.getMessage().startsWith("REPEATED_PRIMITIVE at xs"));
  }

  @Test void pageVersionMappingIsPublicAndExact() {
    assertEquals(DataPageVersion.V1, WriteOptions.builder().build().dataPageVersion());
    assertEquals(DataPageVersion.V2, WriteOptions.builder().dataPageVersion(DataPageVersion.V2).build().dataPageVersion());
    assertEquals(org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0, ParquetFormatProfile.writerVersion(DataPageVersion.V1));
    assertEquals(org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0, ParquetFormatProfile.writerVersion(DataPageVersion.V2));
  }

  @Test void duplicateSiblingNamesAreRejectedAtEveryLogicalLevel() {
    Field duplicateA = f("x", new ArrowType.Int(32, true)), duplicateB = f("x", ArrowType.Utf8.INSTANCE);
    assertDuplicate(new Schema(List.of(duplicateA, duplicateB)), "x");

    Field struct = new Field("s", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(duplicateA, duplicateB));
    assertDuplicate(new Schema(List.of(struct)), "s.x");

    Field element = new Field("element", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(duplicateA, duplicateB));
    Field list = new Field("xs", FieldType.nullable(ArrowType.List.INSTANCE), List.of(element));
    assertDuplicate(new Schema(List.of(list)), "xs.element.x");

    Field key = new Field("x", new FieldType(false, ArrowType.Utf8.INSTANCE, null, null), List.of());
    Field value = f("x", ArrowType.Binary.INSTANCE);
    Field entries = new Field("entries", new FieldType(false, ArrowType.Struct.INSTANCE, null, null), List.of(key, value));
    Field map = new Field("m", FieldType.nullable(new ArrowType.Map(false)), List.of(entries));
    assertDuplicate(new Schema(List.of(map)), "m.entries.x");
  }

  private static void assertDuplicate(Schema schema, String path) {
    UnsupportedParquetTypeException failure = assertThrows(UnsupportedParquetTypeException.class,
        () -> ArrowSchemaToParquet.toParquet(schema));
    assertEquals(path + ": duplicate field name cannot round-trip through Parquet", failure.getMessage());
  }

  private static Field f(String name, ArrowType type) { return new Field(name, FieldType.nullable(type), List.of()); }
}
