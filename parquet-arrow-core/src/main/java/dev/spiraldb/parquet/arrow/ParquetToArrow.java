// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/** Resolves the physical-schema contract; it deliberately performs no value I/O. */
public final class ParquetToArrow {
  private final ListEncodingResolver lists = new ListEncodingResolver();
  private final MapEncodingResolver maps = new MapEncodingResolver();

  public Schema convert(MessageType schema) throws UnsupportedParquetTypeException, UnsupportedNestedEncodingException {
    return toArrow(schema);
  }
  public Schema toArrow(MessageType schema) throws UnsupportedParquetTypeException, UnsupportedNestedEncodingException {
    if (schema == null) throw new NullPointerException("schema");
    List<Field> fields = new ArrayList<>();
    for (Type field : schema.getFields()) fields.add(field(field, field.getName()));
    return new Schema(fields);
  }
  public static Schema fromParquet(MessageType schema) throws UnsupportedParquetTypeException, UnsupportedNestedEncodingException {
    return new ParquetToArrow().toArrow(schema);
  }

  private Field field(Type type, String path) throws UnsupportedParquetTypeException, UnsupportedNestedEncodingException {
    boolean nullable = type.getRepetition() != Type.Repetition.REQUIRED;
    ArrowType arrow;
    List<Field> children = List.of();
    LogicalTypeAnnotation annotation = type.getLogicalTypeAnnotation();
    if (annotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
      Type element = lists.resolve(type, path).element();
      Field child = field(element, path + "." + element.getName());
      arrow = ArrowType.List.INSTANCE; children = List.of(child);
    } else if (annotation instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation || annotation instanceof LogicalTypeAnnotation.MapKeyValueTypeAnnotation) {
      MapEncodingResolver.Resolution resolution = maps.resolve(type, path);
      Field key = field(resolution.key(), path + "." + resolution.key().getName());
      Field value = field(resolution.value(), path + "." + resolution.value().getName());
      Field entry = new Field("entries", new FieldType(false, ArrowType.Struct.INSTANCE, null, null), List.of(key, value));
      arrow = new ArrowType.Map(false); children = List.of(entry);
    } else if (!type.isPrimitive()) {
      if (annotation != null) reject(path, "unrecognized group annotation " + annotation);
      if (type.asGroupType().getFieldCount() == 0) reject(path, "empty struct has no physical descendant");
      List<Field> nested = new ArrayList<>();
      for (Type child : type.asGroupType().getFields()) nested.add(field(child, path + "." + child.getName()));
      arrow = ArrowType.Struct.INSTANCE; children = nested;
    } else {
      arrow = primitive(type.asPrimitiveType(), path);
    }
    return new Field(type.getName(), new FieldType(nullable, arrow, null, null), children);
  }

  private static ArrowType primitive(PrimitiveType type, String path) throws UnsupportedParquetTypeException {
    PrimitiveType.PrimitiveTypeName physical = type.getPrimitiveTypeName();
    LogicalTypeAnnotation a = type.getLogicalTypeAnnotation();
    if (a == null) return plain(physical, path, type.getTypeLength());
    if (a instanceof LogicalTypeAnnotation.UnknownLogicalTypeAnnotation) {
      require(physical == PrimitiveType.PrimitiveTypeName.INT32, path, "UNKNOWN requires INT32");
      require(type.getRepetition() == Type.Repetition.OPTIONAL, path, "UNKNOWN must be OPTIONAL");
      return ArrowType.Null.INSTANCE;
    }
    if (a instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
      require(physical == PrimitiveType.PrimitiveTypeName.BINARY, path, "STRING requires BINARY"); return ArrowType.Utf8.INSTANCE;
    }
    if (a instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
      require(physical == PrimitiveType.PrimitiveTypeName.INT32, path, "DATE requires INT32"); return new ArrowType.Date(DateUnit.DAY);
    }
    if (a instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
      LogicalTypeAnnotation.DecimalLogicalTypeAnnotation d = (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) a;
      int p = d.getPrecision(), s = d.getScale();
      if (p < 1 || p > 76 || s < 0 || s > p) reject(path, "invalid DECIMAL precision/scale");
      if (physical == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY && type.getTypeLength() == 32) return new ArrowType.Decimal(p, s, 256);
      if (p > 38) reject(path, "Decimal256 must use FLBA(32)");
      if (physical == PrimitiveType.PrimitiveTypeName.INT32) require(p <= 9, path, "INT32 DECIMAL precision");
      else if (physical == PrimitiveType.PrimitiveTypeName.INT64) require(p <= 18, path, "INT64 DECIMAL precision");
      else if (physical == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) require(type.getTypeLength() >= ArrowSchemaToParquet.minDecimalBytes(p), path, "FLBA DECIMAL width");
      else reject(path, "invalid DECIMAL physical type");
      return new ArrowType.Decimal(p, s, 128);
    }
    if (a instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
      LogicalTypeAnnotation.IntLogicalTypeAnnotation i = (LogicalTypeAnnotation.IntLogicalTypeAnnotation) a;
      int width = i.getBitWidth();
      if (width != 8 && width != 16 && width != 32 && width != 64) reject(path, "invalid INT width");
      require(physical == (width == 64 ? PrimitiveType.PrimitiveTypeName.INT64 : PrimitiveType.PrimitiveTypeName.INT32), path, "INT physical type");
      return new ArrowType.Int(width, i.isSigned());
    }
    if (a instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
      LogicalTypeAnnotation.TimeLogicalTypeAnnotation time = (LogicalTypeAnnotation.TimeLogicalTypeAnnotation) a;
      TimeUnit unit = arrowTimeUnit(time.getUnit(), path);
      require(physical == (unit == TimeUnit.MILLISECOND ? PrimitiveType.PrimitiveTypeName.INT32 : PrimitiveType.PrimitiveTypeName.INT64), path, "TIME physical type");
      return new ArrowType.Time(unit, unit == TimeUnit.MILLISECOND ? 32 : 64);
    }
    if (a instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
      LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts = (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) a;
      require(physical == PrimitiveType.PrimitiveTypeName.INT64, path, "TIMESTAMP requires INT64");
      return new ArrowType.Timestamp(arrowTimeUnit(ts.getUnit(), path), ts.isAdjustedToUTC() ? "UTC" : null);
    }
    if (a instanceof LogicalTypeAnnotation.IntervalLogicalTypeAnnotation) reject(path, "INTERVAL is not supported");
    reject(path, "unrecognized semantic annotation " + a); return null;
  }

  private static ArrowType plain(PrimitiveType.PrimitiveTypeName physical, String path, int length) throws UnsupportedParquetTypeException {
    switch (physical) {
      case BOOLEAN: return ArrowType.Bool.INSTANCE;
      case INT32: return new ArrowType.Int(32, true);  // bare Parquet INT32 -> signed 32-bit
      case INT64: return new ArrowType.Int(64, true);  // bare Parquet INT64 -> signed 64-bit
      case FLOAT: return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case DOUBLE: return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case BINARY: return ArrowType.Binary.INSTANCE;
      case FIXED_LEN_BYTE_ARRAY: if (length <= 0) reject(path, "invalid FLBA width"); return new ArrowType.FixedSizeBinary(length);
      default: reject(path, "unannotated physical type " + physical); return null;
    }
  }
  private static TimeUnit arrowTimeUnit(LogicalTypeAnnotation.TimeUnit unit, String path) throws UnsupportedParquetTypeException {
    switch (unit) { case MILLIS: return TimeUnit.MILLISECOND; case MICROS: return TimeUnit.MICROSECOND; case NANOS: return TimeUnit.NANOSECOND; default: reject(path, "unsupported time unit " + unit); return null; }
  }
  private static void require(boolean condition, String path, String reason) throws UnsupportedParquetTypeException { if (!condition) reject(path, reason); }
  private static void reject(String path, String reason) throws UnsupportedParquetTypeException { throw new UnsupportedParquetTypeException(path + ": " + reason); }
}
