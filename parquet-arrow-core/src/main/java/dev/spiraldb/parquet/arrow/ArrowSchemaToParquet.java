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
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/** Compiles the Arrow schema contract into canonical Parquet physical schema. */
public final class ArrowSchemaToParquet {
  /** The message name deliberately does not carry application schema semantics. */
  public static final String ROOT_NAME = "arrow";

  public MessageType convert(Schema schema) throws UnsupportedParquetTypeException {
    return toParquet(schema);
  }

  public static MessageType toParquet(Schema schema) throws UnsupportedParquetTypeException {
    if (schema == null) throw new NullPointerException("schema");
    validateUniqueSiblingNames(schema.getFields(), "");
    List<Type> fields = new ArrayList<>();
    for (Field field : schema.getFields()) fields.add(field(field, field.getName()));
    return new MessageType(ROOT_NAME, fields);
  }

  /**
   * Parquet's record-assembly model keys columns by field path, so duplicate sibling names cannot be
   * faithfully round-tripped (parquet-java's RecordReader throws while building its state machine).
   * Surface it as a clear, fail-fast translation limitation at write time.
   */
  private static void validateUniqueSiblingNames(List<Field> siblings, String path)
      throws UnsupportedParquetTypeException {
    java.util.Set<String> seen = new java.util.HashSet<>();
    for (Field sibling : siblings) {
      if (!seen.add(sibling.getName()))
        unsupported(path.isEmpty() ? sibling.getName() : path + "." + sibling.getName(),
            "duplicate field name cannot round-trip through Parquet");
      validateUniqueSiblingNames(sibling.getChildren(), path.isEmpty() ? sibling.getName() : path + "." + sibling.getName());
    }
  }

  private static Type field(Field field, String path) throws UnsupportedParquetTypeException {
    // An Arrow dictionary field describes the *value* type in Field.getType(); the physical
    // vector presented by a batch is an integer index vector.  Never let that implementation
    // detail leak into Parquet's physical schema.  In particular, a dictionary<Utf8, Int8>
    // is BINARY(UTF8), not an INT8-annotated INT32 leaf.
    if (field.getDictionary() != null) field = valueField(field);
    Type.Repetition repetition = field.isNullable() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;
    ArrowType type = field.getType();
    switch (type.getTypeID()) {
      case Null:
        requireNoChildren(field, path);
        return primitive(repetition, PrimitiveType.PrimitiveTypeName.INT32, field.getName(), LogicalTypeAnnotation.unknownType());
      case Bool:
        requireNoChildren(field, path);
        return primitive(repetition, PrimitiveType.PrimitiveTypeName.BOOLEAN, field.getName(), null);
      case Int:
        requireNoChildren(field, path);
        ArrowType.Int integer = (ArrowType.Int) type;
        int width = integer.getBitWidth();
        if (width != 8 && width != 16 && width != 32 && width != 64) unsupported(path, "integer width " + width);
        return primitive(repetition, width == 64 ? PrimitiveType.PrimitiveTypeName.INT64 : PrimitiveType.PrimitiveTypeName.INT32,
            field.getName(), LogicalTypeAnnotation.intType(width, integer.getIsSigned()));
      case FloatingPoint:
        requireNoChildren(field, path);
        FloatingPointPrecision precision = ((ArrowType.FloatingPoint) type).getPrecision();
        if (precision == FloatingPointPrecision.SINGLE)
          return primitive(repetition, PrimitiveType.PrimitiveTypeName.FLOAT, field.getName(), null);
        if (precision == FloatingPointPrecision.DOUBLE)
          return primitive(repetition, PrimitiveType.PrimitiveTypeName.DOUBLE, field.getName(), null);
        unsupported(path, "Float16"); return null;
      case Utf8:
      case LargeUtf8:
      case Utf8View:
        requireNoChildren(field, path);
        return primitive(repetition, PrimitiveType.PrimitiveTypeName.BINARY, field.getName(), LogicalTypeAnnotation.stringType());
      case Binary:
      case LargeBinary:
      case BinaryView:
        requireNoChildren(field, path);
        return primitive(repetition, PrimitiveType.PrimitiveTypeName.BINARY, field.getName(), null);
      case FixedSizeBinary:
        requireNoChildren(field, path);
        int bytes = ((ArrowType.FixedSizeBinary) type).getByteWidth();
        if (bytes <= 0) unsupported(path, "FixedSizeBinary width must be positive");
        return primitive(repetition, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, bytes, field.getName(), null);
      case Decimal:
        requireNoChildren(field, path);
        ArrowType.Decimal decimal = (ArrowType.Decimal) type;
        decimal(path, decimal);
        int p = decimal.getPrecision();
        int byteWidth = decimal.getBitWidth() == 256 ? 32 : minDecimalBytes(p);
        PrimitiveType.PrimitiveTypeName physical = decimal.getBitWidth() == 128 && p <= 9
            ? PrimitiveType.PrimitiveTypeName.INT32 : decimal.getBitWidth() == 128 && p <= 18
            ? PrimitiveType.PrimitiveTypeName.INT64 : PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
        return physical == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
            ? primitive(repetition, physical, byteWidth, field.getName(), LogicalTypeAnnotation.decimalType(decimal.getScale(), p))
            : primitive(repetition, physical, field.getName(), LogicalTypeAnnotation.decimalType(decimal.getScale(), p));
      case Date:
        requireNoChildren(field, path);
        if (((ArrowType.Date) type).getUnit() != DateUnit.DAY && ((ArrowType.Date) type).getUnit() != DateUnit.MILLISECOND)
          unsupported(path, "date unit");
        return primitive(repetition, PrimitiveType.PrimitiveTypeName.INT32, field.getName(), LogicalTypeAnnotation.dateType());
      case Time:
        requireNoChildren(field, path);
        ArrowType.Time time = (ArrowType.Time) type;
        LogicalTypeAnnotation.TimeUnit timeUnit = timeUnit(path, time.getUnit());
        if ((time.getUnit() == TimeUnit.SECOND || time.getUnit() == TimeUnit.MILLISECOND) && time.getBitWidth() != 32)
          unsupported(path, "Time32 must have bit width 32");
        if ((time.getUnit() == TimeUnit.MICROSECOND || time.getUnit() == TimeUnit.NANOSECOND) && time.getBitWidth() != 64)
          unsupported(path, "Time64 must have bit width 64");
        return primitive(repetition, time.getBitWidth() == 32 ? PrimitiveType.PrimitiveTypeName.INT32 : PrimitiveType.PrimitiveTypeName.INT64,
            field.getName(), LogicalTypeAnnotation.timeType(false, timeUnit));
      case Timestamp:
        requireNoChildren(field, path);
        ArrowType.Timestamp timestamp = (ArrowType.Timestamp) type;
        if ("".equals(timestamp.getTimezone())) unsupported(path, "empty timestamp timezone");
        return primitive(repetition, PrimitiveType.PrimitiveTypeName.INT64, field.getName(),
            LogicalTypeAnnotation.timestampType(timestamp.getTimezone() != null, timeUnit(path, timestamp.getUnit())));
      case Duration:
        requireNoChildren(field, path);
        return primitive(repetition, PrimitiveType.PrimitiveTypeName.INT64, field.getName(), null);
      case Struct:
        if (field.getChildren().isEmpty()) unsupported(path, "empty struct has no physical descendant");
        return group(repetition, field.getName(), null, children(field, path));
      case List:
      case LargeList:
      case FixedSizeList:
        return list(field, path, repetition);
      case Map:
        return map(field, path, repetition);
      case Interval:
        unsupported(path, "INTERVAL is not supported"); return null;
      case Union:
      case RunEndEncoded:
      case ListView:
      case LargeListView:
        unsupported(path, type.getTypeID() + " is not supported"); return null;
      default:
        unsupported(path, "unsupported Arrow type " + type); return null;
    }
  }

  /** Returns the same logical value field with only the Arrow dictionary binding removed. */
  static Field valueField(Field field) {
    if (field.getDictionary() == null) return field;
    FieldType type = new FieldType(field.isNullable(), field.getType(), null, field.getMetadata());
    return new Field(field.getName(), type, field.getChildren());
  }

  private static Type list(Field field, String path, Type.Repetition repetition) throws UnsupportedParquetTypeException {
    if (field.getChildren().size() != 1) unsupported(path, "list must have exactly one child");
    Field element = field.getChildren().get(0);
    Type child = field(element, path + "." + element.getName());
    GroupType wrapper = group(Type.Repetition.REPEATED, "list", null, List.of(child));
    return group(repetition, field.getName(), LogicalTypeAnnotation.listType(), List.of(wrapper));
  }

  private static Type map(Field field, String path, Type.Repetition repetition) throws UnsupportedParquetTypeException {
    if (field.getChildren().size() != 1) unsupported(path, "map must contain one entry struct");
    Field entry = field.getChildren().get(0);
    if (entry.getChildren().size() != 2) unsupported(path, "map entry must contain key and value");
    Field key = entry.getChildren().get(0);
    if (key.isNullable()) unsupported(path + "." + key.getName(), "map key must be required");
    Type parquetKey = field(key, path + "." + key.getName());
    Type parquetValue = field(entry.getChildren().get(1), path + "." + entry.getChildren().get(1).getName());
    GroupType entries = group(Type.Repetition.REPEATED, "key_value", null, List.of(parquetKey, parquetValue));
    return group(repetition, field.getName(), LogicalTypeAnnotation.mapType(), List.of(entries));
  }

  private static List<Type> children(Field field, String path) throws UnsupportedParquetTypeException {
    List<Type> result = new ArrayList<>();
    for (Field child : field.getChildren()) result.add(field(child, path + "." + child.getName()));
    return result;
  }

  private static PrimitiveType primitive(Type.Repetition r, PrimitiveType.PrimitiveTypeName p, String name, LogicalTypeAnnotation a) {
    org.apache.parquet.schema.Types.PrimitiveBuilder<PrimitiveType> b = org.apache.parquet.schema.Types.primitive(p, r);
    if (a != null) b.as(a);
    return b.named(name);
  }
  private static PrimitiveType primitive(Type.Repetition r, PrimitiveType.PrimitiveTypeName p, int length, String name, LogicalTypeAnnotation a) {
    org.apache.parquet.schema.Types.PrimitiveBuilder<PrimitiveType> b = org.apache.parquet.schema.Types.primitive(p, r).length(length);
    if (a != null) b.as(a);
    return b.named(name);
  }
  private static GroupType group(Type.Repetition r, String name, LogicalTypeAnnotation a, List<Type> children) {
    org.apache.parquet.schema.Types.GroupBuilder<GroupType> b = org.apache.parquet.schema.Types.buildGroup(r);
    if (a != null) b.as(a);
    for (Type c : children) b.addField(c);
    return b.named(name);
  }
  private static void requireNoChildren(Field field, String path) throws UnsupportedParquetTypeException {
    if (!field.getChildren().isEmpty()) unsupported(path, "primitive has children");
  }
  private static void decimal(String path, ArrowType.Decimal d) throws UnsupportedParquetTypeException {
    if (d.getBitWidth() != 128 && d.getBitWidth() != 256) unsupported(path, "decimal bit width must be 128 or 256");
    int max = d.getBitWidth() == 128 ? 38 : 76;
    if (d.getPrecision() < 1 || d.getPrecision() > max || d.getScale() < 0 || d.getScale() > d.getPrecision())
      unsupported(path, "invalid decimal precision/scale");
  }
  static int minDecimalBytes(int precision) {
    for (int bytes = 1; bytes <= 32; bytes++) if (java.math.BigInteger.TEN.pow(precision).subtract(java.math.BigInteger.ONE)
        .compareTo(java.math.BigInteger.ONE.shiftLeft(8 * bytes - 1).subtract(java.math.BigInteger.ONE)) <= 0) return bytes;
    throw new IllegalArgumentException("precision " + precision);
  }
  private static LogicalTypeAnnotation.TimeUnit timeUnit(String path, TimeUnit unit) throws UnsupportedParquetTypeException {
    switch (unit) { case SECOND: case MILLISECOND: return LogicalTypeAnnotation.TimeUnit.MILLIS; case MICROSECOND: return LogicalTypeAnnotation.TimeUnit.MICROS; case NANOSECOND: return LogicalTypeAnnotation.TimeUnit.NANOS; default: unsupported(path, "time unit " + unit); return null; }
  }
  private static void unsupported(String path, String reason) throws UnsupportedParquetTypeException { throw new UnsupportedParquetTypeException(path + ": " + reason); }
}
