// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow.conformance;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Semantic Arrow oracle.  It intentionally has four independent dimensions: callers can diagnose a
 * metadata disagreement without losing a value disagreement hidden behind it.  This compares vector
 * contents rather than relying on vector implementations' {@code equals} methods.
 */
public final class ArrowLogicalComparator {
  public enum Dimension { SCHEMA, LOGICAL_VALUES, NULL_EMPTY_NESTED_SHAPE, FIELD_METADATA }
  public record Difference(Dimension dimension, String path, String detail) {}

  public static final class Result {
    private final List<Difference> differences;
    private Result(List<Difference> differences) { this.differences = List.copyOf(differences); }
    public boolean matches() { return differences.isEmpty(); }
    public List<Difference> differences() { return differences; }
    public List<Difference> differences(Dimension dimension) {
      return differences.stream().filter(d -> d.dimension() == dimension).toList();
    }
    public void requireMatch() {
      if (!matches()) throw new AssertionError("Arrow logical mismatch: " + differences);
    }
  }

  public Result compare(VectorSchemaRoot expected, VectorSchemaRoot actual) {
    Objects.requireNonNull(expected, "expected"); Objects.requireNonNull(actual, "actual");
    List<Difference> out = new ArrayList<>();
    compareSchema(expected.getSchema(), actual.getSchema(), "$", out);
    compareMetadata(expected.getSchema().getFields(), actual.getSchema().getFields(), "$", out);
    if (expected.getRowCount() != actual.getRowCount()) {
      out.add(new Difference(Dimension.LOGICAL_VALUES, "$", "row count " + expected.getRowCount() + " != " + actual.getRowCount()));
      return new Result(out);
    }
    for (int row = 0; row < expected.getRowCount(); row++) compareRowValues(expected, row, actual, row, out);
    return new Result(out);
  }

  /** Compares one logical row from independently batched streams. */
  public Result compareRow(VectorSchemaRoot expected, int expectedRow, VectorSchemaRoot actual, int actualRow) {
    Objects.requireNonNull(expected, "expected"); Objects.requireNonNull(actual, "actual");
    List<Difference> out = new ArrayList<>();
    compareSchema(expected.getSchema(), actual.getSchema(), "$", out);
    compareMetadata(expected.getSchema().getFields(), actual.getSchema().getFields(), "$", out);
    if (expectedRow < 0 || expectedRow >= expected.getRowCount() || actualRow < 0 || actualRow >= actual.getRowCount()) {
      out.add(new Difference(Dimension.LOGICAL_VALUES, "$", "row outside batch bounds"));
    } else compareRowValues(expected, expectedRow, actual, actualRow, out);
    return new Result(out);
  }

  private static void compareRowValues(VectorSchemaRoot expected, int expectedRow, VectorSchemaRoot actual, int actualRow,
      List<Difference> out) {
    int vectorCount = Math.min(expected.getFieldVectors().size(), actual.getFieldVectors().size());
    for (int i = 0; i < vectorCount; i++) {
      FieldVector left = expected.getFieldVectors().get(i), right = actual.getFieldVectors().get(i);
      Field field = left.getField(); String path = "$." + field.getName() + "[" + expectedRow + "]";
      compareShape(field, left, right, expectedRow, actualRow, path, out);
      compareLogical(field, left.getObject(expectedRow), right.getObject(actualRow), path, out);
    }
  }

  /** Schema-only comparison honouring the same name-normalisation the row oracle uses. */
  public Result compareSchemas(Schema expected, Schema actual) {
    Objects.requireNonNull(expected, "expected"); Objects.requireNonNull(actual, "actual");
    List<Difference> out = new ArrayList<>();
    compareSchema(expected, actual, "$", out);
    return new Result(out);
  }

  private static void compareSchema(Schema left, Schema right, String path, List<Difference> out) {
    if (left.getFields().size() != right.getFields().size()) {
      out.add(new Difference(Dimension.SCHEMA, path, "field count " + left.getFields().size() + " != " + right.getFields().size())); return;
    }
    for (int i = 0; i < left.getFields().size(); i++) compareFieldSchema(left.getFields().get(i), right.getFields().get(i), path + "." + i, true, out);
  }

  // Arrow does not stabilise the *name* of a List's element or a Map's entry wrapper across the
  // Parquet boundary: ArrowFileReader's loaded ListVector reports the internal "$data$" while a
  // fresh Parquet read reports "item"/"element". Those wrapper names carry no logical data, so the
  // oracle compares their type/nullability/nested shape but not their name.  Struct field names and
  // Map key/value names ARE preserved, so name equality still applies everywhere else.
  private static void compareFieldSchema(Field left, Field right, String path, boolean nameSignificant, List<Difference> out) {
    if ((nameSignificant && !left.getName().equals(right.getName())) || !typesCompatible(left.getType(), right.getType()) || left.isNullable() != right.isNullable()
        || !Objects.equals(left.getFieldType().getDictionary(), right.getFieldType().getDictionary())) {
      out.add(new Difference(Dimension.SCHEMA, path, "field " + display(left) + " != " + display(right))); return;
    }
    if (left.getChildren().size() != right.getChildren().size()) {
      out.add(new Difference(Dimension.SCHEMA, path, "child count " + left.getChildren().size() + " != " + right.getChildren().size())); return;
    }
    boolean childNamesSignificant = !isNameNormalizingContainer(left.getType());
    for (int i = 0; i < left.getChildren().size(); i++) compareFieldSchema(left.getChildren().get(i), right.getChildren().get(i), path + "." + left.getName(), childNamesSignificant, out);
  }

  private static boolean isNameNormalizingContainer(ArrowType type) {
    return type instanceof ArrowType.List || type instanceof ArrowType.LargeList
        || type instanceof ArrowType.FixedSizeList || type instanceof ArrowType.Map;
  }

  // Parquet's finest sub-day resolution is MILLIS; Arrow SECOND-precision temporal types have no
  // Parquet equivalent and are promoted to MILLIS on write (values scale, the instant is preserved).
  // That single promotion is the only accepted type difference; everything else must match exactly.
  private static boolean typesCompatible(ArrowType left, ArrowType right) {
    if (left.equals(right)) return true;
    if (left instanceof ArrowType.Timestamp a && right instanceof ArrowType.Timestamp b) {
      return Objects.equals(a.getTimezone(), b.getTimezone()) && secondToMillis(a.getUnit(), b.getUnit());
    }
    // Time32(SECOND) promotes to Parquet TIME(MILLIS) (bit width widens accordingly). Both sides
    // still compare their logical values.
    if (left instanceof ArrowType.Time a && right instanceof ArrowType.Time b) {
      return secondToMillis(a.getUnit(), b.getUnit());
    }
    return false;
  }

  private static boolean secondToMillis(org.apache.arrow.vector.types.TimeUnit left, org.apache.arrow.vector.types.TimeUnit right) {
    return left == org.apache.arrow.vector.types.TimeUnit.SECOND && right == org.apache.arrow.vector.types.TimeUnit.MILLISECOND;
  }

  private static String display(Field field) { return field.getName() + ":" + field.getType() + (field.isNullable() ? "?" : "!"); }

  private static void compareMetadata(List<Field> left, List<Field> right, String path, List<Difference> out) {
    int fields = Math.min(left.size(), right.size());
    for (int i = 0; i < fields; i++) {
      Field a = left.get(i), b = right.get(i);
      if (!Objects.equals(a.getMetadata(), b.getMetadata()))
        out.add(new Difference(Dimension.FIELD_METADATA, path + "." + a.getName(), "metadata " + a.getMetadata() + " != " + b.getMetadata()));
      compareMetadata(a.getChildren(), b.getChildren(), path + "." + a.getName(), out);
    }
  }

  private static void compareShape(Field field, ValueVector left, ValueVector right, int leftIndex, int rightIndex, String path, List<Difference> out) {
    boolean aNull = left.isNull(leftIndex), bNull = right.isNull(rightIndex);
    if (aNull != bNull) {
      out.add(new Difference(Dimension.NULL_EMPTY_NESTED_SHAPE, path, "null " + aNull + " != " + bNull)); return;
    }
    if (aNull) return;
    Object a = left.getObject(leftIndex), b = right.getObject(rightIndex);
    compareContainerShape(a, b, path, out);
  }

  @SuppressWarnings("unchecked")
  private static void compareContainerShape(Object a, Object b, String path, List<Difference> out) {
    if (a instanceof List<?> || b instanceof List<?>) {
      if (!(a instanceof List<?>) || !(b instanceof List<?>)) { shape(out, path, "container kind differs"); return; }
      List<?> left = (List<?>) a, right = (List<?>) b;
      if (left.size() != right.size()) { shape(out, path, "list length " + left.size() + " != " + right.size()); return; }
      for (int i = 0; i < left.size(); i++) {
        if ((left.get(i) == null) != (right.get(i) == null)) shape(out, path + "[" + i + "]", "nested null differs");
        else if (left.get(i) != null) compareContainerShape(left.get(i), right.get(i), path + "[" + i + "]", out);
      }
      return;
    }
    if (a instanceof Map<?, ?> || b instanceof Map<?, ?>) {
      if (!(a instanceof Map<?, ?>) || !(b instanceof Map<?, ?>)) { shape(out, path, "map/struct kind differs"); return; }
      Map<?, ?> left = (Map<?, ?>) a, right = (Map<?, ?>) b;
      if (!left.keySet().equals(right.keySet())) { shape(out, path, "map/struct keys differ"); return; }
      for (Object key : left.keySet()) {
        Object lv = left.get(key), rv = right.get(key);
        if ((lv == null) != (rv == null)) shape(out, path + "." + key, "nested null differs");
        else if (lv != null) compareContainerShape(lv, rv, path + "." + key, out);
      }
    }
  }

  private static void shape(List<Difference> out, String path, String detail) { out.add(new Difference(Dimension.NULL_EMPTY_NESTED_SHAPE, path, detail)); }

  @SuppressWarnings("unchecked")
  private static void compareLogical(Field field, Object left, Object right, String path, List<Difference> out) {
    if (left == null || right == null) { if (left != right) logical(out, path, left + " != " + right); return; }
    if (left instanceof List<?> || right instanceof List<?>) {
      if (!(left instanceof List<?>) || !(right instanceof List<?>)) { logical(out, path, "container kind differs"); return; }
      List<?> a = (List<?>) left, b = (List<?>) right;
      int n = Math.min(a.size(), b.size());
      Field child = field.getChildren().isEmpty() ? field : field.getChildren().get(0);
      for (int i = 0; i < n; i++) compareLogical(child, a.get(i), b.get(i), path + "[" + i + "]", out);
      return;
    }
    if (left instanceof Map<?, ?> || right instanceof Map<?, ?>) {
      if (!(left instanceof Map<?, ?>) || !(right instanceof Map<?, ?>)) { logical(out, path, "map/struct kind differs"); return; }
      Map<?, ?> a = (Map<?, ?>) left, b = (Map<?, ?>) right;
      if (field.getType() instanceof ArrowType.Map) {
        List<? extends Map.Entry<?, ?>> leftEntries = new ArrayList<>(a.entrySet());
        List<? extends Map.Entry<?, ?>> rightEntries = new ArrayList<>(b.entrySet());
        if (leftEntries.size() != rightEntries.size()) { logical(out, path, "map entry count differs"); return; }
        Field entry = field.getChildren().get(0);
        Field key = entry.getChildren().get(0), value = entry.getChildren().get(1);
        for (int i = 0; i < leftEntries.size(); i++) {
          Map.Entry<?, ?> le = leftEntries.get(i), re = rightEntries.get(i);
          // A Map's key set alone is not a Parquet MAP value: entry ordering is part of this
          // conformance fixture contract and catches adapters that sort or hash entries.
          compareLogical(key, le.getKey(), re.getKey(), path + "[" + i + "].key", out);
          compareLogical(value, le.getValue(), re.getValue(), path + "[" + i + "].value", out);
        }
        return;
      }
      for (Object key : a.keySet()) if (b.containsKey(key)) compareLogical(child(field, String.valueOf(key)), a.get(key), b.get(key), path + "." + key, out);
      return;
    }
    if (left instanceof Float a && right instanceof Float b) { if (!floatEqual(a, b)) logical(out, path, a + " != " + b); return; }
    if (left instanceof Double a && right instanceof Double b) { if (!doubleEqual(a, b)) logical(out, path, a + " != " + b); return; }
    if (left instanceof BigDecimal a && right instanceof BigDecimal b) { if (a.scale() != b.scale() || a.unscaledValue().compareTo(b.unscaledValue()) != 0) logical(out, path, a + " != " + b); return; }
    if (left instanceof Number a && right instanceof Number b) { if (!integerEqual(field.getType(), a, b)) logical(out, path, a + " != " + b); return; }
    if (left instanceof byte[] a && right instanceof byte[] b) { if (!Arrays.equals(a, b)) logical(out, path, "binary differs"); return; }
    if (left instanceof ByteBuffer a && right instanceof ByteBuffer b) { if (!byteBufferEqual(a, b)) logical(out, path, "binary differs"); return; }
    if (!Objects.equals(left, right)) logical(out, path, left + " != " + right);
  }

  private static Field child(Field parent, String name) {
    for (Field child : parent.getChildren()) if (child.getName().equals(name)) return child;
    // Map vector objects commonly expose entry values under their actual key. In that case the
    // entry's value field supplies the logical type; schema comparison already proves the shape.
    if (parent.getType() instanceof ArrowType.Map && parent.getChildren().size() == 1) {
      List<Field> entryChildren = parent.getChildren().get(0).getChildren();
      if (entryChildren.size() == 2) return entryChildren.get(1);
    }
    return parent;
  }

  private static boolean integerEqual(ArrowType type, Number a, Number b) {
    if (type instanceof ArrowType.Int integer && !integer.getIsSigned()) {
      int width = integer.getBitWidth();
      if (width == 64) return Long.compareUnsigned(a.longValue(), b.longValue()) == 0;
      long mask = (1L << width) - 1L;
      return (a.longValue() & mask) == (b.longValue() & mask);
    }
    return new BigInteger(a.toString()).equals(new BigInteger(b.toString()));
  }

  private static boolean floatEqual(float a, float b) { return Float.isNaN(a) && Float.isNaN(b) || Float.floatToRawIntBits(a) == Float.floatToRawIntBits(b); }
  private static boolean doubleEqual(double a, double b) { return Double.isNaN(a) && Double.isNaN(b) || Double.doubleToRawLongBits(a) == Double.doubleToRawLongBits(b); }
  private static boolean byteBufferEqual(ByteBuffer a, ByteBuffer b) { ByteBuffer x = a.duplicate(), y = b.duplicate(); if (x.remaining() != y.remaining()) return false; while (x.hasRemaining()) if (x.get() != y.get()) return false; return true; }
  private static void logical(List<Difference> out, String path, String detail) { out.add(new Difference(Dimension.LOGICAL_VALUES, path, detail)); }
}
