// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow.conformance;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Deterministic Stage-3 Arrow inputs.  Values are deliberately literal: fixtures must never depend
 * on clocks, locale, random generators, or the iteration order of a hash map.
 */
public final class ArrowFixtureBuilder {
  public static final int MANIFEST_VERSION = 1;
  public static final String GENERATOR = "stage3-canonical-arrow-fixtures-v1";

  private ArrowFixtureBuilder() {}

  public static FixtureManifest manifest() {
    String json = resource("fixtures/manifest-v1.json");
    String expectedHash = resource("fixtures/manifest-v1.sha256").trim();
    return new FixtureManifest(MANIFEST_VERSION, GENERATOR, json, expectedHash);
  }

  /** A compact vector root covering nulls, signed boundaries, IEEE edge cases, UTF-8 and bytes. */
  public static VectorSchemaRoot primitiveBoundaries(BufferAllocator allocator) {
    Schema schema = new Schema(List.of(
        field("flag", ArrowType.Bool.INSTANCE),
        field("i32", new ArrowType.Int(32, true)),
        field("i64", new ArrowType.Int(64, true)),
        field("f64", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
        field("text", ArrowType.Utf8.INSTANCE), field("bytes", ArrowType.Binary.INSTANCE)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    BitVector flag = (BitVector) root.getVector("flag");
    IntVector i32 = (IntVector) root.getVector("i32");
    BigIntVector i64 = (BigIntVector) root.getVector("i64");
    Float8Vector f64 = (Float8Vector) root.getVector("f64");
    VarCharVector text = (VarCharVector) root.getVector("text");
    VarBinaryVector bytes = (VarBinaryVector) root.getVector("bytes");
    root.allocateNew();
    flag.setSafe(0, 1); i32.setSafe(0, Integer.MIN_VALUE); i64.setSafe(0, Long.MIN_VALUE);
    f64.setSafe(0, -0.0d); text.setSafe(0, "".getBytes(StandardCharsets.UTF_8)); bytes.setSafe(0, new byte[0]);
    flag.setSafe(1, 0); i32.setSafe(1, Integer.MAX_VALUE); i64.setSafe(1, Long.MAX_VALUE);
    f64.setSafe(1, Double.NaN); text.setSafe(1, "snowman ☃".getBytes(StandardCharsets.UTF_8)); bytes.setSafe(1, new byte[] {0, -1, 1});
    flag.setNull(2); i32.setNull(2); i64.setNull(2); f64.setNull(2); text.setNull(2); bytes.setNull(2);
    root.setRowCount(3);
    return root;
  }

  /** Canonical fixture schema covering every Stage-2-supported family, including nested containers. */
  public static Schema supportedTypeSchema() {
    Field element = field("element", new ArrowType.Int(32, true));
    Field innerElement = field("element", new ArrowType.Int(32, true));
    Field innerList = new Field("element", FieldType.nullable(ArrowType.List.INSTANCE), List.of(innerElement));
    Field key = new Field("key", new FieldType(false, ArrowType.Utf8.INSTANCE, null, null), List.of());
    Field value = field("value", ArrowType.Binary.INSTANCE);
    Field entry = new Field("entries", new FieldType(false, ArrowType.Struct.INSTANCE, null, null), List.of(key, value));
    return new Schema(List.of(
        field("null", ArrowType.Null.INSTANCE), field("bool", ArrowType.Bool.INSTANCE),
        field("i8", new ArrowType.Int(8, true)), field("u8", new ArrowType.Int(8, false)),
        field("i16", new ArrowType.Int(16, true)), field("u16", new ArrowType.Int(16, false)),
        field("i32", new ArrowType.Int(32, true)), field("u32", new ArrowType.Int(32, false)),
        field("i64", new ArrowType.Int(64, true)), field("u64", new ArrowType.Int(64, false)),
        field("f32", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE)),
        field("f64", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
        field("utf8", ArrowType.Utf8.INSTANCE), field("large_utf8", ArrowType.LargeUtf8.INSTANCE),
        field("binary", ArrowType.Binary.INSTANCE), field("large_binary", ArrowType.LargeBinary.INSTANCE),
        field("fixed_binary", new ArrowType.FixedSizeBinary(4)), field("decimal128", new ArrowType.Decimal(38, 0, 128)),
        field("decimal256", new ArrowType.Decimal(76, 3, 256)), field("date32", new ArrowType.Date(DateUnit.DAY)),
        field("date64", new ArrowType.Date(DateUnit.MILLISECOND)), field("time64", new ArrowType.Time(TimeUnit.MICROSECOND, 64)),
        field("timestamp", new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC")), field("duration", new ArrowType.Duration(TimeUnit.NANOSECOND)),
        new Field("struct", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(field("member", ArrowType.Utf8.INSTANCE))),
        new Field("list", FieldType.nullable(ArrowType.List.INSTANCE), List.of(element)),
        new Field("nested_list", FieldType.nullable(ArrowType.List.INSTANCE), List.of(innerList)),
        new Field("map", FieldType.nullable(new ArrowType.Map(false)), List.of(entry))));
  }

  /** Versioned manifest rows document the null/empty/nested fixtures exercised by later stages. */
  public static List<String> canonicalFixtureIds() {
    return List.of("primitive-boundaries", "nullable-list", "nested-list", "nullable-map", "all-null-struct");
  }

  private static Field field(String name, ArrowType type) {
    return new Field(name, FieldType.nullable(type), List.of());
  }

  private static String resource(String name) {
    try (InputStream stream = ArrowFixtureBuilder.class.getClassLoader().getResourceAsStream(name)) {
      if (stream == null) throw new IllegalStateException("missing fixture resource " + name);
      return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new IllegalStateException("cannot read fixture resource " + name, e);
    }
  }
}
