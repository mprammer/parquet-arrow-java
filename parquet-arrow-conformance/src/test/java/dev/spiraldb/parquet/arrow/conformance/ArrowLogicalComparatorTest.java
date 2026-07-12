// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow.conformance;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

class ArrowLogicalComparatorTest {
  private final ArrowLogicalComparator comparator = new ArrowLogicalComparator();

  @Test void catchesSeededLogicalValueFaultAndTreatsNaNsAsEqual() {
    try (RootAllocator allocator = new RootAllocator(); VectorSchemaRoot left = ArrowFixtureBuilder.primitiveBoundaries(allocator);
         VectorSchemaRoot right = ArrowFixtureBuilder.primitiveBoundaries(allocator)) {
      assertTrue(comparator.compare(left, right).matches());
      ((IntVector) right.getVector("i32")).setSafe(1, 7);
      assertFalse(comparator.compare(left, right).differences(ArrowLogicalComparator.Dimension.LOGICAL_VALUES).isEmpty());
    }
  }

  @Test void catchesSeededSchemaFault() {
    Schema a = new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(32, true)), List.of())));
    Schema b = new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), List.of())));
    try (RootAllocator allocator = new RootAllocator(); VectorSchemaRoot left = VectorSchemaRoot.create(a, allocator);
         VectorSchemaRoot right = VectorSchemaRoot.create(b, allocator)) {
      assertFalse(comparator.compare(left, right).differences(ArrowLogicalComparator.Dimension.SCHEMA).isEmpty());
    }
  }

  @Test void catchesSeededNullShapeFault() {
    try (RootAllocator allocator = new RootAllocator(); VectorSchemaRoot left = ArrowFixtureBuilder.primitiveBoundaries(allocator);
         VectorSchemaRoot right = ArrowFixtureBuilder.primitiveBoundaries(allocator)) {
      ((IntVector) right.getVector("i32")).setSafe(2, 9);
      assertFalse(comparator.compare(left, right).differences(ArrowLogicalComparator.Dimension.NULL_EMPTY_NESTED_SHAPE).isEmpty());
    }
  }

  @Test void catchesSeededFieldMetadataFault() {
    Field withMetadata = new Field("x", new FieldType(true, new ArrowType.Int(32, true), null, Map.of("origin", "fixture")), List.of());
    Field withoutMetadata = new Field("x", FieldType.nullable(new ArrowType.Int(32, true)), List.of());
    try (RootAllocator allocator = new RootAllocator(); VectorSchemaRoot left = VectorSchemaRoot.create(new Schema(List.of(withMetadata)), allocator);
         VectorSchemaRoot right = VectorSchemaRoot.create(new Schema(List.of(withoutMetadata)), allocator)) {
      assertFalse(comparator.compare(left, right).differences(ArrowLogicalComparator.Dimension.FIELD_METADATA).isEmpty());
    }
  }

  // Regression: compareRow must apply the expected/actual indices to the correct side. When the two
  // streams batch differently the same logical row lands at different in-batch indices; a single
  // shared index silently checked the null-shape of the WRONG actual row (a false NULL_SHAPE fault).
  @Test void compareRowUsesIndependentIndicesForNullShape() {
    Schema schema = new Schema(List.of(new Field("i32", FieldType.nullable(new ArrowType.Int(32, true)), List.of())));
    try (RootAllocator allocator = new RootAllocator(); VectorSchemaRoot left = VectorSchemaRoot.create(schema, allocator);
         VectorSchemaRoot right = VectorSchemaRoot.create(schema, allocator)) {
      IntVector l = (IntVector) left.getVector("i32"); l.setNull(0); l.setSafe(1, 42); left.setRowCount(2);
      IntVector r = (IntVector) right.getVector("i32"); r.setSafe(0, 42); r.setNull(1); right.setRowCount(2);
      // logical row = (left index 1) vs (right index 0): both hold 42, so it must match cleanly.
      assertTrue(comparator.compareRow(left, 1, right, 0).matches());
      // sanity: the mismatched pairing (index 0 vs 0) genuinely differs in null shape.
      assertFalse(comparator.compareRow(left, 0, right, 0).matches());
    }
  }

  // A List element and Map entry wrapper name is not preserved across the Parquet boundary; the
  // oracle compares their shape but not their name, while struct field names still matter.
  @Test void schemaComparisonNormalisesListElementName() {
    ArrowType.Int i32 = new ArrowType.Int(32, true);
    Schema withItem = new Schema(List.of(new Field("xs", FieldType.nullable(new ArrowType.List()),
        List.of(new Field("item", FieldType.nullable(i32), List.of())))));
    Schema withData = new Schema(List.of(new Field("xs", FieldType.nullable(new ArrowType.List()),
        List.of(new Field("$data$", FieldType.nullable(i32), List.of())))));
    assertTrue(comparator.compareSchemas(withData, withItem).matches());
    Schema withStruct = new Schema(List.of(new Field("s", FieldType.nullable(new ArrowType.Struct()),
        List.of(new Field("a", FieldType.nullable(i32), List.of())))));
    Schema withRenamedStruct = new Schema(List.of(new Field("s", FieldType.nullable(new ArrowType.Struct()),
        List.of(new Field("b", FieldType.nullable(i32), List.of())))));
    assertFalse(comparator.compareSchemas(withStruct, withRenamedStruct).matches());
  }

  // Parquet has no second-precision timestamp, so SECOND -> MILLIS is an accepted promotion; other
  // unit changes remain faults.
  @Test void schemaComparisonAcceptsTimestampSecondToMillisPromotion() {
    Schema second = new Schema(List.of(new Field("t", FieldType.nullable(new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.SECOND, null)), List.of())));
    Schema millis = new Schema(List.of(new Field("t", FieldType.nullable(new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null)), List.of())));
    Schema micros = new Schema(List.of(new Field("t", FieldType.nullable(new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, null)), List.of())));
    assertTrue(comparator.compareSchemas(second, millis).matches());
    assertFalse(comparator.compareSchemas(millis, micros).matches());
    // a SECOND->MILLIS promotion across a differing timezone identity is still a fault
    Schema secondTz = new Schema(List.of(new Field("t", FieldType.nullable(new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.SECOND, "UTC")), List.of())));
    assertFalse(comparator.compareSchemas(secondTz, millis).matches());
  }

  @Test void schemaComparisonAcceptsTimeSecondToMillisPromotion() {
    Schema second = new Schema(List.of(new Field("clock", FieldType.nullable(new ArrowType.Time(org.apache.arrow.vector.types.TimeUnit.SECOND, 32)), List.of())));
    Schema millis = new Schema(List.of(new Field("clock", FieldType.nullable(new ArrowType.Time(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, 32)), List.of())));
    Schema micros = new Schema(List.of(new Field("clock", FieldType.nullable(new ArrowType.Time(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, 64)), List.of())));
    assertTrue(comparator.compareSchemas(second, millis).matches());
    assertFalse(comparator.compareSchemas(millis, micros).matches());
  }
}
