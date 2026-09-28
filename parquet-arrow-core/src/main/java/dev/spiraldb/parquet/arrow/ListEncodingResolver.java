// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

/**
 * Resolves a {@code LIST}-annotated group to its element as parquet-format's LogicalTypes.md,
 * "Lists", defines it: the 3-level structure under any names, and the 2-level and legacy
 * structures under that section's backward-compatibility rules.
 */
public final class ListEncodingResolver {
  public enum Reason { NOT_LIST, OUTER_NOT_GROUP, OUTER_NOT_SINGLE_FIELD, FIELD_NOT_REPEATED, EMPTY_REPEATED_GROUP }
  public static final class Resolution {
    private final Type element; private final boolean twoLevel, elementNullable;
    private Resolution(Type element, boolean twoLevel, boolean elementNullable) {
      this.element = element; this.twoLevel = twoLevel; this.elementNullable = elementNullable;
    }
    /** The Parquet field that holds one element's value. */
    public Type element() { return element; }
    /** True when the repeated field is itself the element (a 2-level list), not its wrapper. */
    public boolean twoLevel() { return twoLevel; }
    /** Element nullability; a 2-level list's elements are always required. */
    public boolean elementNullable() { return elementNullable; }
  }
  public Resolution resolve(Type outer, String path) throws UnsupportedNestedEncodingException {
    if (!(outer.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation)) fail(path, Reason.NOT_LIST);
    if (outer.isPrimitive()) fail(path, Reason.OUTER_NOT_GROUP);
    GroupType list = outer.asGroupType();
    if (list.getFieldCount() != 1) fail(path, Reason.OUTER_NOT_SINGLE_FIELD);
    Type repeated = list.getType(0);
    if (repeated.getRepetition() != Type.Repetition.REPEATED) fail(path, Reason.FIELD_NOT_REPEATED);
    // The names "list" and "element" are required of writers, but "these names may not be used
    // in existing data and should not be enforced as errors when reading"; the element type
    // "should always be determined by the following rules" (LogicalTypes.md, Lists,
    // Backward-compatibility rules), applied in order.
    // 1. "If the repeated field is not a group, then its type is the element type and elements
    //    are required."
    if (repeated.isPrimitive()) return twoLevel(repeated);
    GroupType group = repeated.asGroupType();
    if (group.getFieldCount() == 0) fail(path, Reason.EMPTY_REPEATED_GROUP);
    // 2. "If the repeated field is a group with multiple fields, then its type is the element
    //    type and elements are required."
    if (group.getFieldCount() > 1) return twoLevel(repeated);
    Type only = group.getType(0);
    // 3. "If the repeated field is a group with one field with repeated repetition, then its type
    //    is the element type and elements are required."
    if (only.getRepetition() == Type.Repetition.REPEATED) return twoLevel(repeated);
    // 4. "If the repeated field is a group with one field and is named either array or uses the
    //    LIST-annotated group's name with _tuple appended then the repeated type is the element
    //    type and elements are required."
    if ("array".equals(group.getName()) || (outer.getName() + "_tuple").equals(group.getName())) return twoLevel(repeated);
    // 5. "Otherwise, the repeated field's type is the element type with the repeated field's
    //    repetition": the 3-level structure. The spec's Rule 5 example reads the group's one
    //    field (`optional binary str`) as the element, nullable because it is optional.
    return new Resolution(only, false, only.getRepetition() == Type.Repetition.OPTIONAL);
  }
  private static Resolution twoLevel(Type repeated) { return new Resolution(repeated, true, false); }
  private static void fail(String path, Reason reason) throws UnsupportedNestedEncodingException {
    throw new UnsupportedNestedEncodingException(reason.name() + " at " + path);
  }
}
