// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

/** Registry for canonical MAP shapes; names are intentionally not semantic. */
public final class MapEncodingResolver {
  public enum Reason { NOT_MAP, OUTER_NOT_GROUP, MAP_KEY_VALUE_OUTER, OUTER_NOT_SINGLE_REPEATED_GROUP, ENTRY_NOT_GROUP, KEY_ONLY, EXTRA_ENTRY_FIELDS, OPTIONAL_KEY, REPEATED_KEY }
  public static final class Resolution {
    private final Type key; private final Type value;
    private Resolution(Type key, Type value) { this.key = key; this.value = value; }
    public Type key() { return key; } public Type value() { return value; }
  }
  public Resolution resolve(Type outer, String path) throws UnsupportedNestedEncodingException {
    if (outer.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.MapKeyValueTypeAnnotation) fail(path, Reason.MAP_KEY_VALUE_OUTER);
    if (!(outer.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation)) fail(path, Reason.NOT_MAP);
    if (outer.isPrimitive()) fail(path, Reason.OUTER_NOT_GROUP);
    GroupType map = outer.asGroupType();
    if (map.getFieldCount() != 1) fail(path, Reason.OUTER_NOT_SINGLE_REPEATED_GROUP);
    Type entriesType = map.getType(0);
    if (entriesType.getRepetition() != Type.Repetition.REPEATED) fail(path, Reason.OUTER_NOT_SINGLE_REPEATED_GROUP);
    if (entriesType.isPrimitive()) fail(path, Reason.ENTRY_NOT_GROUP);
    GroupType entries = entriesType.asGroupType();
    if (entries.getFieldCount() == 1) fail(path, Reason.KEY_ONLY);
    if (entries.getFieldCount() != 2) fail(path, Reason.EXTRA_ENTRY_FIELDS);
    Type key = entries.getType(0);
    if (key.getRepetition() == Type.Repetition.OPTIONAL) fail(path, Reason.OPTIONAL_KEY);
    if (key.getRepetition() == Type.Repetition.REPEATED) fail(path, Reason.REPEATED_KEY);
    return new Resolution(key, entries.getType(1));
  }
  private static void fail(String path, Reason reason) throws UnsupportedNestedEncodingException {
    throw new UnsupportedNestedEncodingException(reason.name() + " at " + path);
  }
}
