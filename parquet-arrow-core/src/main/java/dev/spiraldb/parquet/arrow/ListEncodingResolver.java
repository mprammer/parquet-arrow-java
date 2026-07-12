// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

/** Registry for the deliberately narrow, canonical Parquet LIST read shapes. */
public final class ListEncodingResolver {
  public enum Reason {
    NOT_LIST, OUTER_NOT_GROUP, OUTER_NOT_SINGLE_REPEATED_GROUP, TWO_LEVEL, REPEATED_PRIMITIVE,
    REPEATED_MULTI_FIELD_GROUP, REPEATED_ONLY_CHILD, LEGACY_ARRAY, LEGACY_TUPLE, LEGACY_BAG,
    LEGACY_ARRAY_ELEMENT, CHILD_NAME, UNREGISTERED
  }
  public static final class Resolution {
    private final Type element;
    private Resolution(Type element) { this.element = element; }
    public Type element() { return element; }
  }
  public Resolution resolve(Type outer, String path) throws UnsupportedNestedEncodingException {
    if (!(outer.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation)) fail(path, Reason.NOT_LIST);
    if (outer.isPrimitive()) fail(path, Reason.OUTER_NOT_GROUP);
    GroupType list = outer.asGroupType();
    if (list.getFieldCount() != 1) fail(path, Reason.OUTER_NOT_SINGLE_REPEATED_GROUP);
    Type wrapperType = list.getType(0);
    if (wrapperType.getRepetition() != Type.Repetition.REPEATED) fail(path, Reason.TWO_LEVEL);
    if (wrapperType.isPrimitive()) fail(path, Reason.REPEATED_PRIMITIVE);
    GroupType wrapper = wrapperType.asGroupType();
    if (wrapper.getFieldCount() != 1) fail(path, Reason.REPEATED_MULTI_FIELD_GROUP);
    Type element = wrapper.getType(0);
    String wrapperName = wrapper.getName();
    String childName = element.getName();
    if ("array".equals(wrapperName)) fail(path, Reason.LEGACY_ARRAY);
    if ((outer.getName() + "_tuple").equals(wrapperName)) fail(path, Reason.LEGACY_TUPLE);
    if ("bag".equals(wrapperName)) fail(path, Reason.LEGACY_BAG);
    if (!"list".equals(wrapperName)) fail(path, Reason.UNREGISTERED);
    if ("array_element".equals(childName)) fail(path, Reason.LEGACY_ARRAY_ELEMENT);
    if (element.getRepetition() == Type.Repetition.REPEATED) fail(path, Reason.REPEATED_ONLY_CHILD);
    if (!("element".equals(childName) || "item".equals(childName))) fail(path, Reason.CHILD_NAME);
    return new Resolution(element);
  }
  private static void fail(String path, Reason reason) throws UnsupportedNestedEncodingException {
    throw new UnsupportedNestedEncodingException(reason.name() + " at " + path);
  }
}
