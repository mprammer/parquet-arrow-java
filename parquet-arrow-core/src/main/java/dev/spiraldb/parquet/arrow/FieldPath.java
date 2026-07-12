// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** A projection path expressed as field-name segments (never as a dotted string). */
public final class FieldPath {
  private final List<String> segments;
  private FieldPath(List<String> segments) { this.segments = List.copyOf(segments); }
  public static FieldPath of(String... segments) {
    Objects.requireNonNull(segments, "segments");
    if (segments.length == 0) throw new IllegalArgumentException("a field path is not empty");
    for (String segment : segments) if (segment == null || segment.isEmpty()) throw new IllegalArgumentException("field path contains an empty segment");
    return new FieldPath(Arrays.asList(segments.clone()));
  }
  public List<String> segments() { return segments; }
  @Override public boolean equals(Object other) { return other instanceof FieldPath && segments.equals(((FieldPath) other).segments); }
  @Override public int hashCode() { return segments.hashCode(); }
  @Override public String toString() { return String.join(".", segments); }
}
