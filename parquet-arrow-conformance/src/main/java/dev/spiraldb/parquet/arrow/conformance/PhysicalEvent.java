// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow.conformance;

import java.util.Arrays;
import java.util.Objects;

/** One independently decoded leaf event: (rowBoundary, RL, DL, valuePresent, value). */
public final class PhysicalEvent {
  private final boolean rowBoundary, valuePresent;
  private final int repetitionLevel, definitionLevel;
  private final Object value;

  public PhysicalEvent(boolean rowBoundary, int repetitionLevel, int definitionLevel, boolean valuePresent, Object value) {
    if (repetitionLevel < 0 || definitionLevel < 0) throw new IllegalArgumentException("levels must be non-negative");
    if (!valuePresent && value != null) throw new IllegalArgumentException("absent value must be null");
    this.rowBoundary = rowBoundary; this.repetitionLevel = repetitionLevel; this.definitionLevel = definitionLevel;
    this.valuePresent = valuePresent; this.value = copy(value);
  }
  public boolean rowBoundary() { return rowBoundary; }
  public int repetitionLevel() { return repetitionLevel; }
  public int definitionLevel() { return definitionLevel; }
  public boolean valuePresent() { return valuePresent; }
  public Object value() { return copy(value); }
  private static Object copy(Object value) { return value instanceof byte[] bytes ? bytes.clone() : value; }
  @Override public boolean equals(Object other) {
    if (!(other instanceof PhysicalEvent that)) return false;
    return rowBoundary == that.rowBoundary && repetitionLevel == that.repetitionLevel && definitionLevel == that.definitionLevel
        && valuePresent == that.valuePresent && valueEquals(value, that.value);
  }
  private static boolean valueEquals(Object a, Object b) { return a instanceof byte[] x && b instanceof byte[] y ? Arrays.equals(x, y) : Objects.equals(a, b); }
  @Override public int hashCode() { return Objects.hash(rowBoundary, repetitionLevel, definitionLevel, valuePresent, value instanceof byte[] b ? Arrays.hashCode(b) : value); }
  @Override public String toString() { return "(" + rowBoundary + "," + repetitionLevel + "," + definitionLevel + "," + valuePresent + "," + (value instanceof byte[] b ? Arrays.toString(b) : value) + ")"; }
}
