// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Immutable name-segment projection.  An empty path set deliberately selects no columns. */
public final class ColumnProjection {
  private final List<FieldPath> paths; private final boolean all;
  private ColumnProjection(boolean all, List<FieldPath> paths) { this.all = all; this.paths = List.copyOf(paths); }
  public static ColumnProjection all() { return new ColumnProjection(true, List.of()); }
  public static ColumnProjection paths(FieldPath... paths) {
    Objects.requireNonNull(paths, "paths");
    for (FieldPath path : paths) Objects.requireNonNull(path, "path");
    return new ColumnProjection(false, Arrays.asList(paths.clone()));
  }
  boolean isAll() { return all; }
  List<FieldPath> paths() { return paths; }
}
