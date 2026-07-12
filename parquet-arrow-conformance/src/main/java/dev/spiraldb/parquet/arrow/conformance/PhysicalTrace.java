// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow.conformance;

import java.util.List;

/** Decoded per-leaf trace and the page header forms that produced it. */
public record PhysicalTrace(String leafPath, int maxRepetitionLevel, int maxDefinitionLevel,
                            List<String> pageHeaders, List<PhysicalEvent> events) {
  public PhysicalTrace {
    if (leafPath == null || leafPath.isBlank()) throw new IllegalArgumentException("leafPath");
    if (maxRepetitionLevel < 0 || maxDefinitionLevel < 0) throw new IllegalArgumentException("max levels");
    pageHeaders = List.copyOf(pageHeaders); events = List.copyOf(events);
    for (String header : pageHeaders) if (!"V1".equals(header) && !"V2".equals(header)) throw new IllegalArgumentException("page header " + header);
    for (PhysicalEvent event : events) {
      if (event.repetitionLevel() > maxRepetitionLevel || event.definitionLevel() > maxDefinitionLevel) throw new IllegalArgumentException("event exceeds max levels");
      if (event.rowBoundary() != (event.repetitionLevel() == 0)) throw new IllegalArgumentException("row boundary must be exactly RL=0");
    }
  }
}
