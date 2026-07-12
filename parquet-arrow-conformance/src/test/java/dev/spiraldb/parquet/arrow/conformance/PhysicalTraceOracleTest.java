// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow.conformance;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

class PhysicalTraceOracleTest {
  private static final PhysicalTrace EXPECTED = new PhysicalTrace("xs.list.element", 2, 5, List.of("V2"), List.of(
      new PhysicalEvent(true, 0, 0, false, null), new PhysicalEvent(true, 0, 1, false, null),
      new PhysicalEvent(true, 0, 2, false, null), new PhysicalEvent(false, 1, 3, false, null),
      new PhysicalEvent(false, 1, 4, false, null), new PhysicalEvent(false, 2, 5, true, 7),
      new PhysicalEvent(false, 1, 5, true, 8)));

  @Test void catchesRlResetFault() {
    PhysicalTrace actual = trace(List.of(
        new PhysicalEvent(true, 0, 0, false, null), new PhysicalEvent(true, 0, 1, false, null),
        new PhysicalEvent(true, 0, 2, false, null), new PhysicalEvent(false, 1, 3, false, null),
        new PhysicalEvent(false, 1, 4, false, null), new PhysicalEvent(false, 2, 5, true, 7),
        new PhysicalEvent(false, 2, 5, true, 8)));
    assertCode("PHYSICAL_RL_RESET", () -> PhysicalTraceOracle.assertTrace(EXPECTED, actual));
  }

  @Test void catchesDlStructuralFault() {
    PhysicalTrace actual = trace(List.of(
        new PhysicalEvent(true, 0, 0, false, null), new PhysicalEvent(true, 0, 1, false, null),
        new PhysicalEvent(true, 0, 2, false, null), new PhysicalEvent(false, 1, 3, false, null),
        new PhysicalEvent(false, 1, 4, false, null), new PhysicalEvent(false, 2, 4, false, null),
        new PhysicalEvent(false, 1, 5, true, 8)));
    assertCode("PHYSICAL_DL_STRUCTURE", () -> PhysicalTraceOracle.assertTrace(EXPECTED, actual));
  }

  @Test void catchesPageHeaderFault() {
    PhysicalTrace actual = new PhysicalTrace(EXPECTED.leafPath(), 2, 5, List.of("V1"), EXPECTED.events());
    assertCode("PHYSICAL_PAGE_HEADER", () -> PhysicalTraceOracle.assertTrace(EXPECTED, actual));
  }

  @Test void catchesValuePresenceFault() {
    PhysicalTrace actual = trace(List.of(
        new PhysicalEvent(true, 0, 0, false, null), new PhysicalEvent(true, 0, 1, false, null),
        new PhysicalEvent(true, 0, 2, false, null), new PhysicalEvent(false, 1, 3, false, null),
        new PhysicalEvent(false, 1, 4, false, null), new PhysicalEvent(false, 2, 5, true, 7),
        new PhysicalEvent(false, 1, 5, false, null)));
    assertCode("PHYSICAL_VALUE_PRESENCE", () -> PhysicalTraceOracle.assertTrace(EXPECTED, actual));
  }

  private static PhysicalTrace trace(List<PhysicalEvent> events) { return new PhysicalTrace(EXPECTED.leafPath(), 2, 5, List.of("V2"), events); }
  private static void assertCode(String code, org.junit.jupiter.api.function.Executable executable) {
    assertEquals(code, assertThrows(ConformanceFailure.class, executable).code());
  }
}
