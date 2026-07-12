// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow.conformance;

/** A deterministic conformance failure; the code is stable enough for fixtures and adapters. */
public final class ConformanceFailure extends IllegalArgumentException {
  private final String code;

  public ConformanceFailure(String code, String message) {
    super(code + ": " + message);
    this.code = code;
  }

  public String code() { return code; }
}
