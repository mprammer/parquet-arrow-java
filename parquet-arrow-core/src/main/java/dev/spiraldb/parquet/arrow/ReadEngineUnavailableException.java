// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

/** Typed unavailable-read-engine error. */
public final class ReadEngineUnavailableException extends ParquetArrowException { public ReadEngineUnavailableException(String message) { super(message); } public ReadEngineUnavailableException(String message, Throwable cause) { super(message, cause); } }
