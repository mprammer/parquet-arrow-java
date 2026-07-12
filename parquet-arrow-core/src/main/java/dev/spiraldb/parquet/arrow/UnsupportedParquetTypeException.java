// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

/** Typed unsupported-type error. */
public final class UnsupportedParquetTypeException extends ParquetArrowException { public UnsupportedParquetTypeException(String message) { super(message); } public UnsupportedParquetTypeException(String message, Throwable cause) { super(message, cause); } }
