// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

/** Typed Parquet decoding error. */
public final class CorruptParquetException extends ParquetArrowException { public CorruptParquetException(String message) { super(message); } public CorruptParquetException(String message, Throwable cause) { super(message, cause); } }
