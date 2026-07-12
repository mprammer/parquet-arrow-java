// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

/** Typed invalid-Parquet error. */
public final class InvalidParquetFileException extends ParquetArrowException { public InvalidParquetFileException(String message) { super(message); } public InvalidParquetFileException(String message, Throwable cause) { super(message, cause); } }
