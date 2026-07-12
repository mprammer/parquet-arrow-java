// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

/** Typed input-schema mismatch. */
public final class InputSchemaMismatchException extends ParquetArrowException { public InputSchemaMismatchException(String message) { super(message); } public InputSchemaMismatchException(String message, Throwable cause) { super(message, cause); } }
