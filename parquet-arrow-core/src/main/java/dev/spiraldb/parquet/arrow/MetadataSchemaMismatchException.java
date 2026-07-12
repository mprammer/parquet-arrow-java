// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

/** Typed metadata/schema mismatch. */
public final class MetadataSchemaMismatchException extends ParquetArrowException { public MetadataSchemaMismatchException(String message) { super(message); } public MetadataSchemaMismatchException(String message, Throwable cause) { super(message, cause); } }
