// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

/** Typed unsupported-nested-encoding error. */
public final class UnsupportedNestedEncodingException extends ParquetArrowException { public UnsupportedNestedEncodingException(String message) { super(message); } public UnsupportedNestedEncodingException(String message, Throwable cause) { super(message, cause); } }
