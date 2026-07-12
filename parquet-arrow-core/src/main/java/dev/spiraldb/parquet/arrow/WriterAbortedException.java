// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

/** Typed writer-state error. */
public final class WriterAbortedException extends ParquetArrowException { public WriterAbortedException(String message) { super(message); } public WriterAbortedException(String message, Throwable cause) { super(message, cause); } }
