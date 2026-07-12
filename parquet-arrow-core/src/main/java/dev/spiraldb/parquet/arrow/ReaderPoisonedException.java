// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

/** Typed reader-state error. */
public final class ReaderPoisonedException extends ParquetArrowException { public ReaderPoisonedException(String message) { super(message); } public ReaderPoisonedException(String message, Throwable cause) { super(message, cause); } }
