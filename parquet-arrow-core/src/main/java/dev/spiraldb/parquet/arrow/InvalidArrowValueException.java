// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

/** Typed Arrow value error. */
public final class InvalidArrowValueException extends ParquetArrowException { public InvalidArrowValueException(String message) { super(message); } public InvalidArrowValueException(String message, Throwable cause) { super(message, cause); } }
