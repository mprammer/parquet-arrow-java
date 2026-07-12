// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

/** Typed unavailable-codec error. */
public final class CodecUnavailableException extends ParquetArrowException { public CodecUnavailableException(String message) { super(message); } public CodecUnavailableException(String message, Throwable cause) { super(message, cause); } }
