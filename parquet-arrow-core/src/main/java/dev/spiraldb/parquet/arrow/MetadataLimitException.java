// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

/** Typed metadata-limit error. */
public final class MetadataLimitException extends ParquetArrowException { public MetadataLimitException(String message) { super(message); } public MetadataLimitException(String message, Throwable cause) { super(message, cause); } }
