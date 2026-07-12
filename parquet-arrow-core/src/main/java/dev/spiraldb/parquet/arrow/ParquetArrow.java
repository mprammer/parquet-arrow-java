// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/** Entry point for the Arrow-native Parquet API. */
public final class ParquetArrow { private ParquetArrow(){} public static WriteBuilder writer(Schema schema){return new WriteBuilder(schema);} public static ReadBuilder reader(BufferAllocator allocator){return new ReadBuilder(allocator);} }
