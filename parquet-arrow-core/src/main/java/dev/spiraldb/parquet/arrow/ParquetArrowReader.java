// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

/** Base ArrowReader contract for parquet-arrow readers. */
public abstract class ParquetArrowReader extends ArrowReader { protected ParquetArrowReader(BufferAllocator allocator){super(allocator);} public abstract ReadEngineId engine(); public abstract ParquetFileInfo fileInfo(); public abstract long rowsRead(); public abstract long rowGroupsCompleted(); public abstract ReadMetrics metrics(); /** Returns -1 when the selected reader cannot truthfully count input bytes. */ @Override public abstract long bytesRead(); }
