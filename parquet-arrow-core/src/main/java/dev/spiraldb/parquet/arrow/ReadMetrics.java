// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

/** Immutable snapshot of reader progress counters. Input bytes are intentionally unavailable. */
public final class ReadMetrics { private final long rows,batches,rowGroups; public ReadMetrics(long rows,long batches,long rowGroups){this.rows=rows;this.batches=batches;this.rowGroups=rowGroups;} public long rowsReturned(){return rows;} public long batchesReturned(){return batches;} public long rowGroupsCompleted(){return rowGroups;} }
