// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

/** Immutable snapshot of writer counters and row-group buffering telemetry. */
public final class WriteMetrics { private final long batches, rows, rowGroups, bytes, buffered, peakBuffered, overshoot;
  public WriteMetrics(long batches,long rows,long rowGroups,long bytes){this(batches,rows,rowGroups,bytes,0,0,0);}
  public WriteMetrics(long batches,long rows,long rowGroups,long bytes,long buffered,long peakBuffered,long overshoot){this.batches=batches;this.rows=rows;this.rowGroups=rowGroups;this.bytes=bytes;this.buffered=buffered;this.peakBuffered=peakBuffered;this.overshoot=overshoot;}
  public long batchesAccepted(){return batches;} public long rowsAccepted(){return rows;} public long rowGroupsFinished(){return rowGroups;} public long bytesWritten(){return bytes;}
  public long currentBufferedBytes(){return buffered;} public long peakBufferedBytes(){return peakBuffered;} public long maximumBufferedOvershootBytes(){return overshoot;}
}
