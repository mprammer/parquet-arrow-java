// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

/** Immutable writer configuration; builders preserve 1.x binary compatibility. */
public final class WriteOptions {
  private final Compression compression; private final DataPageVersion dataPageVersion;
  private final long targetRowGroupBytes; private final int maxRowGroupRows;
  private final int pageSizeBytes; private final int pageRowLimit; private final boolean dictionary;
  private final int dictionaryPageSizeBytes; private final boolean statistics; private final boolean checksums;
  private WriteOptions(Builder b) { compression=b.compression; dataPageVersion=b.dataPageVersion; targetRowGroupBytes=b.targetRowGroupBytes; maxRowGroupRows=b.maxRowGroupRows; pageSizeBytes=b.pageSizeBytes; pageRowLimit=b.pageRowLimit; dictionary=b.dictionary; dictionaryPageSizeBytes=b.dictionaryPageSizeBytes; statistics=b.statistics; checksums=b.checksums; }
  public static Builder builder() { return new Builder(); }
  public Compression compression() { return compression; } public DataPageVersion dataPageVersion() { return dataPageVersion; }
  public long targetRowGroupBytes() { return targetRowGroupBytes; } public int maxRowGroupRows() { return maxRowGroupRows; }
  public int pageSizeBytes() { return pageSizeBytes; } public int pageRowLimit() { return pageRowLimit; }
  public boolean parquetDictionaryEnabled() { return dictionary; } public int dictionaryPageSizeBytes() { return dictionaryPageSizeBytes; }
  public boolean statisticsEnabled() { return statistics; } public boolean pageChecksums() { return checksums; }
  public static final class Builder {
    private Compression compression=Compression.SNAPPY; private DataPageVersion dataPageVersion=DataPageVersion.V1;
    private long targetRowGroupBytes=128L*1024*1024; private int maxRowGroupRows=1_048_576; private int pageSizeBytes=1024*1024; private int pageRowLimit=20_000; private boolean dictionary=true; private int dictionaryPageSizeBytes=1024*1024; private boolean statistics=true; private boolean checksums=true;
    public Builder compression(Compression v) { compression=require(v); return this; } public Builder dataPageVersion(DataPageVersion v) { dataPageVersion=require(v); return this; }
    public Builder targetRowGroupBytes(long v) { if(v<=0) throw new IllegalArgumentException("targetRowGroupBytes"); targetRowGroupBytes=v; return this; } public Builder maxRowGroupRows(int v) { if(v<=0) throw new IllegalArgumentException("maxRowGroupRows"); maxRowGroupRows=v; return this; }
    public Builder pageSizeBytes(int v) { if(v<=0) throw new IllegalArgumentException("pageSizeBytes"); pageSizeBytes=v; return this; } public Builder pageRowLimit(int v) { if(v<=0) throw new IllegalArgumentException("pageRowLimit"); pageRowLimit=v; return this; }
    public Builder parquetDictionaryEnabled(boolean v) { dictionary=v; return this; } public Builder dictionaryPageSizeBytes(int v) { if(v<=0) throw new IllegalArgumentException("dictionaryPageSizeBytes"); dictionaryPageSizeBytes=v; return this; } public Builder statisticsEnabled(boolean v) { statistics=v; return this; } public Builder pageChecksums(boolean v) { checksums=v; return this; }
    public WriteOptions build() { return new WriteOptions(this); } private static <T>T require(T v){if(v==null)throw new NullPointerException();return v;}
  }
}
