// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

/** Immutable reader configuration. */
public final class ReadOptions {
  private final int batchRows; private final long maxVariableBytes; private final boolean verifyChecksums;
  private ReadOptions(Builder b){batchRows=b.batchRows;maxVariableBytes=b.maxVariableBytes;verifyChecksums=b.verifyChecksums;}
  public static Builder builder(){return new Builder();} public int batchRows(){return batchRows;} public long maxVariableBytes(){return maxVariableBytes;} public boolean verifyPageChecksums(){return verifyChecksums;}
  public static final class Builder { private int batchRows=8192; private long maxVariableBytes=64L*1024*1024; private boolean verifyChecksums=true;
    public Builder batchRows(int v){if(v<=0)throw new IllegalArgumentException("batchRows");batchRows=v;return this;} public Builder maxVariableBytes(long v){if(v<=0)throw new IllegalArgumentException("maxVariableBytes");maxVariableBytes=v;return this;} public Builder verifyPageChecksums(boolean v){verifyChecksums=v;return this;} public ReadOptions build(){return new ReadOptions(this);} }
}
