// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import java.util.Collections;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.Schema;

/** Immutable footer tuple containing only fields this reader can truthfully provide. */
public final class ParquetFileInfo {
  private final long rowCount; private final String createdBy; private final Schema schema; private final Map<String,String> metadata;
  public ParquetFileInfo(long rowCount, String createdBy, Schema schema, Map<String,String> metadata) { this.rowCount=rowCount; this.createdBy=createdBy; this.schema=schema; this.metadata=Collections.unmodifiableMap(Map.copyOf(metadata)); }
  public long rowCount(){return rowCount;} public String createdBy(){return createdBy;} public Schema schema(){return schema;} public Map<String,String> metadata(){return metadata;}
}
