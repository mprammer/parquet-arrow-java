// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import org.apache.parquet.column.ParquetProperties;

/** The small immutable part of the v1 Parquet format-feature profile. */
public final class ParquetFormatProfile {
  private ParquetFormatProfile() {}
  public static ParquetProperties.WriterVersion writerVersion(DataPageVersion version) {
    if (version == null) throw new NullPointerException("version");
    return version == DataPageVersion.V1 ? ParquetProperties.WriterVersion.PARQUET_1_0 : ParquetProperties.WriterVersion.PARQUET_2_0;
  }
}
