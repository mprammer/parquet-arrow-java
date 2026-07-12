// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import java.io.IOException;

/** Base type for typed checked parquet-arrow failures. */
public class ParquetArrowException extends IOException { public ParquetArrowException(String m){super(m);} public ParquetArrowException(String m,Throwable c){super(m,c);} }
