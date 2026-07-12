// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import java.io.IOException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;

/** Pull-based Arrow batch source. */
public interface ArrowBatchStream extends AutoCloseable { Schema schema(); boolean loadNextBatch() throws IOException; VectorSchemaRoot getVectorSchemaRoot(); DictionaryProvider dictionaries(); @Override void close() throws IOException; }
