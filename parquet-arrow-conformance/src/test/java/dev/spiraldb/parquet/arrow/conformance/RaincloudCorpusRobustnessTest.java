// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow.conformance;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import dev.spiraldb.parquet.arrow.ParquetArrow;
import dev.spiraldb.parquet.arrow.ParquetArrowException;
import dev.spiraldb.parquet.arrow.ParquetArrowReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

/** Opt-in real-file robustness gate; expected failures stay within the public typed contract. */
class RaincloudCorpusRobustnessTest {
  @Test void everyRaincloudFileSucceedsOrFailsWithTypedParquetArrowException() throws Exception {
    String configured = System.getenv("PA_RAINCLOUD_CORPUS");
    assumeTrue(configured != null && !configured.isBlank(), "PA_RAINCLOUD_CORPUS is not configured");
    Path corpus = Path.of(configured);
    assumeTrue(Files.isDirectory(corpus), "PA_RAINCLOUD_CORPUS is not a directory");
    try (RootAllocator allocator = new RootAllocator(); var paths = Files.walk(corpus)) {
      for (Path file : paths.filter(path -> path.toString().endsWith(".parquet")).toList()) {
        try (ParquetArrowReader reader = ParquetArrow.reader(allocator).build(file)) {
          while (reader.loadNextBatch()) { /* exercise every page/value decoder */ }
        } catch (ParquetArrowException expected) {
          // Unsupported foreign constructs are allowed only through the typed public contract.
        } catch (IOException unexpected) {
          fail("raw IOException for " + file, unexpected);
        } catch (RuntimeException unexpected) {
          fail("raw runtime exception for " + file, unexpected);
        }
      }
    }
  }
}
