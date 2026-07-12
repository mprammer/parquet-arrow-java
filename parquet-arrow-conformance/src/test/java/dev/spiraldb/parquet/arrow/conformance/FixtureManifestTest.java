// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow.conformance;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

class FixtureManifestTest {
  @Test void manifestAndArrowFixtureAreReproducible() {
    FixtureManifest first = ArrowFixtureBuilder.manifest(), second = ArrowFixtureBuilder.manifest();
    assertEquals(first, second);
    assertEquals(first.sha256(), FixtureManifest.sha256(first.canonicalJson()));
    assertEquals(28, ArrowFixtureBuilder.supportedTypeSchema().getFields().size());
    try (RootAllocator allocator = new RootAllocator(); VectorSchemaRoot a = ArrowFixtureBuilder.primitiveBoundaries(allocator);
         VectorSchemaRoot b = ArrowFixtureBuilder.primitiveBoundaries(allocator)) {
      assertTrue(new ArrowLogicalComparator().compare(a, b).matches());
    }
  }
}
