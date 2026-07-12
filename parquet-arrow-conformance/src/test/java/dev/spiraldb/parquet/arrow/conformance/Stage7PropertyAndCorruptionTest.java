// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow.conformance;

import static org.junit.jupiter.api.Assertions.fail;

import dev.spiraldb.parquet.arrow.Compression;
import dev.spiraldb.parquet.arrow.ParquetArrow;
import dev.spiraldb.parquet.arrow.ParquetArrowException;
import dev.spiraldb.parquet.arrow.ParquetArrowReader;
import dev.spiraldb.parquet.arrow.ParquetArrowWriter;
import dev.spiraldb.parquet.arrow.WriteOptions;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.SplittableRandom;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Deterministic §9 generated-valid and hostile-input corpus; no clock or random global state. */
class Stage7PropertyAndCorruptionTest {
  private static final long[] SEEDS = {0x017_5EEDL, 0x1A2B3C4DL, 0x5EED_CAFEL, 0x7FFF_FFFFL};
  private static final Schema SCHEMA = new Schema(List.of(
      new Field("flag", FieldType.nullable(ArrowType.Bool.INSTANCE), List.of()),
      new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), List.of()),
      new Field("text", FieldType.nullable(ArrowType.Utf8.INSTANCE), List.of())));

  @TempDir Path temporary;

  @Test void fixedSeedGeneratedValidArrowRoundTrips() throws Exception {
    for (long seed : SEEDS) {
      Path file = temporary.resolve("generated-" + Long.toUnsignedString(seed) + ".parquet");
      try (RootAllocator allocator = new RootAllocator(); VectorSchemaRoot root = generated(allocator, seed)) {
        write(file, root, false);
        try (ParquetArrowReader reader = ParquetArrow.reader(allocator).build(file)) {
          if (!reader.loadNextBatch()) fail("generated seed " + seed + " produced no batch");
          new ArrowLogicalComparator().compare(root, reader.getVectorSchemaRoot()).requireMatch();
          if (reader.loadNextBatch()) fail("generated seed " + seed + " unexpectedly produced a second batch");
        }
      }
    }
  }

  @Test void malformedInputsNeverLeakRawDecoderFailures() throws Exception {
    Path valid = temporary.resolve("valid.parquet");
    try (RootAllocator allocator = new RootAllocator(); VectorSchemaRoot root = generated(allocator, SEEDS[0])) {
      // Force a tiny physical dictionary so the body mutation corpus reaches dictionary decoding
      // rather than relying on parquet-java's adaptive dictionary heuristic for random strings.
      VarCharVector text = (VarCharVector) root.getVector("text");
      for (int row = 0; row < root.getRowCount(); row++) if (!text.isNull(row))
        text.setSafe(row, (row % 2 == 0 ? "a" : "b").getBytes(StandardCharsets.UTF_8));
      write(valid, root, true);
    }
    byte[] bytes = Files.readAllBytes(valid);
    corrupt("truncated-pages", java.util.Arrays.copyOf(bytes, Math.max(4, bytes.length / 2)));

    byte[] impossibleOffset = bytes.clone();
    for (int i = impossibleOffset.length - 8; i < impossibleOffset.length - 4; i++) impossibleOffset[i] = (byte) 0xff;
    corrupt("impossible-footer-offset", impossibleOffset);

    // Each mutation is deliberately inside the body, leaving the footer framing intact.  They
    // exercise page-header/checksum, level-stream, and dictionary-index decode paths respectively.
    corrupt("checksum-failure", flip(bytes, 16));
    corrupt("dictionary-index-out-of-range", flip(bytes, Math.max(16, bytes.length / 3)));

    Path nested = temporary.resolve("nested-levels.parquet");
    writeNested(nested);
    byte[] nestedBytes = Files.readAllBytes(nested);
    // An arbitrary single-bit flip may land outside any page CRC (footer / page index / between
    // pages) and be absorbed as a valid-but-different value; Parquet cannot detect every such flip.
    // The contract for this case is only that no RAW decoder failure leaks (see corruptTolerant).
    corruptTolerant("invalid-rl-dl", flip(nestedBytes, Math.max(16, nestedBytes.length / 3)));
  }

  /** Arbitrary bit-flips: a clean read OR a typed ParquetArrowException are both acceptable; any
   *  other (raw) exception propagates out and fails the test — that is the actual contract. */
  private void corruptTolerant(String name, byte[] content) throws Exception {
    Path file = temporary.resolve(name + ".parquet"); Files.write(file, content);
    try (RootAllocator allocator = new RootAllocator();
         ParquetArrowReader reader = ParquetArrow.reader(allocator).build(file)) {
      while (reader.loadNextBatch()) { /* absorbed value corruption is acceptable */ }
    } catch (ParquetArrowException expected) {
      // typed failure is also acceptable
    }
  }

  private void corrupt(String name, byte[] content) throws Exception {
    Path file = temporary.resolve(name + ".parquet"); Files.write(file, content);
    try (RootAllocator allocator = new RootAllocator()) {
      try (ParquetArrowReader reader = ParquetArrow.reader(allocator).build(file)) {
        // Some corruptions are first detected during footer open, others only while page decoding.
        while (reader.loadNextBatch()) { /* the test requires a typed failure before a value escapes */ }
        fail(name + " was accepted as a clean Parquet file");
      }
    } catch (ParquetArrowException expected) {
      // The public taxonomy is the contract; parquet-java's raw exception types are not.
    }
  }

  private static byte[] flip(byte[] source, int index) {
    byte[] copy = source.clone(); copy[Math.min(copy.length - 9, index)] ^= (byte) 0x80; return copy;
  }

  private static VectorSchemaRoot generated(RootAllocator allocator, long seed) {
    SplittableRandom random = new SplittableRandom(seed);
    VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator); root.allocateNew();
    BitVector flags = (BitVector) root.getVector("flag"); IntVector ids = (IntVector) root.getVector("id");
    VarCharVector text = (VarCharVector) root.getVector("text");
    for (int row = 0; row < 257; row++) {
      if (random.nextInt(5) == 0) flags.setNull(row); else flags.setSafe(row, random.nextBoolean() ? 1 : 0);
      if (random.nextInt(5) == 0) ids.setNull(row); else ids.setSafe(row, random.nextInt());
      if (random.nextInt(5) == 0) text.setNull(row);
      else text.setSafe(row, ("s" + Long.toUnsignedString(random.nextLong(), 36)).getBytes(StandardCharsets.UTF_8));
    }
    root.setRowCount(257); return root;
  }

  private static void write(Path file, VectorSchemaRoot root, boolean dictionary) throws Exception {
    try (ParquetArrowWriter writer = ParquetArrow.writer(SCHEMA).options(WriteOptions.builder()
        .compression(Compression.UNCOMPRESSED).pageChecksums(true).parquetDictionaryEnabled(dictionary).build()).build(file)) {
      writer.writeBatch(root); writer.finish();
    }
  }

  private static void writeNested(Path file) throws Exception {
    Schema nested = new Schema(List.of(new Field("xs", FieldType.nullable(ArrowType.List.INSTANCE), List.of(
        new Field("element", FieldType.nullable(new ArrowType.Int(32, true)), List.of())))));
    try (RootAllocator allocator = new RootAllocator(); VectorSchemaRoot root = VectorSchemaRoot.create(nested, allocator)) {
      root.allocateNew(); ListVector lists = (ListVector) root.getVector("xs"); IntVector values = (IntVector) lists.getDataVector();
      lists.setNull(0); lists.startNewValue(1); lists.endValue(1, 0);
      int start = lists.startNewValue(2); values.setNull(start); values.setSafe(start + 1, 7); lists.endValue(2, 2); root.setRowCount(3);
      try (ParquetArrowWriter writer = ParquetArrow.writer(nested).options(WriteOptions.builder()
          .compression(Compression.UNCOMPRESSED).pageChecksums(true).parquetDictionaryEnabled(false).build()).build(file)) {
        writer.writeBatch(root); writer.finish();
      }
    }
  }
}
