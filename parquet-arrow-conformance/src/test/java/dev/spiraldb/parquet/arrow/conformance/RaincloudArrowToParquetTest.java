// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow.conformance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import dev.spiraldb.parquet.arrow.ParquetArrow;
import dev.spiraldb.parquet.arrow.ParquetArrowReader;
import dev.spiraldb.parquet.arrow.ParquetArrowWriter;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/**
 * The production raincloud path: a compressed Arrow IPC file becomes Arrow memory, standard
 * Parquet, and plain Arrow values again. It is intentionally opt-in because the corpus is local.
 */
class RaincloudArrowToParquetTest {
  private static final String DUPLICATE_NAME_DATASET = "uci-spambase.arrow.zstd";
  private static final String DUPLICATE_NAME_REASON = "char_freq: duplicate field name cannot round-trip through Parquet";

  // Pinned corpus membership: the gate must exercise exactly these files, not "at least one". A
  // missing or unexpected basename fails the run so the corpus cannot silently shrink to a single
  // green file. `uci-spambase` is the one documented duplicate-name rejection; the rest round-trip.
  private static final java.util.Set<String> EXPECTED_CORPUS = java.util.Set.of(
      "120-years-of-olympic-history-athletes-and-results.arrow.zstd", "ai2-arc.arrow.zstd",
      "amazon-reviews-2023-subscription-boxes.arrow.zstd", "anthropic-interviewer.arrow.zstd",
      "countries-of-the-world.arrow.zstd", "finepdfs-en-test.arrow.zstd", "frames-benchmark.arrow.zstd",
      "google-cluster-trace-2011-machine-events.arrow.zstd", "helpsteer2.arrow.zstd", "humaneval.arrow.zstd",
      "kepler-exoplanet-search-results.arrow.zstd", "mbpp.arrow.zstd", "mnist.arrow.zstd",
      "pubmedqa-labeled.arrow.zstd", "truthfulqa-mc.arrow.zstd", "uci-bike-sharing-dataset.arrow.zstd",
      "uci-diabetes.arrow.zstd", "uci-iris.arrow.zstd", "uci-seeds.arrow.zstd", DUPLICATE_NAME_DATASET,
      "uci-wine-quality.arrow.zstd", "uci-wine.arrow.zstd", "wdi.arrow.zstd");

  @Test void translatesEverySerializedArrowFile() throws Exception {
    String configured = System.getenv("PA_ARROW_CORPUS");
    assumeTrue(configured != null && !configured.isBlank(), "PA_ARROW_CORPUS is not configured");
    Path corpus = Path.of(configured);
    assumeTrue(Files.isDirectory(corpus), "PA_ARROW_CORPUS is not a directory");
    java.util.List<Path> arrowFiles;
    try (var paths = Files.walk(corpus)) {
      arrowFiles = paths.filter(path -> path.getFileName().toString().endsWith(".arrow.zstd")).toList();
    }
    java.util.Set<String> found = arrowFiles.stream().map(p -> p.getFileName().toString()).collect(java.util.stream.Collectors.toCollection(java.util.TreeSet::new));
    assertEquals(EXPECTED_CORPUS, found, "corpus membership drifted from the pinned set (missing or unexpected files)");
    int roundTripped = 0;
    int allowlistedRejections = 0;
    for (Path arrowFile : arrowFiles) {
      try {
        translate(arrowFile);
        roundTripped++;
      } catch (dev.spiraldb.parquet.arrow.UnsupportedParquetTypeException typed) {
        if (!arrowFile.getFileName().toString().equals(DUPLICATE_NAME_DATASET)
            || !typed.getMessage().equals(DUPLICATE_NAME_REASON)) throw typed;
        allowlistedRejections++;
      }
    }
    System.out.println("[raincloud corpus] round-tripped=" + roundTripped + " duplicate-name-rejections=" + allowlistedRejections);
    assertEquals(EXPECTED_CORPUS.size() - 1, roundTripped, "every non-rejected corpus file must round-trip");
    assertTrue(allowlistedRejections == 1, "the corpus must contain exactly the documented duplicate-name rejection");
  }

  private static void translate(Path arrowFile) throws Exception {
    Path parquet = Files.createTempFile("raincloud-arrow-", ".parquet");
    Files.delete(parquet); // writer is create-new by default; start from a non-existent path
    try (RootAllocator allocator = new RootAllocator()) {
      Schema expectedSchema;
      try (SeekableByteChannel channel = Files.newByteChannel(arrowFile);
           ArrowFileReader input = new ArrowFileReader(channel, allocator, CommonsCompressionFactory.INSTANCE)) {
        expectedSchema = valueSchema(input.getVectorSchemaRoot().getSchema(), input);
        try (ParquetArrowWriter writer = ParquetArrow.writer(input.getVectorSchemaRoot().getSchema()).build(parquet)) {
          writer.writeAll(input);
        }
      }

      try (SeekableByteChannel channel = Files.newByteChannel(arrowFile);
           ArrowFileReader expected = new ArrowFileReader(channel, allocator, CommonsCompressionFactory.INSTANCE);
           ParquetArrowReader actual = ParquetArrow.reader(allocator).build(parquet)) {
        var schemaDiff = new ArrowLogicalComparator().compareSchemas(expectedSchema, actual.getVectorSchemaRoot().getSchema());
        assertTrue(schemaDiff.matches(), "schema for " + arrowFile + ": " + schemaDiff.differences());
        compareAllRows(expected, actual, arrowFile);
      }
    } finally {
      Files.deleteIfExists(parquet);
    }
  }

  /** Removes Arrow dictionary identity from the expected schema, matching the public contract. */
  private static Schema valueSchema(Schema schema, org.apache.arrow.vector.dictionary.DictionaryProvider dictionaries) {
    return new Schema(schema.getFields().stream().map(field -> valueField(field, dictionaries)).toList(), schema.getCustomMetadata());
  }
  private static Field valueField(Field field, org.apache.arrow.vector.dictionary.DictionaryProvider dictionaries) {
    Field source = field;
    if (field.getDictionary() != null) {
      Dictionary dictionary = dictionaries.lookup(field.getDictionary().getId());
      if (dictionary != null) source = dictionary.getVector().getField();
    }
    List<Field> children = source.getChildren().stream().map(child -> valueField(child, dictionaries)).toList();
    return new Field(field.getName(), new FieldType(field.isNullable(), source.getType(), null, field.getMetadata()), children);
  }

  /**
   * Compares the two independently-batched streams one GLOBAL row at a time via per-side cursors that
   * cross batch boundaries transparently. Advancing both each iteration guarantees row i is compared
   * with row i regardless of differing batch sizes (the previous in-place pairing could drift).
   */
  private static void compareAllRows(ArrowFileReader expected, ParquetArrowReader actual, Path arrowFile) throws Exception {
    ArrowLogicalComparator comparator = new ArrowLogicalComparator();
    long row = 0;
    try (ExpectedCursor exp = new ExpectedCursor(expected); ActualCursor act = new ActualCursor(actual)) {
      while (true) {
        boolean e = exp.advance(), a = act.advance();
        if (!e && !a) break;
        assertTrue(e && a, "row count differs for " + arrowFile + " at row " + row);
        comparator.compareRow(exp.root(), exp.index(), act.root(), act.index()).requireMatch();
        row++;
      }
    }
  }

  /** A row cursor that hides batch boundaries: advance() yields the next global row or false at EOF. */
  private abstract static class RowCursor implements AutoCloseable {
    VectorSchemaRoot current; int index = -1;
    final boolean advance() throws Exception {
      if (current != null) { index++; if (index < current.getRowCount()) return true; release(); current = null; }
      while (loadNext()) { index = 0; if (current.getRowCount() > 0) return true; release(); current = null; }
      return false;
    }
    VectorSchemaRoot root() { return current; }
    int index() { return index; }
    abstract boolean loadNext() throws Exception; // sets `current` to the next batch's comparable VSR
    void release() {}
    @Override public void close() { if (current != null) release(); }
  }
  private static final class ExpectedCursor extends RowCursor {
    private final ArrowFileReader reader; private List<FieldVector> decodedVectors = List.of(); private VectorSchemaRoot sourceRoot;
    ExpectedCursor(ArrowFileReader reader) { this.reader = reader; }
    @Override boolean loadNext() throws Exception {
      if (!reader.loadNextBatch()) return false;
      sourceRoot = reader.getVectorSchemaRoot();
      decodedVectors = decoded(sourceRoot, reader);
      current = new VectorSchemaRoot(decodedVectors);
      current.setRowCount(sourceRoot.getRowCount());
      return true;
    }
    @Override void release() { if (sourceRoot != null) closeDecoded(decodedVectors, sourceRoot); }
  }
  private static final class ActualCursor extends RowCursor {
    private final ParquetArrowReader reader;
    ActualCursor(ParquetArrowReader reader) { this.reader = reader; }
    @Override boolean loadNext() throws Exception { if (!reader.loadNextBatch()) return false; current = reader.getVectorSchemaRoot(); return true; }
  }
  private static List<FieldVector> decoded(VectorSchemaRoot source, org.apache.arrow.vector.dictionary.DictionaryProvider dictionaries) {
    return source.getFieldVectors().stream().map(vector -> decoded(vector, dictionaries)).toList();
  }
  private static void closeDecoded(List<FieldVector> decoded, VectorSchemaRoot source) { for (int i = 0; i < decoded.size(); i++) if (decoded.get(i) != source.getVector(i)) decoded.get(i).close(); }
  private static FieldVector decoded(FieldVector source, org.apache.arrow.vector.dictionary.DictionaryProvider dictionaries) {
    if (source.getField().getDictionary() == null) return source;
    Dictionary dictionary = dictionaries.lookup(source.getField().getDictionary().getId());
    if (dictionary == null) throw new AssertionError("missing source dictionary for " + source.getName());
    FieldVector values = (FieldVector) DictionaryEncoder.decode(source, dictionary, source.getAllocator());
    // decode names the result after the dictionary vector ("DICT0"); restore the original field name
    // so the values view matches the public contract (dictionary -> values under the same name).
    if (values.getField().getName().equals(source.getName())) return values;
    org.apache.arrow.vector.util.TransferPair tp = values.getTransferPair(source.getName(), source.getAllocator());
    tp.transfer();
    values.close();
    return (FieldVector) tp.getTo();
  }
}
