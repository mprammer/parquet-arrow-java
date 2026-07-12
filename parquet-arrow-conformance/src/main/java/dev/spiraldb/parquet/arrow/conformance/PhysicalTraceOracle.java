// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow.conformance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

/**
 * Page-level oracle.  This deliberately knows nothing about the product writer event tree or product
 * reader assemblers: it consumes parquet-java's raw page readers and level/value encodings itself.
 */
public final class PhysicalTraceOracle {
  private PhysicalTraceOracle() {}

  public static Map<String, PhysicalTrace> decode(InputFile input) throws IOException {
    // Open with our Hadoop-free codec factory + PlainParquetConfiguration so page decompression
    // never routes through parquet-java's Hadoop CodecFactory (which needs Configuration.getClassLoader
    // and real Hadoop codec classes). This mirrors the product reader.
    org.apache.parquet.ParquetReadOptions readOptions = org.apache.parquet.ParquetReadOptions
        .builder(new org.apache.parquet.conf.PlainParquetConfiguration())
        .withCodecFactory(new dev.spiraldb.parquet.arrow.CoreCompressionCodecFactory())
        .build();
    try (ParquetFileReader reader = ParquetFileReader.open(input, readOptions)) {
      Map<String, TraceBuilder> traces = new LinkedHashMap<>();
      PageReadStore rowGroup;
      while ((rowGroup = reader.readNextRowGroup()) != null) {
        for (ColumnDescriptor column : reader.getFooter().getFileMetaData().getSchema().getColumns()) {
          String path = String.join(".", column.getPath());
          TraceBuilder trace = traces.computeIfAbsent(path, ignored -> new TraceBuilder(path, column));
          PageReader pages = rowGroup.getPageReader(column);
          if (pages == null) continue;
          DictionaryPage dictionaryPage = pages.readDictionaryPage();
          Dictionary dictionary = dictionaryPage == null ? null : dictionaryPage.getEncoding().initDictionary(column, dictionaryPage);
          DataPage page;
          while ((page = pages.readPage()) != null) {
            if (page instanceof DataPageV1 v1) decodeV1(column, dictionary, v1, trace);
            else if (page instanceof DataPageV2 v2) decodeV2(column, dictionary, v2, trace);
            else throw new ConformanceFailure("PHYSICAL_PAGE", "unknown data page " + page.getClass().getName());
          }
        }
      }
      Map<String, PhysicalTrace> result = new LinkedHashMap<>();
      for (Map.Entry<String, TraceBuilder> e : traces.entrySet()) result.put(e.getKey(), e.getValue().finish());
      return Map.copyOf(result);
    }
  }

  public static void assertTrace(PhysicalTrace expected, PhysicalTrace actual) {
    validate(expected); validate(actual);
    if (!expected.leafPath().equals(actual.leafPath())) throw new ConformanceFailure("PHYSICAL_LEAF", expected.leafPath() + " != " + actual.leafPath());
    if (expected.maxRepetitionLevel() != actual.maxRepetitionLevel() || expected.maxDefinitionLevel() != actual.maxDefinitionLevel())
      throw new ConformanceFailure("PHYSICAL_LEVEL_MAX", "max levels differ for " + expected.leafPath());
    if (!expected.pageHeaders().equals(actual.pageHeaders())) throw new ConformanceFailure("PHYSICAL_PAGE_HEADER", "page header sequence differs for " + expected.leafPath());
    if (expected.events().size() != actual.events().size()) throw new ConformanceFailure("PHYSICAL_EVENT_COUNT", "event count differs for " + expected.leafPath());
    for (int i = 0; i < expected.events().size(); i++) if (!expected.events().get(i).equals(actual.events().get(i)))
      throw new ConformanceFailure(eventCode(expected.events().get(i), actual.events().get(i)), expected.leafPath() + " event " + i + ": expected " + expected.events().get(i) + ", got " + actual.events().get(i));
  }

  /** Checks invariants independently of a golden trace; useful when diagnosing a malformed producer. */
  public static void validate(PhysicalTrace trace) {
    for (int i = 0; i < trace.events().size(); i++) {
      PhysicalEvent event = trace.events().get(i);
      if (event.rowBoundary() != (event.repetitionLevel() == 0))
        throw new ConformanceFailure("PHYSICAL_RL_RESET", trace.leafPath() + " event " + i + " has inconsistent row boundary");
      if (event.valuePresent() != (event.definitionLevel() == trace.maxDefinitionLevel()))
        throw new ConformanceFailure("PHYSICAL_VALUE_PRESENCE", trace.leafPath() + " event " + i + " has inconsistent value presence");
    }
  }

  private static String eventCode(PhysicalEvent expected, PhysicalEvent actual) {
    if (expected.repetitionLevel() != actual.repetitionLevel() || expected.rowBoundary() != actual.rowBoundary()) return "PHYSICAL_RL_RESET";
    if (expected.definitionLevel() != actual.definitionLevel()) return "PHYSICAL_DL_STRUCTURE";
    if (expected.valuePresent() != actual.valuePresent()) return "PHYSICAL_VALUE_PRESENCE";
    return "PHYSICAL_VALUE";
  }

  private static void decodeV1(ColumnDescriptor column, Dictionary dictionary, DataPageV1 page, TraceBuilder trace) throws IOException {
    ByteBufferInputStream in = page.getBytes().toInputStream();
    int count = page.getValueCount();
    ValuesReader rl = levels(page.getRlEncoding(), column, ValuesType.REPETITION_LEVEL, count, in);
    ValuesReader dl = levels(page.getDlEncoding(), column, ValuesType.DEFINITION_LEVEL, count, in);
    ValuesReader values = values(page.getValueEncoding(), column, dictionary, count, in);
    trace.headers.add("V1");
    for (int i = 0; i < count; i++) {
      int repetition = column.getMaxRepetitionLevel() == 0 ? 0 : rl.readInteger();
      int definition = column.getMaxDefinitionLevel() == 0 ? 0 : dl.readInteger();
      boolean present = definition == column.getMaxDefinitionLevel();
      trace.add(repetition, definition, present ? read(column, values) : null);
    }
  }

  private static void decodeV2(ColumnDescriptor column, Dictionary dictionary, DataPageV2 page, TraceBuilder trace) throws IOException {
    int count = page.getValueCount();
    // DataPageV2 rep/def level sections are RLE WITHOUT the leading little-endian length prefix used
    // by v1 (the byte length lives in the page header), so decode them directly like parquet-java does.
    int[] rl = readV2Levels(column.getMaxRepetitionLevel(), page.getRepetitionLevels(), count);
    int[] dl = readV2Levels(column.getMaxDefinitionLevel(), page.getDefinitionLevels(), count);
    ValuesReader values = values(page.getDataEncoding(), column, dictionary, count, page.getData().toInputStream());
    trace.headers.add("V2");
    for (int i = 0; i < count; i++) {
      int definition = dl[i];
      boolean present = definition == column.getMaxDefinitionLevel();
      trace.add(rl[i], definition, present ? read(column, values) : null);
    }
  }

  private static int[] readV2Levels(int maxLevel, org.apache.parquet.bytes.BytesInput bytes, int count)
      throws IOException {
    int[] out = new int[count]; // maxLevel == 0 -> all zeros, no encoded bytes present
    if (maxLevel == 0) return out;
    int bitWidth = org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt(maxLevel);
    org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder decoder =
        new org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder(bitWidth, bytes.toInputStream());
    for (int i = 0; i < count; i++) out[i] = decoder.readInt();
    return out;
  }

  private static ValuesReader levels(Encoding encoding, ColumnDescriptor column, ValuesType kind, int count, ByteBufferInputStream input) throws IOException {
    ValuesReader reader = encoding.getValuesReader(column, kind); reader.initFromPage(count, input); return reader;
  }
  private static ValuesReader values(Encoding encoding, ColumnDescriptor column, Dictionary dictionary, int count, ByteBufferInputStream input) throws IOException {
    ValuesReader reader = encoding.usesDictionary() ? encoding.getDictionaryBasedValuesReader(column, ValuesType.VALUES, dictionary) : encoding.getValuesReader(column, ValuesType.VALUES);
    reader.initFromPage(count, input); return reader;
  }
  private static Object read(ColumnDescriptor column, ValuesReader reader) {
    switch (column.getPrimitiveType().getPrimitiveTypeName()) {
      case BOOLEAN: return reader.readBoolean();
      case INT32: return reader.readInteger();
      case INT64: return reader.readLong();
      case FLOAT: return reader.readFloat();
      case DOUBLE: return reader.readDouble();
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
      case INT96: return reader.readBytes().getBytes();
      default: throw new ConformanceFailure("PHYSICAL_TYPE", "unsupported primitive " + column.getPrimitiveType().getPrimitiveTypeName());
    }
  }

  private static final class TraceBuilder {
    final String path; final ColumnDescriptor column; final List<String> headers = new ArrayList<>(); final List<PhysicalEvent> events = new ArrayList<>();
    TraceBuilder(String path, ColumnDescriptor column) { this.path = path; this.column = column; }
    void add(int repetition, int definition, Object value) { events.add(new PhysicalEvent(repetition == 0, repetition, definition, value != null || definition == column.getMaxDefinitionLevel(), value)); }
    PhysicalTrace finish() { return new PhysicalTrace(path, column.getMaxRepetitionLevel(), column.getMaxDefinitionLevel(), headers, events); }
  }
}
