// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/**
 * Direct parquet-java reader.  The converter graph is compiled once for the requested schema and
 * writes Arrow vectors while parquet-java is decoding records; it never constructs Group values.
 */
final class CoreParquetArrowReader extends ParquetArrowReader {
  private enum State { NEW, READY, LOADING, EOF, FAILED, CLOSED }

  private final ReadOptions options;
  private final InputFile input;
  private final CoreCompressionCodecFactory codecs = new CoreCompressionCodecFactory();
  private Object file;
  private final MessageType fileSchema;
  private final MessageType requestedSchema;
  private final Schema schema;
  private final ParquetFileInfo info;
  private final MessageColumnIO columnIO;
  private final Rows materializer;
  private State state = State.NEW;
  private RecordReader<Void> recordReader;
  private long remainingInGroup;
  private long rows, batches, groups;
  private Throwable firstFailure;
  private boolean sourceClosed, readerClosed;

  CoreParquetArrowReader(BufferAllocator allocator, ReadOptions options, ColumnProjection projection,
      InputFile input) throws IOException {
    super(allocator);
    this.options = options; this.input = input;
    ParquetReadOptions read = ParquetReadOptions.builder(new PlainParquetConfiguration())
        .withCodecFactory(codecs).withPageChecksumVerification(options.verifyPageChecksums())
        // parquet-java reads the footer before this adapter sees its metadata map. Keep a cheap
        // allocation circuit breaker around that untrusted input boundary.
        .withMaxAllocationInBytes(64 * 1024 * 1024).build();
    try {
      file = open(input, read);
      ParquetMetadata metadata = (ParquetMetadata) call(file, "getFooter");
      FileMetaData footer = metadata.getFileMetaData();
      fileSchema = footer.getSchema();
      Schema resolved = resolveSchema(footer);
      Projection selected = project(resolved, fileSchema, projection);
      schema = selected.arrow;
      requestedSchema = selected.parquet;
      call(file, "setRequestedSchema", new Class<?>[] { MessageType.class }, requestedSchema);
      columnIO = new ColumnIOFactory(footer.getCreatedBy()).getColumnIO(requestedSchema, fileSchema);
      info = new ParquetFileInfo((Long) call(file, "getRecordCount"), footer.getCreatedBy(), schema,
          footer.getKeyValueMetaData());
      materializer = new Rows(schema, requestedSchema);
      state = State.READY;
    } catch (Throwable failure) {
      Throwable cleanup = null;
      if (file != null) try { call(file, "close"); } catch (Throwable close) { cleanup = close; }
      try { codecs.release(); } catch (Throwable release) { if (cleanup == null) cleanup = release; else cleanup.addSuppressed(release); }
      if (input instanceof ChannelInputFile) try { ((ChannelInputFile) input).close(); } catch (Throwable close) { if (cleanup == null) cleanup = close; else cleanup.addSuppressed(close); }
      if (cleanup != null) failure.addSuppressed(cleanup);
      failure = openFailure(failure);
      if (failure instanceof IOException) throw (IOException) failure;
      if (failure instanceof RuntimeException) throw (RuntimeException) failure;
      if (failure instanceof Error) throw (Error) failure;
      throw new IOException("construct parquet reader", failure);
    }
  }

  @Override protected Schema readSchema() { return schema; }
  @Override public ReadEngineId engine() { return ReadEngineId.PARQUET_JAVA; }
  @Override public ParquetFileInfo fileInfo() { return info; }
  @Override public long rowsRead() { return rows; }
  @Override public long rowGroupsCompleted() { return groups; }
  @Override public ReadMetrics metrics() { return new ReadMetrics(rows, batches, groups); }
  /** parquet-java does not expose a correct input-byte counter through this API. */
  @Override public long bytesRead() { return -1L; }

  @Override public boolean loadNextBatch() throws IOException {
    if (state == State.CLOSED) throw new IOException("reader is closed");
    if (state == State.FAILED) throw new ReaderPoisonedException("reader is poisoned", firstFailure);
    try { ensureInitialized(); }
    catch (OutOfMemoryException failure) { firstFailure = failure; state = State.FAILED; throw failure; }
    // This happens even after EOF: every call invalidates the borrowed previous batch.
    prepareLoadNextBatch();
    VectorSchemaRoot root = getVectorSchemaRoot();
    root.setRowCount(0);
    if (state == State.EOF) return false;
    state = State.LOADING;
    try {
      materializer.bind(root);
      int loaded = 0;
      boolean variableTargetReached = false;
      while (loaded < options.batchRows() && !variableTargetReached) {
        if (recordReader == null) {
          PageReadStore store = (PageReadStore) call(file, "readNextRowGroup");
          if (store == null) break;
          remainingInGroup = store.getRowCount();
          recordReader = columnIO.getRecordReader(store, materializer);
        }
        while (loaded < options.batchRows() && remainingInGroup > 0) {
          materializer.row(loaded);
          recordReader.read();
          loaded++; remainingInGroup--;
          // The bound is deliberately checked after a whole logical row. A single large or deeply
          // nested row is never split, but subsequent rows wait for the next reusable batch.
          if (liveVariableOutputBytes(root, loaded) >= options.maxVariableBytes()) { variableTargetReached = true; break; }
        }
        if (remainingInGroup == 0) { recordReader = null; groups++; }
      }
      root.setRowCount(loaded);
      if (loaded == 0) { state = State.EOF; return false; }
      rows += loaded; batches++; state = State.READY;
      return true;
    } catch (Throwable failure) {
      if (failure instanceof ConverterFailure) failure = failure.getCause();
      if (failure instanceof OutOfMemoryException) {
        poison(root, failure);
        throw (OutOfMemoryException) failure;
      }
      failure = pageFailure(failure);
      poison(root, failure);
      if (failure instanceof IOException) throw (IOException) failure;
      if (failure instanceof RuntimeException) throw (RuntimeException) failure;
      if (failure instanceof Error) throw (Error) failure;
      throw new IOException("parquet reader failure", failure);
    }
  }

  private void poison(VectorSchemaRoot root, Throwable failure) {
    if (firstFailure == null) firstFailure = failure;
    try { root.clear(); root.setRowCount(0); } catch (RuntimeException cleanup) { failure.addSuppressed(cleanup); }
    recordReader = null; remainingInGroup = 0; state = State.FAILED;
  }

  /** Bytes retained by variable-width or nested output vectors in the current borrowed batch. */
  private static long liveVariableOutputBytes(VectorSchemaRoot root, int count) {
    long total = 0;
    for (FieldVector vector : root.getFieldVectors()) {
      long bytes = variableOutputBytes(vector, count);
      if (Long.MAX_VALUE - total < bytes) return Long.MAX_VALUE;
      total += bytes;
    }
    return total;
  }
  private static long variableOutputBytes(FieldVector vector, int count) {
    switch (vector.getField().getType().getTypeID()) {
      case Utf8: case LargeUtf8: case Utf8View:
      case Binary: case LargeBinary: case BinaryView:
      case Struct: case List: case LargeList: case FixedSizeList: case Map:
        // getBufferSizeFor(count) reflects the bytes actually written for `count` rows; the vector's
        // own valueCount is not set until end-of-batch, so getBufferSize() would lag by a row.
        return vector.getBufferSizeFor(count);
      default: return 0;
    }
  }

  /**
   * parquet-java exposes a mixture of checked and unchecked decoding failures.  Neither is a
   * public part of this reader's contract: once a footer has been accepted, an unexpected
   * decoder failure denotes corrupt file contents and must not leak as an implementation crash.
   */
  private static Throwable pageFailure(Throwable failure) {
    if (failure instanceof ParquetArrowException) return failure;
    if (failure instanceof IOException) return new CorruptParquetException("cannot decode Parquet page", failure);
    if (failure instanceof RuntimeException) return new CorruptParquetException("invalid Parquet page encoding", failure);
    return failure;
  }

  /** Footer/open failures occur before any rows are observable and are file-structure failures. */
  private static Throwable openFailure(Throwable failure) {
    if (failure instanceof ParquetArrowException) return failure;
    if (failure instanceof IOException) return new InvalidParquetFileException("invalid Parquet file", failure);
    if (failure instanceof RuntimeException) return new InvalidParquetFileException("invalid Parquet file", failure);
    return failure;
  }
  /** parquet Converter callbacks cannot declare IOException; preserve typed corrupt-input causes. */
  private static final class ConverterFailure extends RuntimeException {
    ConverterFailure(IOException cause) { super(cause); }
  }

  @Override protected void closeReadSource() throws IOException {
    if (sourceClosed) return;
    sourceClosed = true; state = State.CLOSED;
    Throwable failure = null;
    if (file != null) try { call(file, "close"); } catch (Throwable close) { failure = close; }
    try { codecs.release(); } catch (Throwable release) { if (failure == null) failure = release; else failure.addSuppressed(release); }
    if (input instanceof ChannelInputFile) try { ((ChannelInputFile) input).close(); } catch (Throwable close) { if (failure == null) failure = close; else failure.addSuppressed(close); }
    if (failure != null) throw asIOException("reader source close", failure);
  }
  @Override public void close() throws IOException { closeIndependently(); }
  @Override public void close(boolean ignored) throws IOException { closeIndependently(); }
  private void closeIndependently() throws IOException {
    if (readerClosed) return;
    readerClosed = true;
    Throwable failure = null;
    // ArrowReader's close(boolean) intentionally skips closeReadSource when false. Do vector
    // cleanup first, but never let an allocator/vector failure suppress source cleanup.
    try { super.close(false); } catch (Throwable close) { failure = close; }
    try { closeReadSource(); } catch (Throwable close) { if (failure == null) failure = close; else failure.addSuppressed(close); }
    if (failure != null) throw asIOException("reader close", failure);
  }
  private static IOException asIOException(String action, Throwable failure) throws IOException {
    if (failure instanceof IOException) return (IOException) failure;
    if (failure instanceof RuntimeException) throw (RuntimeException) failure;
    if (failure instanceof Error) throw (Error) failure;
    return new IOException(action, failure);
  }

  private Schema resolveSchema(FileMetaData footer) throws IOException {
    Schema physical = ParquetToArrow.fromParquet(fileSchema);
    String encoded = footer.getKeyValueMetaData().get(ArrowSchemaMetadataCodec.KEY);
    if (encoded == null) return physical;
    try { return refine(physical, new ArrowSchemaMetadataCodec().decode(encoded)); }
    catch (IOException | RuntimeException ignored) { return physical; }
  }

  /**
   * ARROW:schema is a conventional, optional hint. Parquet supplies the value domain; this only
   * recovers Arrow representations that Parquet intentionally does not carry.
   */
  private static Schema refine(Schema physical, Schema hint) {
    if (physical.getFields().size() != hint.getFields().size()) return physical;
    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < physical.getFields().size(); i++) fields.add(refine(physical.getFields().get(i), hint.getFields().get(i)));
    return new Schema(fields, hint.getCustomMetadata());
  }
  private static Field refine(Field physical, Field hint) {
    if (!physical.getName().equals(hint.getName()) || physical.isNullable() != hint.isNullable()
        || physical.getChildren().size() != hint.getChildren().size()) return physical;
    List<Field> children = new ArrayList<>();
    for (int i = 0; i < physical.getChildren().size(); i++) children.add(refine(physical.getChildren().get(i), hint.getChildren().get(i)));
    ArrowType type = refinedType(physical.getType(), hint.getType());
    return new Field(physical.getName(), new org.apache.arrow.vector.types.pojo.FieldType(
        physical.isNullable(), type, null, hint.getMetadata()), children);
  }
  private static ArrowType refinedType(ArrowType physical, ArrowType hint) {
    if (physical.getTypeID() == ArrowType.ArrowTypeID.Utf8 && hint.getTypeID() == ArrowType.ArrowTypeID.LargeUtf8) return hint;
    if (physical.getTypeID() == ArrowType.ArrowTypeID.Binary && hint.getTypeID() == ArrowType.ArrowTypeID.LargeBinary) return hint;
    if (physical.getTypeID() == ArrowType.ArrowTypeID.Timestamp && hint.getTypeID() == ArrowType.ArrowTypeID.Timestamp) {
      ArrowType.Timestamp left = (ArrowType.Timestamp) physical, right = (ArrowType.Timestamp) hint;
      // Parquet exposes only the adjusted-to-UTC bit. A named zone is advisory only when the bit
      // already agrees; always keep the PHYSICAL unit (never request a narrowing conversion) but
      // adopt the hinted zone name. This also recovers the zone name after a SECOND->MILLIS
      // write-side promotion, where the hint's unit no longer matches the physical unit.
      if ((left.getTimezone() == null) == (right.getTimezone() == null)) return new ArrowType.Timestamp(left.getUnit(), right.getTimezone());
    }
    return physical;
  }

  private static final class Projection {
    final Schema arrow; final MessageType parquet;
    Projection(Schema arrow, MessageType parquet) { this.arrow = arrow; this.parquet = parquet; }
  }
  private static Projection project(Schema source, MessageType physical, ColumnProjection projection) throws IOException {
    List<Field> arrow = new ArrayList<>(); List<Type> parquet = new ArrayList<>();
    for (int i = 0; i < source.getFields().size(); i++) {
      Field f = source.getFields().get(i); Type t = physical.getType(i);
      List<FieldPath> paths = pathsFor(f.getName(), projection);
      if (!paths.isEmpty()) {
        SelectedField selected = projectField(f, t, paths, f.getName());
        arrow.add(selected.arrow); parquet.add(selected.parquet);
      }
    }
    if (!projection.isAll()) for (FieldPath path : projection.paths()) {
      boolean found = false;
      for (Field f : source.getFields()) if (f.getName().equals(path.segments().get(0))) { found = true; break; }
      if (!found) throw new InvalidParquetFileException("unknown projected field " + path);
    }
    return new Projection(new Schema(arrow, source.getCustomMetadata()), new MessageType(physical.getName(), parquet));
  }
  private static List<FieldPath> pathsFor(String name, ColumnProjection projection) {
    if (projection.isAll()) return List.of(FieldPath.of(name));
    List<FieldPath> result = new ArrayList<>();
    for (FieldPath path : projection.paths()) if (path.segments().get(0).equals(name)) result.add(path);
    return result;
  }
  private static final class SelectedField { final Field arrow; final Type parquet; SelectedField(Field arrow, Type parquet) { this.arrow = arrow; this.parquet = parquet; } }
  private static SelectedField projectField(Field field, Type type, List<FieldPath> paths, String path) throws IOException {
    for (FieldPath selected : paths) if (selected.segments().size() == 1) return new SelectedField(field, type);
    ArrowType arrowType = field.getType();
    // A LIST/MAP needs all physical leaves to retain offsets, ancestor validity, and entry boundaries.
    switch (arrowType.getTypeID()) {
      case List: case LargeList: case FixedSizeList: case Map:
        return new SelectedField(field, type);
      default: break;
    }
    if (arrowType.getTypeID() != ArrowType.Struct.INSTANCE.getTypeID() || type.isPrimitive())
      throw new InvalidParquetFileException("projection descends through non-struct field " + path);
    GroupType group = type.asGroupType(); List<Field> arrowChildren = new ArrayList<>(); List<Type> parquetChildren = new ArrayList<>();
    for (int i = 0; i < field.getChildren().size(); i++) {
      Field child = field.getChildren().get(i); List<FieldPath> childPaths = new ArrayList<>();
      for (FieldPath selected : paths) if (selected.segments().size() > 1 && selected.segments().get(1).equals(child.getName()))
        childPaths.add(FieldPath.of(selected.segments().subList(1, selected.segments().size()).toArray(new String[0])));
      if (!childPaths.isEmpty()) { SelectedField nested = projectField(child, group.getType(i), childPaths, path + "." + child.getName()); arrowChildren.add(nested.arrow); parquetChildren.add(nested.parquet); }
    }
    if (arrowChildren.isEmpty()) throw new InvalidParquetFileException("unknown projected field " + path);
    return new SelectedField(new Field(field.getName(), field.getFieldType(), arrowChildren), group.withNewFields(parquetChildren));
  }

  /** Per-record converter tree. Index ownership flows from a parent container to its children. */
  private static final class Rows extends RecordMaterializer<Void> {
    private final Schema arrow; private final MessageType parquet;
    private Node[] fields; private GroupConverter root;
    private int row;
    Rows(Schema arrow, MessageType parquet) { this.arrow = arrow; this.parquet = parquet; }
    void bind(VectorSchemaRoot vectorRoot) throws IOException {
      if (root != null) return;
      if (arrow.getFields().size() != parquet.getFieldCount() || vectorRoot.getFieldVectors().size() != arrow.getFields().size())
        throw new CorruptParquetException("projected Arrow/Parquet field count disagreement");
      fields = new Node[arrow.getFields().size()];
      Converter[] converters = new Converter[fields.length];
      for (int i = 0; i < fields.length; i++) {
        fields[i] = compile(arrow.getFields().get(i), parquet.getType(i), vectorRoot.getVector(i));
        converters[i] = fields[i].converter;
      }
      root = new GroupConverter() {
        @Override public Converter getConverter(int fieldIndex) { return converters[fieldIndex]; }
        @Override public void start() { for (Node field : fields) field.index(row); }
        @Override public void end() { }
      };
    }
    void row(int value) { row = value; }
    @Override public Void getCurrentRecord() { return null; }
    @Override public GroupConverter getRootConverter() { return root; }
  }

  private abstract static class Node {
    final Field field; final Type physical; final FieldVector vector; Converter converter; int index;
    Node(Field field, Type physical, FieldVector vector) { this.field = field; this.physical = physical; this.vector = vector; }
    void index(int value) { index = value; vector.setNull(value); }
  }
  private static Node compile(Field field, Type physical, FieldVector vector) throws IOException {
    ArrowType type = field.getType();
    switch (type.getTypeID()) {
      case Struct: return new StructNode(field, physical, vector);
      case List: return new ListNode(field, physical, vector);
      case LargeList: return new LargeListNode(field, physical, vector);
      case FixedSizeList: return new FixedListNode(field, physical, vector);
      case Map: return new MapNode(field, physical, vector);
      default: return new PrimitiveNode(field, physical, vector);
    }
  }
  private static final class StructNode extends Node {
    private final Node[] children;
    StructNode(Field field, Type physical, FieldVector vector) throws IOException {
      super(field, physical, vector);
      if (physical.isPrimitive() || !(vector instanceof StructVector)) throw new CorruptParquetException("struct vector/physical schema disagreement at " + field.getName());
      GroupType group = physical.asGroupType();
      if (field.getChildren().size() != group.getFieldCount()) throw new CorruptParquetException("struct child count disagreement at " + field.getName());
      StructVector struct = (StructVector) vector;
      children = new Node[group.getFieldCount()];
      for (int i = 0; i < children.length; i++) {
        Field child = field.getChildren().get(i); children[i] = compile(child, group.getType(i), (FieldVector) struct.getChild(child.getName()));
      }
      converter = new GroupConverter() {
        @Override public Converter getConverter(int i) { return children[i].converter; }
        @Override public void start() { ((StructVector) StructNode.this.vector).setIndexDefined(index); for (Node child : children) child.index(index); }
        @Override public void end() { }
      };
    }
  }
  private static final class ListNode extends Node {
    private final ListVector list; private final Node element; private int start, count;
    ListNode(Field field, Type physical, FieldVector vector) throws IOException {
      super(field, physical, vector);
      if (!(vector instanceof ListVector)) throw new CorruptParquetException("list vector disagreement at " + field.getName());
      ListEncodingResolver.Resolution resolution = new ListEncodingResolver().resolve(physical, field.getName());
      if (field.getChildren().size() != 1) throw new CorruptParquetException("list Arrow child count disagreement at " + field.getName());
      list = (ListVector) vector;
      element = compile(field.getChildren().get(0), resolution.element(), (FieldVector) list.getDataVector());
      GroupConverter repeated = new GroupConverter() {
        @Override public Converter getConverter(int i) { if (i != 0) throw new IndexOutOfBoundsException(); return element.converter; }
        @Override public void start() { element.index(start + count++); }
        @Override public void end() { }
      };
      converter = new GroupConverter() {
        @Override public Converter getConverter(int i) { if (i != 0) throw new IndexOutOfBoundsException(); return repeated; }
        @Override public void start() { start = list.startNewValue(index); count = 0; }
        @Override public void end() { list.endValue(index, count); }
      };
    }
  }
  /** A LargeList has the same physical LIST events as List, but Arrow offsets are 64-bit. */
  private static final class LargeListNode extends Node {
    private final LargeListVector list; private final Node element; private long start; private int count;
    LargeListNode(Field field, Type physical, FieldVector vector) throws IOException {
      super(field, physical, vector);
      if (!(vector instanceof LargeListVector)) throw new CorruptParquetException("large-list vector disagreement at " + field.getName());
      ListEncodingResolver.Resolution resolution = new ListEncodingResolver().resolve(physical, field.getName());
      if (field.getChildren().size() != 1) throw new CorruptParquetException("large-list Arrow child count disagreement at " + field.getName());
      list = (LargeListVector) vector;
      element = compile(field.getChildren().get(0), resolution.element(), (FieldVector) list.getDataVector());
      GroupConverter repeated = new GroupConverter() {
        @Override public Converter getConverter(int i) { if (i != 0) throw new IndexOutOfBoundsException(); return element.converter; }
        @Override public void start() { if (start + count > Integer.MAX_VALUE) throw new ConverterFailure(new CorruptParquetException("large-list child index exceeds Arrow capacity at " + field.getName())); element.index((int) (start + count++)); }
        @Override public void end() { }
      };
      converter = new GroupConverter() {
        @Override public Converter getConverter(int i) { if (i != 0) throw new IndexOutOfBoundsException(); return repeated; }
        @Override public void start() { start = list.startNewValue(index); count = 0; }
        @Override public void end() { list.endValue(index, count); }
      };
    }
  }
  /** FixedSizeList cardinality is Arrow-only semantics and is verified while materializing. */
  private static final class FixedListNode extends Node {
    private final FixedSizeListVector list; private final Node element; private final int width; private int count;
    FixedListNode(Field field, Type physical, FieldVector vector) throws IOException {
      super(field, physical, vector);
      if (!(vector instanceof FixedSizeListVector)) throw new CorruptParquetException("fixed-list vector disagreement at " + field.getName());
      ListEncodingResolver.Resolution resolution = new ListEncodingResolver().resolve(physical, field.getName());
      if (field.getChildren().size() != 1) throw new CorruptParquetException("fixed-list Arrow child count disagreement at " + field.getName());
      width = ((ArrowType.FixedSizeList) field.getType()).getListSize();
      if (width <= 0) throw new CorruptParquetException("invalid fixed-list width at " + field.getName());
      list = (FixedSizeListVector) vector;
      element = compile(field.getChildren().get(0), resolution.element(), (FieldVector) list.getDataVector());
      GroupConverter repeated = new GroupConverter() {
        @Override public Converter getConverter(int i) { if (i != 0) throw new IndexOutOfBoundsException(); return element.converter; }
        @Override public void start() {
          if (count == width) throw new ConverterFailure(new CorruptParquetException("fixed-list has more than " + width + " entries at " + field.getName()));
          element.index(Math.multiplyExact(index, width) + count++);
        }
        @Override public void end() { }
      };
      converter = new GroupConverter() {
        @Override public Converter getConverter(int i) { if (i != 0) throw new IndexOutOfBoundsException(); return repeated; }
        @Override public void start() { list.setNotNull(index); count = 0; }
        @Override public void end() {
          if (count != width) throw new ConverterFailure(new CorruptParquetException("fixed-list has " + count + " entries; expected " + width + " at " + field.getName()));
        }
      };
    }
  }
  private static final class MapNode extends Node {
    private final ListVector map; private final StructVector entries; private final Node key, value; private int start, count;
    MapNode(Field field, Type physical, FieldVector vector) throws IOException {
      super(field, physical, vector);
      if (!(vector instanceof ListVector)) throw new CorruptParquetException("map vector disagreement at " + field.getName());
      MapEncodingResolver.Resolution resolution = new MapEncodingResolver().resolve(physical, field.getName());
      if (field.getChildren().size() != 1 || field.getChildren().get(0).getChildren().size() != 2)
        throw new CorruptParquetException("map Arrow entry disagreement at " + field.getName());
      map = (ListVector) vector; entries = (StructVector) map.getDataVector();
      Field entry = field.getChildren().get(0); Field keyField = entry.getChildren().get(0), valueField = entry.getChildren().get(1);
      key = compile(keyField, resolution.key(), (FieldVector) entries.getChild(keyField.getName()));
      value = compile(valueField, resolution.value(), (FieldVector) entries.getChild(valueField.getName()));
      GroupConverter repeated = new GroupConverter() {
        @Override public Converter getConverter(int i) { return i == 0 ? key.converter : i == 1 ? value.converter : bad(i); }
        @Override public void start() { int entryIndex = start + count++; entries.setIndexDefined(entryIndex); key.index(entryIndex); value.index(entryIndex); }
        @Override public void end() { }
      };
      converter = new GroupConverter() {
        @Override public Converter getConverter(int i) { if (i != 0) throw new IndexOutOfBoundsException(); return repeated; }
        @Override public void start() { start = map.startNewValue(index); count = 0; }
        @Override public void end() { map.endValue(index, count); duplicateKeys(); }
      };
    }
    private Converter bad(int i) { throw new IndexOutOfBoundsException("map entry field " + i); }
    private void duplicateKeys() {
      Set<MapKeyEquality.Key> seen = new HashSet<>(); int start = map.getOffsetBuffer().getInt((long) index * 4);
      for (int i = 0; i < count; i++) try {
        if (!seen.add(MapKeyEquality.key(key.vector, start + i, field.getName())))
          throw new ConverterFailure(new CorruptParquetException("duplicate map key at " + field.getName()));
      } catch (InvalidArrowValueException invalid) { throw new ConverterFailure(new CorruptParquetException("invalid map key at " + field.getName(), invalid)); }
    }
  }
  private static final class PrimitiveNode extends Node {
    PrimitiveNode(Field field, Type physical, FieldVector vector) throws IOException {
      super(field, physical, vector);
      if (!physical.isPrimitive()) throw new CorruptParquetException("primitive Arrow field has group physical type at " + field.getName());
      converter = new PrimitiveConverter() {
        @Override public void addBoolean(boolean v) { put(vector, field.getType(), index, v); }
        @Override public void addInt(int v) { put(vector, field.getType(), index, v); }
        @Override public void addLong(long v) { put(vector, field.getType(), index, v); }
        @Override public void addFloat(float v) { put(vector, field.getType(), index, v); }
        @Override public void addDouble(double v) { put(vector, field.getType(), index, v); }
        @Override public void addBinary(Binary v) { put(vector, field.getType(), index, v.getBytes()); }
      };
    }
  }

  private static void put(FieldVector vector, ArrowType type, int row, Object value) {
    switch (type.getTypeID()) {
      case Null: throw new ConverterFailure(new InvalidArrowValueException("UNKNOWN/Null column contains a defined value"));
      case Bool: ((BitVector) vector).setSafe(row, (Boolean) value ? 1 : 0); return;
      case Int: putInt(vector, (ArrowType.Int) type, row, value); return;
      case FloatingPoint: if (((ArrowType.FloatingPoint) type).getPrecision() == FloatingPointPrecision.SINGLE) ((Float4Vector) vector).setSafe(row, (Float) value); else ((Float8Vector) vector).setSafe(row, (Double) value); return;
      case Utf8: case LargeUtf8: case Utf8View: putText(vector, row, (byte[]) value); return;
      case Binary: case LargeBinary: case BinaryView: putBytes(vector, row, (byte[]) value); return;
      case FixedSizeBinary: ((FixedSizeBinaryVector) vector).setSafe(row, (byte[]) value); return;
      case Decimal: putDecimal(vector, (ArrowType.Decimal) type, row, value); return;
      case Date: if (((ArrowType.Date) type).getUnit() == DateUnit.DAY) ((DateDayVector) vector).setSafe(row, (Integer) value); else ((DateMilliVector) vector).setSafe(row, Math.multiplyExact(((Integer) value).longValue(), 86_400_000L)); return;
      case Time: putTime(vector, (ArrowType.Time) type, row, value); return;
      case Timestamp: putTimestamp(vector, (ArrowType.Timestamp) type, row, (Long) value); return;
      case Duration: ((DurationVector) vector).setSafe(row, (Long) value); return;
      default: throw new UnsupportedOperationException("unsupported reader type " + type);
    }
  }
  private static void putInt(FieldVector v, ArrowType.Int t, int r, Object x) {
    if (t.getBitWidth() == 8) {
      int value = (Integer) x;
      if (t.getIsSigned()) { checked(value, Byte.MIN_VALUE, Byte.MAX_VALUE, "Int8"); ((TinyIntVector) v).setSafe(r, (byte) value); }
      else { checked(value, 0, 255, "UInt8"); ((UInt1Vector) v).setSafe(r, value); }
    } else if (t.getBitWidth() == 16) {
      int value = (Integer) x;
      if (t.getIsSigned()) { checked(value, Short.MIN_VALUE, Short.MAX_VALUE, "Int16"); ((SmallIntVector) v).setSafe(r, (short) value); }
      else { checked(value, 0, 65_535, "UInt16"); ((UInt2Vector) v).setSafe(r, value); }
    } else if (t.getBitWidth() == 32) {
      if (t.getIsSigned()) ((IntVector) v).setSafe(r, (Integer) x); else ((UInt4Vector) v).setSafe(r, (Integer) x);
    } else {
      if (t.getIsSigned()) ((BigIntVector) v).setSafe(r, (Long) x); else ((UInt8Vector) v).setSafe(r, (Long) x);
    }
  }
  private static void checked(long value, long minimum, long maximum, String type) {
    if (value < minimum || value > maximum)
      throw new ConverterFailure(new MetadataSchemaMismatchException(type + " value is out of range: " + value));
  }
  private static void putDecimal(FieldVector v, ArrowType.Decimal t, int r, Object x) { BigInteger u = x instanceof Integer ? BigInteger.valueOf((Integer) x) : x instanceof Long ? BigInteger.valueOf((Long) x) : new BigInteger((byte[]) x); BigDecimal d = new BigDecimal(u, t.getScale()); if (t.getBitWidth() == 128) ((DecimalVector) v).setSafe(r, d); else ((Decimal256Vector) v).setSafe(r, d); }
  private static void putText(FieldVector v, int r, byte[] x) { if (v instanceof VarCharVector) ((VarCharVector) v).setSafe(r, x); else if (v instanceof LargeVarCharVector) ((LargeVarCharVector) v).setSafe(r, x); else ((ViewVarCharVector) v).setSafe(r, x); }
  private static void putBytes(FieldVector v, int r, byte[] x) { if (v instanceof VarBinaryVector) ((VarBinaryVector) v).setSafe(r, x); else if (v instanceof LargeVarBinaryVector) ((LargeVarBinaryVector) v).setSafe(r, x); else ((ViewVarBinaryVector) v).setSafe(r, x); }
  private static void putTime(FieldVector v, ArrowType.Time t, int r, Object x) { long n=t.getBitWidth()==32?(Integer)x:(Long)x;long limit=t.getUnit()==TimeUnit.SECOND?86_399L:t.getUnit()==TimeUnit.MILLISECOND?86_399_999L:t.getUnit()==TimeUnit.MICROSECOND?86_399_999_999L:86_399_999_999_999L;if(t.getUnit()==TimeUnit.SECOND){if(n%1_000!=0||n/1_000<0||n/1_000>limit)throw new ConverterFailure(new MetadataSchemaMismatchException("Time(second) is not an exact in-range millisecond value"));n/=1_000;}else if(n<0||n>limit)throw new ConverterFailure(new MetadataSchemaMismatchException("TIME value is out of range"));if(t.getBitWidth()==32){if(t.getUnit()==TimeUnit.SECOND)((TimeSecVector)v).setSafe(r,(int)n);else((TimeMilliVector)v).setSafe(r,(int)n);}else if(t.getUnit()==TimeUnit.MICROSECOND)((TimeMicroVector)v).setSafe(r,n);else((TimeNanoVector)v).setSafe(r,n);}
  private static void putTimestamp(FieldVector v, ArrowType.Timestamp t, int r, long x) { if (t.getUnit() == TimeUnit.SECOND) {if(x%1_000!=0)throw new ConverterFailure(new MetadataSchemaMismatchException("timestamp(second) is not an exact millisecond value"));x/=1_000;} boolean z = t.getTimezone() != null; if (t.getUnit() == TimeUnit.SECOND) { if (z) ((TimeStampSecTZVector) v).setSafe(r, x); else ((TimeStampSecVector) v).setSafe(r, x); } else if (t.getUnit() == TimeUnit.MILLISECOND) { if (z) ((TimeStampMilliTZVector) v).setSafe(r, x); else ((TimeStampMilliVector) v).setSafe(r, x); } else if (t.getUnit() == TimeUnit.MICROSECOND) { if (z) ((TimeStampMicroTZVector) v).setSafe(r, x); else ((TimeStampMicroVector) v).setSafe(r, x); } else { if (z) ((TimeStampNanoTZVector) v).setSafe(r, x); else ((TimeStampNanoVector) v).setSafe(r, x); } }

  private static Object open(InputFile input, ParquetReadOptions options) throws IOException {
    try {
      Class<?> type = Class.forName("org.apache.parquet.hadoop.ParquetFileReader");
      return MethodHandles.publicLookup().findStatic(type, "open", MethodType.methodType(type, InputFile.class, ParquetReadOptions.class)).invoke(input, options);
    } catch (Throwable e) { throw failure("open parquet reader", e); }
  }
  private static Object call(Object target, String name) throws IOException { return call(target, name, new Class<?>[0]); }
  private static Object call(Object target, String name, Class<?>[] types, Object... args) throws IOException {
    try {
      Class<?> result;
      if (name.equals("getFooter")) result = ParquetMetadata.class;
      else if (name.equals("getRecordCount")) result = long.class;
      else if (name.equals("readNextRowGroup")) result = PageReadStore.class;
      else result = void.class;
      return MethodHandles.publicLookup().findVirtual(target.getClass(), name, MethodType.methodType(result, types)).invokeWithArguments(join(target, args));
    } catch (Throwable e) { throw failure(name, e); }
  }
  private static Object[] join(Object target, Object[] args) { Object[] all = new Object[args.length + 1]; all[0] = target; System.arraycopy(args, 0, all, 1, args.length); return all; }
  private static IOException failure(String action, Throwable e) { Throwable cause = e instanceof java.lang.reflect.InvocationTargetException ? ((java.lang.reflect.InvocationTargetException) e).getCause() : e; return cause instanceof IOException ? (IOException) cause : new IOException(action, cause); }

}
