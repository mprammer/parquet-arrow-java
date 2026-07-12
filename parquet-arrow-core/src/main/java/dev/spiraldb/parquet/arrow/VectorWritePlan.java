// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.math.BigInteger;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

/**
 * Arrow-indexed Arrow-to-Parquet event plan.
 *
 * <p>The plan is deliberately compiled once per writer.  Its hot path reads Arrow validity,
 * offset and value buffers directly; it never calls {@code getObject()}, constructs a Group, or
 * boxes a scalar value.  The container nodes deliberately emit only the Parquet event tree:
 * parquet-java derives repetition/definition levels from that tree.
 */
final class VectorWritePlan {
  private static final long MILLIS_PER_DAY = 86_400_000L;
  /** Schema supplied to the writer.  It retains Arrow's index-vector representation. */
  private final Schema inputSchema;
  private final InputSchemaCompatibility compatibility;
  private final Node[] fields;

  static VectorWritePlan compile(Schema inputSchema, Schema logicalSchema, InputSchemaCompatibility compatibility) throws IOException {
    List<Field> fields = logicalSchema.getFields();
    if (inputSchema.getFields().size() != fields.size())
      throw new InputSchemaMismatchException("dictionary value schema does not match input field count");
    Node[] writers = new Node[fields.size()];
    for (int i = 0; i < writers.length; i++)
      writers[i] = Node.compile(fields.get(i), inputSchema.getFields().get(i), i, fields.get(i).getName());
    return new VectorWritePlan(inputSchema, compatibility, writers);
  }
  static VectorWritePlan compile(Schema schema, InputSchemaCompatibility compatibility) throws IOException { return compile(schema, schema, compatibility); }
  static VectorWritePlan compile(Schema schema) throws IOException { return compile(schema, schema, InputSchemaCompatibility.COMPATIBLE); }

  private VectorWritePlan(Schema inputSchema, InputSchemaCompatibility compatibility, Node[] fields) { this.inputSchema = inputSchema; this.compatibility = compatibility; this.fields = fields; }

  boolean hasUnsignedLong() {
    for (Node field : fields) if (field.hasUnsignedLong()) return true;
    return false;
  }

  /** PRECHECK phase.  It performs every validation that can happen before a record event. */
  void preflight(VectorSchemaRoot root) throws IOException { preflight(root, null); }
  void preflight(VectorSchemaRoot root, DictionaryProvider dictionaries) throws IOException {
    if (compatibility == InputSchemaCompatibility.STRICT && !inputSchema.equals(root.getSchema()))
      throw new InputSchemaMismatchException("input schema does not exactly match writer schema");
    if (compatibility == InputSchemaCompatibility.COMPATIBLE) compatible(inputSchema.getFields(), root.getSchema().getFields(), root);
    for (int i = 0; i < fields.length; i++) {
      FieldVector vector = root.getVector(i);
      if (vector == null) throw new InputSchemaMismatchException("missing vector for " + fields[i].field.getName());
      fields[i].prepare(dictionaries);
      for (int row = 0; row < root.getRowCount(); row++) fields[i].validate(vector, row);
    }
  }

  private static void compatible(List<Field> expected, List<Field> actual, VectorSchemaRoot root) throws InputSchemaMismatchException {
    if (expected.size() != actual.size()) throw new InputSchemaMismatchException("input field count does not match writer schema");
    for (int i = 0; i < expected.size(); i++) compatible(expected.get(i), actual.get(i), root.getVector(i), expected.get(i).getName());
  }
  private static void compatible(Field expected, Field actual, FieldVector vector, String path) throws InputSchemaMismatchException {
    if (!expected.getName().equals(actual.getName()) || !expected.getType().equals(actual.getType())
        || !java.util.Objects.equals(expected.getDictionary(), actual.getDictionary())
        || expected.getChildren().size() != actual.getChildren().size())
      throw new InputSchemaMismatchException("input field does not match writer schema at " + path);
    // Requiredness (a null in a required field) is NOT scanned here: this flat pass would visit
    // child slots under an absent parent container, which are unreachable and carry no logical
    // value. Requiredness is enforced per row, container-aware, by Node.validate() -> required(),
    // which descends only into present parents. compatible() stays a pure schema-shape check.
    for (int i = 0; i < expected.getChildren().size(); i++) {
      FieldVector child = child(vector, expected.getType(), expected.getChildren().get(i).getName());
      compatible(expected.getChildren().get(i), actual.getChildren().get(i), child, path + "." + expected.getChildren().get(i).getName());
    }
  }
  private static FieldVector child(FieldVector vector, ArrowType type, String name) {
    if (vector instanceof StructVector) return (FieldVector) ((StructVector) vector).getChild(name);
    if (vector instanceof MapVector) return (FieldVector) ((StructVector) ((MapVector) vector).getDataVector()).getChild(name);
    if (vector instanceof ListVector) return ((ListVector) vector).getDataVector();
    if (vector instanceof LargeListVector) return ((LargeListVector) vector).getDataVector();
    if (vector instanceof FixedSizeListVector) return ((FixedSizeListVector) vector).getDataVector();
    return null;
  }

  /** Emits one already preflighted top-level row. */
  void writeRow(VectorSchemaRoot root, int row, RecordConsumer records) throws IOException {
    for (int i = 0; i < fields.length; i++) {
      Node field = fields[i];
      FieldVector vector = root.getVector(i);
      if (vector.isNull(row)) continue; // optional absence is represented by omitting the field.
      records.startField(field.field.getName(), field.index);
      field.write(vector, row, records);
      records.endField(field.field.getName(), field.index);
    }
  }

  /** A node owns Arrow indices; it never owns Dremel levels. */
  private abstract static class Node {
    final Field field; final int index; final String path;
    Node(Field field, int index, String path) { this.field = field; this.index = index; this.path = path; }
    abstract void validate(FieldVector vector, int value) throws IOException;
    abstract void write(FieldVector vector, int value, RecordConsumer out) throws IOException;
    void prepare(DictionaryProvider dictionaries) throws IOException { }
    boolean hasUnsignedLong() { return false; }

    static Node compile(Field field, Field inputField, int index, String path) throws IOException {
      if (inputField.getDictionary() != null) return new DictionaryNode(field, inputField, index, path);
      switch (field.getType().getTypeID()) {
        case Struct: return new StructNode(field, inputField, index, path);
        case List: case LargeList: return new ListNode(field, inputField, index, path);
        case FixedSizeList: return new FixedListNode(field, inputField, index, path);
        case Map: return new MapNode(field, inputField, index, path);
        default: return new PrimitiveNode(field, index, path, Leaf.compile(field, index));
      }
    }

    final void required(FieldVector vector, int value) throws InvalidArrowValueException {
      if (!field.isNullable() && vector.isNull(value)) throw invalid(value, "required field is null");
    }
    final void emit(Field child, int childIndex, Node writer, FieldVector vector, int value, RecordConsumer out) throws IOException {
      if (vector.isNull(value)) return;
      out.startField(child.getName(), childIndex); writer.write(vector, value, out); out.endField(child.getName(), childIndex);
    }
    final InvalidArrowValueException invalid(int value, String message) { return new InvalidArrowValueException(path + " row/index " + value + ": " + message); }
  }

  /** Resolves Arrow dictionary indices to ordinary values before the Parquet event plan sees them. */
  private static final class DictionaryNode extends Node {
    private final Node values;
    private final long dictionaryId;
    private FieldVector dictionary;
    DictionaryNode(Field field, Field inputField, int index, String path) throws IOException {
      super(field, index, path);
      dictionaryId = inputField.getDictionary().getId();
      values = Node.compile(field, ArrowSchemaToParquet.valueField(field), index, path);
    }
    @Override void prepare(DictionaryProvider dictionaries) throws IOException {
      if (dictionaries == null) throw new InvalidArrowValueException(path + ": dictionary provider is required");
      Dictionary resolved = dictionaries.lookup(dictionaryId);
      if (resolved == null || !(resolved.getVector() instanceof FieldVector))
        throw new InvalidArrowValueException(path + ": missing dictionary " + dictionaryId);
      dictionary = (FieldVector) resolved.getVector();
      if (!sameValueField(field, dictionary.getField()))
        throw new InvalidArrowValueException(path + ": dictionary value type does not match the field value type");
      values.prepare(dictionaries);
    }
    @Override void validate(FieldVector vector, int value) throws IOException {
      required(vector, value); if (vector.isNull(value)) return;
      int dictionaryIndex = dictionaryIndex(vector, value, path);
      if (dictionaryIndex >= dictionary.getValueCount()) throw invalid(value, "dictionary index out of range");
      values.validate(dictionary, dictionaryIndex);
    }
    @Override void write(FieldVector vector, int value, RecordConsumer out) throws IOException {
      values.write(dictionary, dictionaryIndex(vector, value, path), out);
    }
    @Override boolean hasUnsignedLong() { return values.hasUnsignedLong(); }
  }

  private static boolean sameValueField(Field expected, Field actual) {
    Field plainExpected = ArrowSchemaToParquet.valueField(expected);
    Field plainActual = ArrowSchemaToParquet.valueField(actual);
    return plainExpected.getType().equals(plainActual.getType())
        && plainExpected.getChildren().equals(plainActual.getChildren());
  }

  private static final class PrimitiveNode extends Node {
    private final Leaf leaf;
    PrimitiveNode(Field field, int index, String path, Leaf leaf) { super(field, index, path); this.leaf = leaf; }
    @Override void validate(FieldVector vector, int value) throws IOException { required(vector, value); if (!vector.isNull(value)) leaf.validateValue(vector, value); }
    @Override void write(FieldVector vector, int value, RecordConsumer out) throws IOException { leaf.write(vector, value, out); }
    @Override boolean hasUnsignedLong() { return leaf.kind == Kind.UINT64; }
  }

  private static final class StructNode extends Node {
    private final Node[] children;
    StructNode(Field field, Field inputField, int index, String path) throws IOException {
      super(field, index, path); children = new Node[field.getChildren().size()];
      if (children.length == 0) throw new UnsupportedParquetTypeException(path + ": empty struct has no physical descendant");
      if (inputField.getChildren().size() != children.length) throw new InputSchemaMismatchException("input field does not match writer schema at " + path);
      for (int i = 0; i < children.length; i++) children[i] = Node.compile(field.getChildren().get(i), inputField.getChildren().get(i), i, path + "." + field.getChildren().get(i).getName());
    }
    @Override void validate(FieldVector vector, int value) throws IOException {
      required(vector, value); if (vector.isNull(value)) return;
      if (!(vector instanceof StructVector)) throw invalid(value, "expected StructVector");
      StructVector struct = (StructVector) vector;
      for (int i = 0; i < children.length; i++) { FieldVector child = (FieldVector) struct.getChild(children[i].field.getName()); if (child == null) throw invalid(value, "missing child " + children[i].field.getName()); children[i].validate(child, value); }
    }
    @Override void write(FieldVector vector, int value, RecordConsumer out) throws IOException {
      StructVector struct = (StructVector) vector; out.startGroup();
      for (int i = 0; i < children.length; i++) emit(children[i].field, i, children[i], (FieldVector) struct.getChild(children[i].field.getName()), value, out);
      out.endGroup();
    }
    @Override boolean hasUnsignedLong() { for (Node child : children) if (child.hasUnsignedLong()) return true; return false; }
    @Override void prepare(DictionaryProvider dictionaries) throws IOException { for(Node child:children)child.prepare(dictionaries); }
  }

  private static class ListNode extends Node {
    final Node element;
    ListNode(Field field, Field inputField, int index, String path) throws IOException { super(field,index,path); if (field.getChildren().size()!=1) throw new UnsupportedParquetTypeException(path+": list must have one child"); if(inputField.getChildren().size()!=1)throw new InputSchemaMismatchException("input field does not match writer schema at "+path); element=Node.compile(field.getChildren().get(0),inputField.getChildren().get(0),0,path+"."+field.getChildren().get(0).getName()); }
    @Override void validate(FieldVector vector, int value) throws IOException {
      required(vector,value); if(vector.isNull(value)) return; int start=start(vector,value), end=end(vector,value);
      if(start<0||end<start||end>data(vector).getValueCount()) throw invalid(value,"invalid list offsets");
      FieldVector child=data(vector); for(int i=start;i<end;i++) element.validate(child,i);
    }
    @Override void write(FieldVector vector, int value, RecordConsumer out) throws IOException {
      int start=start(vector,value), end=end(vector,value); FieldVector child=data(vector); out.startGroup();
      if(start!=end) { out.startField("list",0); for(int i=start;i<end;i++){ out.startGroup(); emit(element.field,0,element,child,i,out); out.endGroup(); } out.endField("list",0); }
      out.endGroup();
    }
    @Override boolean hasUnsignedLong(){return element.hasUnsignedLong();}
    @Override void prepare(DictionaryProvider dictionaries) throws IOException { element.prepare(dictionaries); }
    static FieldVector data(FieldVector v) throws InvalidArrowValueException { if(v instanceof ListVector)return ((ListVector)v).getDataVector(); if(v instanceof LargeListVector)return ((LargeListVector)v).getDataVector(); if(v instanceof FixedSizeListVector)return ((FixedSizeListVector)v).getDataVector(); if(v instanceof MapVector)return ((MapVector)v).getDataVector(); throw new InvalidArrowValueException("expected list-compatible vector"); }
    static int start(FieldVector v,int i)throws InvalidArrowValueException { if(v instanceof ListVector)return ((ListVector)v).getOffsetBuffer().getInt((long)i*4); if(v instanceof LargeListVector){long x=((LargeListVector)v).getOffsetBuffer().getLong((long)i*8);if(x>Integer.MAX_VALUE)throw new InvalidArrowValueException("large-list offset exceeds int range");return(int)x;} throw new InvalidArrowValueException("expected offset list vector"); }
    static int end(FieldVector v,int i)throws InvalidArrowValueException { if(v instanceof ListVector)return ((ListVector)v).getOffsetBuffer().getInt((long)(i+1)*4); if(v instanceof LargeListVector){long x=((LargeListVector)v).getOffsetBuffer().getLong((long)(i+1)*8);if(x>Integer.MAX_VALUE)throw new InvalidArrowValueException("large-list offset exceeds int range");return(int)x;} throw new InvalidArrowValueException("expected offset list vector"); }
  }

  private static final class FixedListNode extends ListNode {
    private final int width;
    FixedListNode(Field field,Field inputField,int index,String path)throws IOException { super(field,inputField,index,path); width=((ArrowType.FixedSizeList)field.getType()).getListSize(); if(width<=0)throw new UnsupportedParquetTypeException(path+": fixed list size must be positive"); }
    @Override void validate(FieldVector vector,int value)throws IOException { required(vector,value);if(vector.isNull(value))return;if(!(vector instanceof FixedSizeListVector))throw invalid(value,"expected FixedSizeListVector");FieldVector child=data(vector);int start=Math.multiplyExact(value,width);if(start+width>child.getValueCount())throw invalid(value,"fixed-list child is truncated");for(int i=start;i<start+width;i++)element.validate(child,i); }
    @Override void write(FieldVector vector,int value,RecordConsumer out)throws IOException { FieldVector child=data(vector);int start=value*width;out.startGroup();out.startField("list",0);for(int i=start;i<start+width;i++){out.startGroup();emit(element.field,0,element,child,i,out);out.endGroup();}out.endField("list",0);out.endGroup(); }
  }

  private static final class MapNode extends Node {
    private final Node key,value; private final Field keyField,valueField;
    MapNode(Field field,Field inputField,int index,String path)throws IOException { super(field,index,path);if(field.getChildren().size()!=1||field.getChildren().get(0).getChildren().size()!=2)throw new UnsupportedParquetTypeException(path+": map must contain key/value entry");if(inputField.getChildren().size()!=1||inputField.getChildren().get(0).getChildren().size()!=2)throw new InputSchemaMismatchException("input field does not match writer schema at "+path);Field entry=field.getChildren().get(0);Field inputEntry=inputField.getChildren().get(0);keyField=entry.getChildren().get(0);valueField=entry.getChildren().get(1);if(keyField.isNullable())throw new UnsupportedParquetTypeException(path+": map key must be required");key=Node.compile(keyField,inputEntry.getChildren().get(0),0,path+"."+keyField.getName());value=Node.compile(valueField,inputEntry.getChildren().get(1),1,path+"."+valueField.getName()); }
    @Override void validate(FieldVector vector,int row)throws IOException { required(vector,row);if(vector.isNull(row))return;if(!(vector instanceof MapVector))throw invalid(row,"expected MapVector");MapVector map=(MapVector)vector;int start=map.getOffsetBuffer().getInt((long)row*4),end=map.getOffsetBuffer().getInt((long)(row+1)*4);FieldVector entries=map.getDataVector();if(start<0||end<start||end>entries.getValueCount()||!(entries instanceof StructVector))throw invalid(row,"invalid map offsets");StructVector entry=(StructVector)entries;FieldVector keys=(FieldVector)entry.getChild(keyField.getName()), values=(FieldVector)entry.getChild(valueField.getName());if(keys==null||values==null)throw invalid(row,"missing map key/value child");java.util.ArrayList<MapKeyEquality.Key> seen=new java.util.ArrayList<>();for(int i=start;i<end;i++){if(keys.isNull(i))throw invalid(row,"null map key at entry "+(i-start));key.validate(keys,i);value.validate(values,i);MapKeyEquality.Key identity=MapKeyEquality.key(keys,i,path);int previous=seen.indexOf(identity);if(previous>=0)throw new InvalidArrowValueException(path+" row "+row+": duplicate map key at entries "+previous+" and "+(i-start));seen.add(identity);} }
    @Override void write(FieldVector vector,int row,RecordConsumer out)throws IOException { MapVector map=(MapVector)vector;int start=map.getOffsetBuffer().getInt((long)row*4),end=map.getOffsetBuffer().getInt((long)(row+1)*4);StructVector entries=(StructVector)map.getDataVector();FieldVector keys=(FieldVector)entries.getChild(keyField.getName()),values=(FieldVector)entries.getChild(valueField.getName());out.startGroup();if(start!=end){out.startField("key_value",0);for(int i=start;i<end;i++){out.startGroup();emit(keyField,0,key,keys,i,out);emit(valueField,1,value,values,i,out);out.endGroup();}out.endField("key_value",0);}out.endGroup(); }
    @Override boolean hasUnsignedLong(){return key.hasUnsignedLong()||value.hasUnsignedLong();}
    @Override void prepare(DictionaryProvider dictionaries) throws IOException { key.prepare(dictionaries);value.prepare(dictionaries); }
  }

  private enum Kind {
    NULL, BOOL, INT8, UINT8, INT16, UINT16, INT32, UINT32, INT64, UINT64,
    FLOAT4, FLOAT8, UTF8, BINARY, FIXED_BINARY, DECIMAL32, DECIMAL64, DECIMAL_FLBA,
    DATE32, DATE64, TIME32_SECOND, TIME32_MILLI, TIME64_MICRO, TIME64_NANO,
    TIMESTAMP_SECOND, TIMESTAMP_MILLI, TIMESTAMP_MICRO, TIMESTAMP_NANO, DURATION
  }

  private static int dictionaryIndex(FieldVector vector, int row, String path) throws InvalidArrowValueException {
    ArrowType type=vector.getField().getType();if(type.getTypeID()!=ArrowType.ArrowTypeID.Int)throw new InvalidArrowValueException(path+": dictionary index vector must be integer");ArrowType.Int integer=(ArrowType.Int)type;ArrowBuf data=vector.getDataBuffer();long value;switch(integer.getBitWidth()){case 8:value=integer.getIsSigned()?data.getByte(row):Byte.toUnsignedInt(data.getByte(row));break;case 16:value=integer.getIsSigned()?data.getShort((long)row*2):Short.toUnsignedInt(data.getShort((long)row*2));break;case 32:value=integer.getIsSigned()?data.getInt((long)row*4):Integer.toUnsignedLong(data.getInt((long)row*4));break;case 64:value=data.getLong((long)row*8);break;default:throw new InvalidArrowValueException(path+": unsupported dictionary index width");}if(value<0||value>Integer.MAX_VALUE)throw new InvalidArrowValueException(path+": dictionary index out of range");return(int)value;
  }

  private static final class Leaf {
    final Field field; final int index; final Kind kind; final int fixedWidth; final byte[] decimalScratch;
    private Leaf(Field field, int index, Kind kind, int fixedWidth) {
      this.field = field; this.index = index; this.kind = kind; this.fixedWidth = fixedWidth;
      this.decimalScratch = kind == Kind.DECIMAL_FLBA ? new byte[fixedWidth] : null;
    }

    static Leaf compile(Field field, int index) throws UnsupportedParquetTypeException {
      if (!field.getChildren().isEmpty())
        throw new UnsupportedParquetTypeException("primitive field has children " + field.getName());
      ArrowType type = field.getType();
      switch (type.getTypeID()) {
        case Null:
          if (!field.isNullable()) throw new UnsupportedParquetTypeException("required Null field " + field.getName());
          return new Leaf(field, index, Kind.NULL, 0);
        case Bool: return new Leaf(field, index, Kind.BOOL, 0);
        case Int: {
          ArrowType.Int v = (ArrowType.Int) type;
          if (v.getBitWidth() == 8) return new Leaf(field, index, v.getIsSigned() ? Kind.INT8 : Kind.UINT8, 0);
          if (v.getBitWidth() == 16) return new Leaf(field, index, v.getIsSigned() ? Kind.INT16 : Kind.UINT16, 0);
          if (v.getBitWidth() == 32) return new Leaf(field, index, v.getIsSigned() ? Kind.INT32 : Kind.UINT32, 0);
          if (v.getBitWidth() == 64) return new Leaf(field, index, v.getIsSigned() ? Kind.INT64 : Kind.UINT64, 0);
          break;
        }
        case FloatingPoint:
          switch (((ArrowType.FloatingPoint) type).getPrecision()) {
            case SINGLE: return new Leaf(field, index, Kind.FLOAT4, 0);
            case DOUBLE: return new Leaf(field, index, Kind.FLOAT8, 0);
            default: break;
          }
          break;
        case Utf8: case LargeUtf8: case Utf8View: return new Leaf(field, index, Kind.UTF8, 0);
        case Binary: case LargeBinary: case BinaryView: return new Leaf(field, index, Kind.BINARY, 0);
        case FixedSizeBinary: {
          int n = ((ArrowType.FixedSizeBinary) type).getByteWidth();
          if (n > 0) return new Leaf(field, index, Kind.FIXED_BINARY, n);
          break;
        }
        case Decimal: {
          ArrowType.Decimal d = (ArrowType.Decimal) type;
          int max = d.getBitWidth() == 128 ? 38 : d.getBitWidth() == 256 ? 76 : 0;
          if (d.getPrecision() < 1 || d.getPrecision() > max || d.getScale() < 0 || d.getScale() > d.getPrecision()) break;
          if (d.getBitWidth() == 128 && d.getPrecision() <= 9) return new Leaf(field, index, Kind.DECIMAL32, 0);
          if (d.getBitWidth() == 128 && d.getPrecision() <= 18) return new Leaf(field, index, Kind.DECIMAL64, 0);
          return new Leaf(field, index, Kind.DECIMAL_FLBA, d.getBitWidth() == 256 ? 32 : ArrowSchemaToParquet.minDecimalBytes(d.getPrecision()));
        }
        case Date:
          return new Leaf(field, index, ((ArrowType.Date) type).getUnit() == DateUnit.DAY ? Kind.DATE32 : Kind.DATE64, 0);
        case Time: {
          ArrowType.Time t = (ArrowType.Time) type;
          if (t.getBitWidth() == 32 && t.getUnit() == TimeUnit.SECOND) return new Leaf(field, index, Kind.TIME32_SECOND, 0);
          if (t.getBitWidth() == 32 && t.getUnit() == TimeUnit.MILLISECOND) return new Leaf(field, index, Kind.TIME32_MILLI, 0);
          if (t.getBitWidth() == 64 && t.getUnit() == TimeUnit.MICROSECOND) return new Leaf(field, index, Kind.TIME64_MICRO, 0);
          if (t.getBitWidth() == 64 && t.getUnit() == TimeUnit.NANOSECOND) return new Leaf(field, index, Kind.TIME64_NANO, 0);
          break;
        }
        case Timestamp: {
          ArrowType.Timestamp t = (ArrowType.Timestamp) type;
          if ("".equals(t.getTimezone())) break;
          if (t.getUnit() == TimeUnit.SECOND) return new Leaf(field, index, Kind.TIMESTAMP_SECOND, 0);
          if (t.getUnit() == TimeUnit.MILLISECOND) return new Leaf(field, index, Kind.TIMESTAMP_MILLI, 0);
          if (t.getUnit() == TimeUnit.MICROSECOND) return new Leaf(field, index, Kind.TIMESTAMP_MICRO, 0);
          if (t.getUnit() == TimeUnit.NANOSECOND) return new Leaf(field, index, Kind.TIMESTAMP_NANO, 0);
          break;
        }
        case Duration: return new Leaf(field, index, Kind.DURATION, 0);
        default: break;
      }
      throw new UnsupportedParquetTypeException("unsupported Stage 4 primitive field " + field);
    }

    void validateValue(FieldVector vector, int row) throws IOException {
      ArrowBuf values = vector.getDataBuffer();
      if (kind == Kind.NULL) {
        if (!vector.isNull(row)) throw invalid(row, "Null vector has a defined value");
        return;
      }
      if (vector.isNull(row)) return;
      switch (kind) {
          case UTF8: validateUtf8(variableBytes(vector, row).toByteBuffer(), row); break;
          case FIXED_BINARY:
            if ((long) (row + 1) * fixedWidth > values.capacity()) throw invalid(row, "fixed-size-binary buffer is truncated");
            break;
          case DECIMAL32: case DECIMAL64: case DECIMAL_FLBA:
            validateDecimal(values, row);
            break;
          case DATE64: {
            long v = values.getLong((long) row * 8);
            if (v % MILLIS_PER_DAY != 0 || v / MILLIS_PER_DAY < Integer.MIN_VALUE || v / MILLIS_PER_DAY > Integer.MAX_VALUE)
              throw invalid(row, "Date64 must be an exact int32 day");
            break;
          }
          case TIME32_SECOND: range(row, values.getInt((long) row * 4), 0, 86_399, "Time32(second)"); break;
          case TIME32_MILLI: range(row, values.getInt((long) row * 4), 0, 86_399_999, "Time32(milli)"); break;
          case TIME64_MICRO: range(row, values.getLong((long) row * 8), 0, 86_399_999_999L, "Time64(micro)"); break;
          case TIME64_NANO: range(row, values.getLong((long) row * 8), 0, 86_399_999_999_999L, "Time64(nano)"); break;
          case TIMESTAMP_SECOND:
            try { Math.multiplyExact(values.getLong((long) row * 8), 1_000L); }
            catch (ArithmeticException bad) { throw invalid(row, "timestamp seconds overflows millis"); }
            break;
        default: break;
      }
    }

    void write(FieldVector vector, int row, RecordConsumer out) throws IOException {
      ArrowBuf values = vector.getDataBuffer();
      switch (kind) {
        case NULL: return;
        case BOOL: out.addBoolean((values.getByte(row >>> 3) & (1 << (row & 7))) != 0); return;
        case INT8: out.addInteger(values.getByte(row)); return;
        case UINT8: out.addInteger(Byte.toUnsignedInt(values.getByte(row))); return;
        case INT16: out.addInteger(values.getShort((long) row * 2)); return;
        case UINT16: out.addInteger(Short.toUnsignedInt(values.getShort((long) row * 2))); return;
        case INT32: case UINT32: out.addInteger(values.getInt((long) row * 4)); return;
        case INT64: case UINT64: case TIMESTAMP_MILLI: case TIMESTAMP_MICRO: case TIMESTAMP_NANO: case DURATION:
          out.addLong(values.getLong((long) row * 8)); return;
        case FLOAT4: out.addFloat(values.getFloat((long) row * 4)); return;
        case FLOAT8: out.addDouble(values.getDouble((long) row * 8)); return;
        case UTF8: case BINARY: out.addBinary(variableBytes(vector, row)); return;
        case FIXED_BINARY: out.addBinary(binary(values, row * fixedWidth, (row + 1) * fixedWidth)); return;
        case DECIMAL32: out.addInteger(values.getInt((long) row * 16)); return;
        case DECIMAL64: out.addLong(values.getLong((long) row * 16)); return;
        case DECIMAL_FLBA: writeDecimal(values, row); out.addBinary(Binary.fromReusedByteArray(decimalScratch)); return;
        case DATE32: out.addInteger(values.getInt((long) row * 4)); return;
        case DATE64: out.addInteger((int) (values.getLong((long) row * 8) / MILLIS_PER_DAY)); return;
        case TIME32_SECOND: out.addInteger(Math.multiplyExact(values.getInt((long) row * 4), 1_000)); return;
        case TIME32_MILLI: out.addInteger(values.getInt((long) row * 4)); return;
        case TIME64_MICRO: case TIME64_NANO: out.addLong(values.getLong((long) row * 8)); return;
        case TIMESTAMP_SECOND: out.addLong(Math.multiplyExact(values.getLong((long) row * 8), 1_000L)); return;
        default: throw new AssertionError(kind);
      }
    }

    private void writeDecimal(ArrowBuf values, int row) {
      int sourceWidth = ((ArrowType.Decimal) field.getType()).getBitWidth() / 8;
      long source = (long) row * sourceWidth;
      // Arrow stores decimal words little-endian; Parquet FLBA is big-endian two's complement.
      for (int i = 0; i < fixedWidth; i++) decimalScratch[fixedWidth - 1 - i] = values.getByte(source + i);
    }
    private void validateDecimal(ArrowBuf values, int row) throws InvalidArrowValueException {
      ArrowType.Decimal decimal = (ArrowType.Decimal) field.getType();
      int sourceWidth = decimal.getBitWidth() / 8;
      long source = (long) row * sourceWidth;
      if (source < 0 || source + sourceWidth > values.capacity()) throw invalid(row, "decimal buffer is truncated");
      int physicalWidth = kind == Kind.DECIMAL32 ? 4 : kind == Kind.DECIMAL64 ? 8 : fixedWidth;
      boolean negative = (values.getByte(source + physicalWidth - 1) & 0x80) != 0;
      for (int i = physicalWidth; i < sourceWidth; i++) if (values.getByte(source + i) != (byte) (negative ? 0xff : 0))
        throw invalid(row, "decimal does not sign-extend to physical width");
      byte[] bigEndian = new byte[sourceWidth];
      for (int i = 0; i < sourceWidth; i++) bigEndian[sourceWidth - 1 - i] = values.getByte(source + i);
      BigInteger unscaled = new BigInteger(bigEndian);
      if (unscaled.abs().compareTo(BigInteger.TEN.pow(decimal.getPrecision())) >= 0)
        throw invalid(row, "decimal exceeds declared precision");
      if (unscaled.bitLength() > physicalWidth * 8 - 1)
        throw invalid(row, "decimal exceeds physical width capacity");
    }
    private static Binary variableBytes(FieldVector vector, int row) throws InvalidArrowValueException {
      ArrowType.ArrowTypeID type = vector.getField().getType().getTypeID();
      if (type == ArrowType.ArrowTypeID.LargeUtf8 || type == ArrowType.ArrowTypeID.LargeBinary) {
        long start = vector.getOffsetBuffer().getLong((long) row * 8);
        long end = vector.getOffsetBuffer().getLong((long) (row + 1) * 8);
        if (start < 0 || end < start || end - start > Integer.MAX_VALUE) throw new InvalidArrowValueException("invalid large variable-width offsets at row " + row);
        return binary(vector.getDataBuffer(), (int) start, (int) end);
      }
      if (type == ArrowType.ArrowTypeID.Utf8View) return Binary.fromReusedByteArray(((org.apache.arrow.vector.ViewVarCharVector) vector).get(row));
      if (type == ArrowType.ArrowTypeID.BinaryView) return Binary.fromReusedByteArray(((org.apache.arrow.vector.ViewVarBinaryVector) vector).get(row));
      int start = vector.getOffsetBuffer().getInt((long) row * 4);
      int end = vector.getOffsetBuffer().getInt((long) (row + 1) * 4);
      if (start < 0 || end < start) throw new InvalidArrowValueException("invalid variable-width offsets at row " + row);
      return binary(vector.getDataBuffer(), start, end);
    }
    private static Binary binary(ArrowBuf values, int start, int end) {
      ByteBuffer bytes = values.nioBuffer(start, end - start);
      return Binary.fromReusedByteBuffer(bytes, 0, end - start);
    }
    private static void range(int row, long v, long lo, long hi, String type) throws InvalidArrowValueException {
      if (v < lo || v > hi) throw new InvalidArrowValueException(type + " out of range at row " + row);
    }
    private void validateUtf8(ByteBuffer value, int row) throws InvalidArrowValueException {
      try { StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).decode(value); }
      catch (CharacterCodingException invalid) { throw invalid(row, "invalid UTF-8"); }
    }
    private InvalidArrowValueException invalid(int row, String message) { return new InvalidArrowValueException(field.getName() + " row " + row + ": " + message); }
  }
}
