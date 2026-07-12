// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ColumnChunkPageWriteStore;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

/** Direct parquet-java writer over the compiled Arrow-indexed event plan. */
public final class ParquetArrowWriter implements AutoCloseable {
  private final Schema schema; private final WriteOptions options; private final InputSchemaCompatibility compatibility; private final OutputFile output; private MessageType physical; private VectorWritePlan plan; private final CoreCompressionCodecFactory codecs=new CoreCompressionCodecFactory(); private ParquetProperties properties; private Object file;
  /** Physical Arrow value schema; dictionary index bindings never escape the write boundary. */
  private Schema logicalSchema;
  private ColumnChunkPageWriteStore pages; private ColumnWriteStore columns; private RecordConsumer records; private long inGroup, rows, batches, groups, buffered, peakBuffered, maximumOvershoot; private boolean finished, closed, closing, aborted; private Throwable abortCause; private ParquetFileInfo info;
  ParquetArrowWriter(Schema schema,WriteOptions options,InputSchemaCompatibility compatibility,OutputFile output)throws IOException {
    this.schema=schema;this.options=options;this.compatibility=compatibility;this.output=output;
  }
  public Schema schema(){return schema;}
  public void writeBatch(VectorSchemaRoot root)throws IOException{writeBatch(root,null);} public void writeBatch(VectorSchemaRoot root,DictionaryProvider provider)throws IOException {
    ensureOpen(); Schema resolved=resolveDictionaryValueSchema(provider); VectorWritePlan candidate=plan;
    if(candidate==null) candidate=VectorWritePlan.compile(schema,resolved,compatibility);
    else if(!logicalSchema.equals(resolved)) throw new InputSchemaMismatchException("dictionary value type changed between batches");
    // PRECHECK: resolving and validating the batch happens before opening the Parquet writer.
    candidate.preflight(root,provider);
    if(plan==null) initialize(resolved,candidate);
    try { for(int row=0;row<root.getRowCount();row++){ensureGroup(); records.startMessage();plan.writeRow(root,row,records);records.endMessage();inGroup++;rows++;buffered=columns.getBufferedSize();peakBuffered=Math.max(peakBuffered,buffered);maximumOvershoot=Math.max(maximumOvershoot,Math.max(0,buffered-options.targetRowGroupBytes()));if(inGroup>=options.maxRowGroupRows()||buffered>=options.targetRowGroupBytes())flushRowGroup();}batches++; }
    catch(IOException failure){throw abort(failure);} catch(RuntimeException failure){abort(failure);throw failure;}
  }
  public void writeAll(ArrowReader input)throws IOException{while(input.loadNextBatch())writeBatch(input.getVectorSchemaRoot(),input);if(plan==null)initializeFromDictionaryProvider(input);}
  public void writeAll(ArrowBatchStream input)throws IOException{while(input.loadNextBatch())writeBatch(input.getVectorSchemaRoot(),input.dictionaries());if(plan==null)initializeFromDictionaryProvider(input.dictionaries());}
  private void initializeFromDictionaryProvider(DictionaryProvider provider) throws IOException { Schema resolved=resolveDictionaryValueSchema(provider); initialize(resolved,VectorWritePlan.compile(schema,resolved,compatibility)); }
  private void initialize(Schema resolved, VectorWritePlan compiled) throws IOException {
    MessageType candidatePhysical=ArrowSchemaToParquet.toParquet(resolved);
    ParquetProperties.Builder builder=ParquetProperties.builder().withPageSize(options.pageSizeBytes()).withPageRowCountLimit(options.pageRowLimit()).withDictionaryEncoding(options.parquetDictionaryEnabled()).withDictionaryPageSize(options.dictionaryPageSizeBytes()).withStatisticsEnabled(options.statisticsEnabled()).withPageWriteChecksumEnabled(options.pageChecksums()).withBloomFilterEnabled(false).withWriterVersion(ParquetFormatProfile.writerVersion(options.dataPageVersion()));
    if(compiled.hasUnsignedLong()) disableUnsignedLongStatistics(builder, resolved, candidatePhysical);
    ParquetProperties candidateProperties=builder.build(); Object created=null;
    try { created=newWriter(output,candidatePhysical,options.targetRowGroupBytes(),candidateProperties); call(created,"start"); }
    catch(Throwable failure){
      if(created!=null)try{call(created,"close");}catch(IOException cleanup){failure.addSuppressed(cleanup);}
      try{codecs.release();}catch(Throwable cleanup){failure.addSuppressed(cleanup);}
      if(output instanceof ChannelOutputFile)try{((ChannelOutputFile)output).close();}catch(Throwable cleanup){failure.addSuppressed(cleanup);}
      abortCause=failure;aborted=true;closed=true;
      if(failure instanceof IOException)throw(IOException)failure;if(failure instanceof RuntimeException)throw(RuntimeException)failure;throw new IOException("start parquet writer",failure);
    }
    logicalSchema=resolved; plan=compiled; physical=candidatePhysical; properties=candidateProperties; file=created;
  }
  private Schema resolveDictionaryValueSchema(DictionaryProvider provider) throws IOException {
    java.util.List<org.apache.arrow.vector.types.pojo.Field> fields=new java.util.ArrayList<>();
    for(org.apache.arrow.vector.types.pojo.Field field:schema.getFields()) fields.add(resolveDictionaryValueField(field,provider,field.getName()));
    return new Schema(fields,schema.getCustomMetadata());
  }
  private static org.apache.arrow.vector.types.pojo.Field resolveDictionaryValueField(org.apache.arrow.vector.types.pojo.Field field,DictionaryProvider provider,String path)throws IOException {
    if(field.getDictionary()!=null){
      if(provider==null)throw new InvalidArrowValueException(path+": dictionary provider is required to resolve its value type");
      org.apache.arrow.vector.dictionary.Dictionary dictionary=provider.lookup(field.getDictionary().getId());
      if(dictionary==null||dictionary.getVector()==null)throw new InvalidArrowValueException(path+": missing dictionary "+field.getDictionary().getId());
      if(!(dictionary.getVector() instanceof FieldVector))throw new InvalidArrowValueException(path+": dictionary vector is not an Arrow field vector");
      org.apache.arrow.vector.types.pojo.Field value=((FieldVector)dictionary.getVector()).getField();
      org.apache.arrow.vector.types.pojo.FieldType type=new org.apache.arrow.vector.types.pojo.FieldType(field.isNullable(),value.getType(),null,field.getMetadata());
      java.util.List<org.apache.arrow.vector.types.pojo.Field> children=new java.util.ArrayList<>();
      for(org.apache.arrow.vector.types.pojo.Field child:value.getChildren())children.add(resolveDictionaryValueField(child,provider,path+"."+child.getName()));
      return new org.apache.arrow.vector.types.pojo.Field(field.getName(),type,children);
    }
    java.util.List<org.apache.arrow.vector.types.pojo.Field> children=new java.util.ArrayList<>();
    for(org.apache.arrow.vector.types.pojo.Field child:field.getChildren())children.add(resolveDictionaryValueField(child,provider,path+"."+child.getName()));
    return new org.apache.arrow.vector.types.pojo.Field(field.getName(),field.getFieldType(),children);
  }
  private Schema zeroBatchSchema() throws IOException {
    requireKnownZeroBatchDictionaryTypes(schema.getFields(),"");
    java.util.List<org.apache.arrow.vector.types.pojo.Field> fields = new java.util.ArrayList<>();
    for (org.apache.arrow.vector.types.pojo.Field field : schema.getFields()) fields.add(stripDictionary(field));
    return new Schema(fields, schema.getCustomMetadata());
  }
  private static org.apache.arrow.vector.types.pojo.Field stripDictionary(org.apache.arrow.vector.types.pojo.Field field) {
    java.util.List<org.apache.arrow.vector.types.pojo.Field> children = new java.util.ArrayList<>();
    for (org.apache.arrow.vector.types.pojo.Field child : field.getChildren()) children.add(stripDictionary(child));
    return new org.apache.arrow.vector.types.pojo.Field(field.getName(), new org.apache.arrow.vector.types.pojo.FieldType(
        field.isNullable(), field.getType(), null, field.getMetadata()), children);
  }
  private static void requireKnownZeroBatchDictionaryTypes(java.util.List<org.apache.arrow.vector.types.pojo.Field> fields,String prefix)throws IOException { for(org.apache.arrow.vector.types.pojo.Field field:fields){String path=prefix.isEmpty()?field.getName():prefix+"."+field.getName();if(field.getDictionary()!=null&&field.getType().equals(field.getDictionary().getIndexType()))throw new InvalidArrowValueException(path+": cannot finish a zero-batch writer with an index-typed dictionary field; write a batch with its DictionaryProvider first");requireKnownZeroBatchDictionaryTypes(field.getChildren(),path);} }
  private void ensureGroup(){if(records!=null)return; pages=new ColumnChunkPageWriteStore(codecs.getCompressor(codec()),physical,HeapByteBufferAllocator.getInstance(),options.pageSizeBytes(),options.pageChecksums());columns=properties.newColumnWriteStore(physical,pages);MessageColumnIO columnIO=new ColumnIOFactory().getColumnIO(physical);records=columnIO.getRecordWriter(columns);}
  public void flushRowGroup()throws IOException {ensureOpen();if(inGroup==0)return;try{records.flush();call(file,"startBlock",new Class<?>[]{long.class},inGroup);columns.flush();flushPages();call(file,"endBlock");pages.close();pages=null;columns=null;records=null;inGroup=0;buffered=0;groups++;}catch(IOException failure){throw abort(failure);}catch(RuntimeException failure){abort(failure);throw failure;}}
  public ParquetFileInfo finish()throws IOException {if(finished)return info;ensureOpen();try{if(plan==null){Schema zero=zeroBatchSchema();initialize(zero,VectorWritePlan.compile(schema,zero,compatibility));}flushRowGroup();Map<String,String> metadata=new LinkedHashMap<>();metadata.put(ArrowSchemaMetadataCodec.KEY,new ArrowSchemaMetadataCodec().encode(logicalSchema));call(file,"end",new Class<?>[]{Map.class},metadata);finished=true;org.apache.parquet.hadoop.metadata.FileMetaData footer=((org.apache.parquet.hadoop.metadata.ParquetMetadata)call(file,"getFooter")).getFileMetaData();info=new ParquetFileInfo(rows,footer.getCreatedBy(),logicalSchema,footer.getKeyValueMetaData());return info;}catch(IOException failure){throw abort(failure);}catch(RuntimeException failure){abort(failure);throw failure;}}
  private CompressionCodecName codec(){switch(options.compression()){case UNCOMPRESSED:return CompressionCodecName.UNCOMPRESSED;case ZSTD:return CompressionCodecName.ZSTD;case SNAPPY:return CompressionCodecName.SNAPPY;case GZIP:return CompressionCodecName.GZIP;default:throw new AssertionError();}}
  public WriteMetrics metrics(){long bytes;try{bytes=file==null?0:(Long)call(file,"getPos");}catch(IOException e){bytes=0;}return new WriteMetrics(batches,rows,groups,bytes,buffered,peakBuffered,maximumOvershoot);} private void ensureOpen()throws IOException{if(aborted)throw new WriterAbortedException("writer aborted",abortCause);if(finished||closed)throw new IOException("writer finished");}
  @Override public void close()throws IOException {if(closed||closing)return;closing=true;Throwable failure=null;try{if(!finished&&!aborted)finish();}catch(Throwable e){failure=e;}finally{failure=release(failure,()->{if(pages!=null)pages.close();});pages=null;columns=null;records=null;if(file!=null)failure=release(failure,()->call(file,"close"));failure=release(failure,()->codecs.release());if(output instanceof ChannelOutputFile)failure=release(failure,()->((ChannelOutputFile)output).close());closed=true;closing=false;}if(failure==null)return;if(failure instanceof IOException)throw(IOException)failure;if(failure instanceof RuntimeException)throw(RuntimeException)failure;if(failure instanceof Error)throw(Error)failure;throw new IOException("writer close",failure);}
  private void flushPages()throws IOException {try{Class<?> writer=Class.forName("org.apache.parquet.hadoop.ParquetFileWriter");MethodHandles.publicLookup().findVirtual(pages.getClass(),"flushToFileWriter",MethodType.methodType(void.class,writer)).invoke(pages,file);}catch(Throwable e){throw failure("flush pages",e);}}
  private IOException abort(IOException failure){abort((Throwable)failure);return failure;}
  private void abort(Throwable failure){if(aborted)return;aborted=true;abortCause=failure;if(file!=null)try{call(file,"abort");}catch(IOException cleanup){failure.addSuppressed(cleanup);}try{if(pages!=null)pages.close();}catch(RuntimeException cleanup){failure.addSuppressed(cleanup);}finally{pages=null;columns=null;records=null;}}
  private static void disableUnsignedLongStatistics(ParquetProperties.Builder builder,Schema schema,MessageType physical)throws IOException { for(int i=0;i<schema.getFields().size();i++) disableUnsignedLongStatistics(builder,schema.getFields().get(i),physical.getType(i),physical.getType(i).getName()); }
  private static void disableUnsignedLongStatistics(ParquetProperties.Builder builder,org.apache.arrow.vector.types.pojo.Field field,org.apache.parquet.schema.Type physical,String path)throws IOException {if(field.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Int){org.apache.arrow.vector.types.pojo.ArrowType.Int integer=(org.apache.arrow.vector.types.pojo.ArrowType.Int)field.getType();if(integer.getBitWidth()==64&&!integer.getIsSigned()){invokeColumnBoolean(builder,"withStatisticsEnabled",path);invokeColumnBoolean(builder,"withSizeStatisticsEnabled",path);}}if(field.getChildren().isEmpty())return;org.apache.parquet.schema.GroupType group=physical.asGroupType();switch(field.getType().getTypeID()){case List:case LargeList:case FixedSizeList:{org.apache.parquet.schema.Type wrapper=group.getType(0);disableUnsignedLongStatistics(builder,field.getChildren().get(0),wrapper.asGroupType().getType(0),path+"."+wrapper.getName()+"."+wrapper.asGroupType().getType(0).getName());return;}case Map:{org.apache.parquet.schema.Type entry=group.getType(0);org.apache.parquet.schema.GroupType entries=entry.asGroupType();org.apache.arrow.vector.types.pojo.Field arrowEntry=field.getChildren().get(0);disableUnsignedLongStatistics(builder,arrowEntry.getChildren().get(0),entries.getType(0),path+"."+entry.getName()+"."+entries.getType(0).getName());disableUnsignedLongStatistics(builder,arrowEntry.getChildren().get(1),entries.getType(1),path+"."+entry.getName()+"."+entries.getType(1).getName());return;}default:for(int i=0;i<field.getChildren().size();i++){org.apache.parquet.schema.Type child=group.getType(i);disableUnsignedLongStatistics(builder,field.getChildren().get(i),child,path+"."+child.getName());}}}
  @FunctionalInterface private interface Release { void run() throws Exception; }
  private static Throwable release(Throwable previous, Release release) { try { release.run(); } catch (Throwable cleanup) { if(previous==null)return cleanup;previous.addSuppressed(cleanup); } return previous; }
  private static void invokeColumnBoolean(ParquetProperties.Builder builder,String name,String path)throws IOException {try{for(java.lang.reflect.Method method:builder.getClass().getMethods()){Class<?>[] parameters=method.getParameterTypes();if(method.getName().equals(name)&&parameters.length==2&&parameters[0]==String.class&&(parameters[1]==boolean.class||parameters[1]==Boolean.class)){method.invoke(builder,path,false);return;}}throw new NoSuchMethodException(name+"(String,boolean)");}catch(ReflectiveOperationException failure){throw new IOException("parquet-java does not expose the required unsigned-long statistics gate",failure);}}
  @SuppressWarnings({"unchecked","rawtypes"}) private static Object newWriter(OutputFile output,MessageType schema,long blockSize,ParquetProperties properties)throws IOException {try{Class<?> writer=Class.forName("org.apache.parquet.hadoop.ParquetFileWriter");Class<?> mode=Class.forName("org.apache.parquet.hadoop.ParquetFileWriter$Mode");Object create=Enum.valueOf((Class<? extends Enum>)mode,"CREATE");return MethodHandles.publicLookup().findConstructor(writer,MethodType.methodType(void.class,OutputFile.class,MessageType.class,mode,long.class,int.class,org.apache.parquet.crypto.FileEncryptionProperties.class,ParquetProperties.class)).invoke(output,schema,create,blockSize,0,null,properties);}catch(Throwable e){throw failure("construct parquet writer",e);}}
  private static Object call(Object target,String name)throws IOException{return call(target,name,new Class<?>[0]);} private static Object call(Object target,String name,Class<?>[] types,Object... args)throws IOException {try{Class<?> returnType; if(name.equals("getPos"))returnType=long.class;else if(name.equals("getFooter"))returnType=org.apache.parquet.hadoop.metadata.ParquetMetadata.class;else returnType=void.class;return MethodHandles.publicLookup().findVirtual(target.getClass(),name,MethodType.methodType(returnType,types)).invokeWithArguments(join(target,args));}catch(Throwable e){throw failure(name,e);}} private static Object[] join(Object target,Object[] args){Object[] all=new Object[args.length+1];all[0]=target;System.arraycopy(args,0,all,1,args.length);return all;} private static IOException failure(String action,Throwable e){Throwable cause=e instanceof java.lang.reflect.InvocationTargetException?((java.lang.reflect.InvocationTargetException)e).getCause():e;return cause instanceof IOException?(IOException)cause:new IOException(action,cause);}
}
