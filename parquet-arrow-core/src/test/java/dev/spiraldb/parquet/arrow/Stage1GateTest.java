// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/** The Stage 1 gate is deliberately a fresh-JVM friendly real Parquet codec test. */
class Stage1GateTest {
  private static final Schema SCHEMA=new Schema(List.of(new Field("i",FieldType.nullable(new ArrowType.Int(32,true)),List.of()),new Field("s",FieldType.nullable(ArrowType.Utf8.INSTANCE),List.of()),new Field("b",FieldType.nullable(ArrowType.Bool.INSTANCE),List.of())));
  @Test void noHadoopAndAllCodecsRoundTrip() throws Exception {
    // Amended gate (Option 1, 2026-07-11): "Hadoop-free" = no Hadoop dependency JAR.
    // Minimal org.apache.hadoop.* shims ship inside core, so Configuration DOES resolve
    // — but from our own code source, never a hadoop-*.jar.
    var src=Class.forName("org.apache.hadoop.conf.Configuration").getProtectionDomain().getCodeSource();
    String loc=src==null?"jdk":src.getLocation().toString();
    assertFalse(loc.contains("hadoop"),"org.apache.hadoop.conf.Configuration must resolve from our shim, not a hadoop jar: "+loc);
    for(Compression codec:Compression.values()) roundTrip(codec);
  }
  @Test void emptyFileAndOwnership() throws Exception { Path path=Files.createTempFile("stage1-empty",".parquet"); Files.delete(path); try(RootAllocator allocator=new RootAllocator(); ParquetArrowWriter writer=ParquetArrow.writer(SCHEMA).build(path)){assertEquals(0,writer.finish().rowCount());} try(RootAllocator allocator=new RootAllocator(); ParquetArrowReader reader=ParquetArrow.reader(allocator).build(path)){assertFalse(reader.loadNextBatch());} Files.deleteIfExists(path); Path channelPath=Files.createTempFile("stage1-channel",".parquet"); try(SeekableByteChannel c=Files.newByteChannel(channelPath,StandardOpenOption.WRITE,StandardOpenOption.TRUNCATE_EXISTING)){try(ParquetArrowWriter writer=ParquetArrow.writer(SCHEMA).build(c,CloseMode.LEAVE_OPEN)){writer.finish();}assertTrue(c.isOpen());} try(SeekableByteChannel c=Files.newByteChannel(channelPath,StandardOpenOption.WRITE,StandardOpenOption.TRUNCATE_EXISTING)){try(ParquetArrowWriter writer=ParquetArrow.writer(SCHEMA).build(c,CloseMode.CLOSE)){writer.finish();}assertFalse(c.isOpen());} Files.deleteIfExists(channelPath); }
  @Test void readerChannelOwnershipHonorsBothModes() throws Exception { Path path=Files.createTempFile("stage1-reader-channel",".parquet"); Files.delete(path); try(RootAllocator allocator=new RootAllocator(); ParquetArrowWriter writer=ParquetArrow.writer(SCHEMA).build(path)){writer.finish();} try(RootAllocator allocator=new RootAllocator(); SeekableByteChannel leave=Files.newByteChannel(path,StandardOpenOption.READ)){try(ParquetArrowReader reader=ParquetArrow.reader(allocator).build(leave,CloseMode.LEAVE_OPEN)){assertFalse(reader.loadNextBatch());}assertTrue(leave.isOpen());} try(RootAllocator allocator=new RootAllocator(); SeekableByteChannel close=Files.newByteChannel(path,StandardOpenOption.READ)){try(ParquetArrowReader reader=ParquetArrow.reader(allocator).build(close,CloseMode.CLOSE)){assertFalse(reader.loadNextBatch());}assertFalse(close.isOpen());} finally {Files.deleteIfExists(path);} }
  private static void roundTrip(Compression codec)throws Exception { Path path=Files.createTempFile("stage1-"+codec,".parquet"); Files.delete(path); try(RootAllocator allocator=new RootAllocator(); VectorSchemaRoot root=VectorSchemaRoot.create(SCHEMA,allocator)){root.allocateNew();((IntVector)root.getVector(0)).setSafe(0,7);((VarCharVector)root.getVector(1)).setSafe(0,"one".getBytes());((BitVector)root.getVector(2)).setSafe(0,1);((VarCharVector)root.getVector(1)).setSafe(1,"two".getBytes());((BitVector)root.getVector(2)).setNull(1);((IntVector)root.getVector(0)).setSafe(2,9);((BitVector)root.getVector(2)).setSafe(2,0);root.setRowCount(3); ParquetFileInfo info;try(ParquetArrowWriter writer=ParquetArrow.writer(SCHEMA).options(WriteOptions.builder().compression(codec).build()).build(path)){writer.writeBatch(root);info=writer.finish();}assertNotNull(info.createdBy());assertTrue(info.createdBy().contains("parquet-mr version 1.17.1"));try(ParquetArrowReader reader=ParquetArrow.reader(allocator).build(path)){assertTrue(reader.loadNextBatch());VectorSchemaRoot result=reader.getVectorSchemaRoot();assertEquals(3,result.getRowCount());assertEquals(7,((IntVector)result.getVector(0)).get(0));assertArrayEquals("two".getBytes(),((VarCharVector)result.getVector(1)).get(1));assertTrue(result.getVector(0).isNull(1));assertTrue(result.getVector(2).isNull(1));assertFalse(reader.loadNextBatch());}} finally {Files.deleteIfExists(path);} }
}
