// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.parquet.io.InputFile;

/** Reader builder. */
public final class ReadBuilder { private final BufferAllocator allocator; private ReadOptions options=ReadOptions.builder().build(); private ColumnProjection projection=ColumnProjection.all(); private ReadEngineId engine=ReadEngineId.PARQUET_JAVA; ReadBuilder(BufferAllocator allocator){this.allocator=Objects.requireNonNull(allocator);} public ReadBuilder options(ReadOptions v){options=Objects.requireNonNull(v);return this;} public ReadBuilder projection(ColumnProjection v){projection=Objects.requireNonNull(v);return this;} public ReadBuilder engine(ReadEngineId v){engine=Objects.requireNonNull(v);return this;} public ParquetArrowReader build(InputFile input)throws IOException{return build0(input);} public ParquetArrowReader build(Path input)throws IOException{return build0(NioFiles.input(input));} public ParquetArrowReader build(SeekableByteChannel input)throws IOException{return build(input,CloseMode.LEAVE_OPEN);} public ParquetArrowReader build(SeekableByteChannel input,CloseMode mode)throws IOException{return build0(new ChannelInputFile(input,mode==CloseMode.CLOSE));} private ParquetArrowReader build0(InputFile input)throws IOException{if(engine!=ReadEngineId.PARQUET_JAVA){ReadEngineUnavailableException failure=new ReadEngineUnavailableException("Hardwood reader is unavailable");if(input instanceof ChannelInputFile)try{((ChannelInputFile)input).close();}catch(Exception close){failure.addSuppressed(close);}throw failure;}return new CoreParquetArrowReader(allocator,options,projection,input);} }
