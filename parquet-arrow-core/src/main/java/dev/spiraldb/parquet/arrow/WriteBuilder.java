// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.Objects;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.io.OutputFile;

/** Writer builder. */
public final class WriteBuilder { private final Schema schema; private WriteOptions options=WriteOptions.builder().build(); private InputSchemaCompatibility compatibility=InputSchemaCompatibility.COMPATIBLE; WriteBuilder(Schema schema){this.schema=Objects.requireNonNull(schema);} public WriteBuilder options(WriteOptions v){options=Objects.requireNonNull(v);return this;} public WriteBuilder schemaCompatibility(InputSchemaCompatibility v){compatibility=Objects.requireNonNull(v);return this;} public ParquetArrowWriter build(OutputFile out)throws IOException{return new ParquetArrowWriter(schema,options,compatibility,out);} public ParquetArrowWriter build(Path out)throws IOException{return new ParquetArrowWriter(schema,options,compatibility,NioFiles.output(out));} public ParquetArrowWriter build(SeekableByteChannel out)throws IOException{return build(out,CloseMode.LEAVE_OPEN);} public ParquetArrowWriter build(SeekableByteChannel out,CloseMode mode)throws IOException{return new ParquetArrowWriter(schema,options,compatibility,new ChannelOutputFile(out,mode==CloseMode.CLOSE));} }
