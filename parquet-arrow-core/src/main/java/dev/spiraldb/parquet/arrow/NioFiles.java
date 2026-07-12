// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;

/** NIO-backed parquet endpoint helpers. */
final class NioFiles { private NioFiles(){} static InputFile input(Path p)throws IOException{SeekableByteChannel c=Files.newByteChannel(p,StandardOpenOption.READ);return new ChannelInputFile(c,true);} static OutputFile output(Path p)throws IOException{SeekableByteChannel c=Files.newByteChannel(p,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE);return new ChannelOutputFile(c,true);} }
