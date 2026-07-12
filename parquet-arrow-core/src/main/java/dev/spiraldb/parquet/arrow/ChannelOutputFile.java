// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/** Channel adapter. Creation requires an empty channel at offset zero and may own the channel. */
class ChannelOutputFile implements OutputFile, AutoCloseable {
  private final SeekableByteChannel channel;
  private final boolean closeUnderlying;
  private boolean closed;
  ChannelOutputFile(SeekableByteChannel channel){this(channel,false);}
  ChannelOutputFile(SeekableByteChannel channel,boolean closeUnderlying){this.channel=channel;this.closeUnderlying=closeUnderlying;}
  @Override public PositionOutputStream create(long hint)throws IOException{return stream(false);} @Override public PositionOutputStream createOrOverwrite(long hint)throws IOException{return stream(true);} private PositionOutputStream stream(boolean overwrite)throws IOException{synchronized(channel){if(overwrite){channel.truncate(0);channel.position(0);}else if(channel.position()!=0||channel.size()!=0)throw new IOException("output channel must be empty at position zero");return new Stream(channel);}}
  @Override public boolean supportsBlockSize(){return false;} @Override public long defaultBlockSize(){return 0;}
  @Override public void close() throws IOException {if(closed)return;closed=true;if(closeUnderlying)channel.close();}
  private static final class Stream extends PositionOutputStream { private final SeekableByteChannel c; private boolean closed;Stream(SeekableByteChannel c){this.c=c;} @Override public long getPos()throws IOException{synchronized(c){return c.position();}} @Override public void write(int v)throws IOException{write(new byte[]{(byte)v});} @Override public void write(byte[] b,int off,int len)throws IOException{synchronized(c){if(closed)throw new IOException("stream closed");ByteBuffer bb=ByteBuffer.wrap(b,off,len);while(bb.hasRemaining())c.write(bb);}} @Override public void close() {closed=true;}
  }
}
