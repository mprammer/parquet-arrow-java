// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/** Channel adapter; each parquet stream shares its channel position and may own the channel. */
class ChannelInputFile implements InputFile, AutoCloseable {
  private final SeekableByteChannel channel;
  private final boolean closeUnderlying;
  private boolean closed;
  ChannelInputFile(SeekableByteChannel channel){this(channel,false);}
  ChannelInputFile(SeekableByteChannel channel, boolean closeUnderlying){this.channel=channel;this.closeUnderlying=closeUnderlying;}
  @Override public long getLength() throws IOException { synchronized(channel){return channel.size();} }
  @Override public SeekableInputStream newStream(){return new Stream(channel);}
  @Override public void close() throws IOException { if(closed)return;closed=true;if(closeUnderlying)channel.close(); }
  private static final class Stream extends SeekableInputStream { private final SeekableByteChannel c; private boolean closed; Stream(SeekableByteChannel c){this.c=c;} private void open()throws IOException{if(closed)throw new IOException("stream closed");}
    @Override public long getPos()throws IOException{synchronized(c){open();return c.position();}} @Override public void seek(long p)throws IOException{synchronized(c){open();c.position(p);}}
    @Override public int read()throws IOException{ByteBuffer b=ByteBuffer.allocate(1);return read(b)==-1?-1:(b.get(0)&255);} @Override public int read(byte[] b,int off,int len)throws IOException{return read(ByteBuffer.wrap(b,off,len));}
    @Override public int read(ByteBuffer b)throws IOException{synchronized(c){open();return c.read(b);}} @Override public void readFully(byte[] b)throws IOException{readFully(b,0,b.length);} @Override public void readFully(byte[] b,int off,int len)throws IOException{readFully(ByteBuffer.wrap(b,off,len));}
    @Override public void readFully(ByteBuffer b)throws IOException{while(b.hasRemaining()){int n=read(b);if(n<0)throw new EOFException();}} @Override public void close() {closed=true;}
  }
}
