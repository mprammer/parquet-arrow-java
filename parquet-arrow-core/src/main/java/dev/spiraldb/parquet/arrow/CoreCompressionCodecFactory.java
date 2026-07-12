// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import com.github.luben.zstd.Zstd;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.xerial.snappy.Snappy;

/** Direct JNI/JDK codecs, intentionally independent of parquet's Hadoop codec factory. */
public final class CoreCompressionCodecFactory implements CompressionCodecFactory {
  @Override public BytesInputCompressor getCompressor(CompressionCodecName codec) { return new Compressor(codec); }
  @Override public BytesInputDecompressor getDecompressor(CompressionCodecName codec) { return new Decompressor(codec); }
  @Override public void release() { }
  private static void supported(CompressionCodecName c) throws CodecUnavailableException { if(c!=CompressionCodecName.UNCOMPRESSED&&c!=CompressionCodecName.ZSTD&&c!=CompressionCodecName.SNAPPY&&c!=CompressionCodecName.GZIP) throw new CodecUnavailableException("unsupported codec: "+c); }
  private static final class Compressor implements BytesInputCompressor { private final CompressionCodecName codec; private boolean released; Compressor(CompressionCodecName codec){this.codec=codec;}
    @Override public BytesInput compress(BytesInput input) throws IOException { if(released) throw new IOException("compressor released"); supported(codec); byte[] data=input.toByteArray(); try { switch(codec) { case UNCOMPRESSED:return BytesInput.from(data); case SNAPPY:return BytesInput.from(Snappy.compress(data)); case ZSTD:return BytesInput.from(Zstd.compress(data)); case GZIP: ByteArrayOutputStream out=new ByteArrayOutputStream(); try(GZIPOutputStream gzip=new GZIPOutputStream(out)){gzip.write(data);} return BytesInput.from(out.toByteArray()); default:throw new AssertionError(codec); } } catch(UnsatisfiedLinkError|ExceptionInInitializerError e){throw new CodecUnavailableException("codec unavailable: "+codec,e);} catch(Exception e){if(e instanceof IOException)throw (IOException)e;throw new CodecUnavailableException("cannot compress with "+codec,e);} }
    @Override public CompressionCodecName getCodecName(){return codec;} @Override public void release(){released=true;}
  }
  private static final class Decompressor implements BytesInputDecompressor { private final CompressionCodecName codec; private boolean released; Decompressor(CompressionCodecName codec){this.codec=codec;}
    @Override public BytesInput decompress(BytesInput input,int size) throws IOException { if(released)throw new IOException("decompressor released"); supported(codec); if(size<0)throw new CorruptParquetException("negative uncompressed size"); return BytesInput.from(decode(input.toByteArray(),size)); }
    @Override public void decompress(ByteBuffer input,int compressedSize,ByteBuffer output,int uncompressedSize) throws IOException { if(compressedSize<0||compressedSize>input.remaining()||uncompressedSize<0||uncompressedSize>output.remaining())throw new CorruptParquetException("invalid compressed page bounds"); byte[] in=new byte[compressedSize];input.get(in);byte[] decoded=decode(in,uncompressedSize);output.put(decoded); }
    private byte[] decode(byte[] input,int expected) throws IOException { try { byte[] out; switch(codec) { case UNCOMPRESSED: out=Arrays.copyOf(input,input.length);break; case SNAPPY: out=Snappy.uncompress(input);break; case ZSTD: out=Zstd.decompress(input,expected); if(Zstd.isError(out.length))throw new CorruptParquetException("zstd: "+Zstd.getErrorName(out.length));break; case GZIP: out=gzip(input,expected);break; default:throw new AssertionError(codec); } if(out.length!=expected)throw new CorruptParquetException("decoded "+out.length+" bytes, expected "+expected);return out; } catch(UnsatisfiedLinkError|ExceptionInInitializerError e){throw new CodecUnavailableException("codec unavailable: "+codec,e);} catch(CorruptParquetException e){throw e;} catch(Exception e){throw new CorruptParquetException("corrupt "+codec+" page",e);} }
    private static byte[] gzip(byte[] input,int expected) throws IOException { try(GZIPInputStream gzip=new GZIPInputStream(new ByteArrayInputStream(input)); ByteArrayOutputStream out=new ByteArrayOutputStream(expected)){ byte[] b=new byte[8192];int n;while((n=gzip.read(b))!=-1){if(out.size()+n>expected)throw new CorruptParquetException("gzip exceeds page size");out.write(b,0,n);} return out.toByteArray(); } }
    @Override public void release(){released=true;}
  }
}
