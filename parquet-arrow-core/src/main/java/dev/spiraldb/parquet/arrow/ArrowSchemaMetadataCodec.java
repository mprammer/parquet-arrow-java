// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Base64;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.MetadataVersion;
import org.apache.arrow.vector.types.pojo.Schema;

/** Bounded codec for conventional {@code ARROW:schema} footer metadata. */
public final class ArrowSchemaMetadataCodec {
  public static final String KEY = "ARROW:schema";
  public static final int MAX_ENCODED_BYTES = 8 * 1024 * 1024;
  public static final int MAX_DECODED_BYTES = 6 * 1024 * 1024;
  public String encode(Schema schema) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    // Arrow Java 19 defines DEFAULT as the current V5 metadata version.
    MessageSerializer.serialize(new WriteChannel(Channels.newChannel(bytes)), schema, new IpcOption(false, MetadataVersion.DEFAULT));
    byte[] raw = bytes.toByteArray();
    if (raw.length > MAX_DECODED_BYTES) throw new MetadataLimitException("ARROW:schema decoded size exceeds 6 MiB");
    String encoded = Base64.getEncoder().encodeToString(raw);
    if (encoded.length() > MAX_ENCODED_BYTES) throw new MetadataLimitException("ARROW:schema encoded size exceeds 8 MiB");
    return encoded;
  }
  public Schema decode(String encoded) throws IOException {
    if (encoded == null) throw new InvalidParquetFileException("missing ARROW:schema");
    if (encoded.length() > MAX_ENCODED_BYTES) throw new MetadataLimitException("ARROW:schema encoded size exceeds 8 MiB");
    int decodedBound = decodedBound(encoded.length());
    if (decodedBound > MAX_DECODED_BYTES) throw new MetadataLimitException("ARROW:schema decoded size exceeds 6 MiB");
    final byte[] raw;
    try { raw = Base64.getDecoder().decode(encoded); }
    catch (IllegalArgumentException malformed) { throw new InvalidParquetFileException("malformed ARROW:schema base64", malformed); }
    if (raw.length > MAX_DECODED_BYTES) throw new MetadataLimitException("ARROW:schema decoded size exceeds 6 MiB");
    try {
      ByteArrayInputStream input = new ByteArrayInputStream(raw);
      Schema schema = MessageSerializer.deserializeSchema(new ReadChannel(Channels.newChannel(input)));
      if (input.available() != 0) throw new InvalidParquetFileException("ARROW:schema must contain exactly one schema message with no body");
      int fieldCount = validateDepth(schema, 1, 0);
      if (fieldCount > 10_000) throw new MetadataLimitException("ARROW:schema has more than 10000 fields");
      try { ArrowSchemaToParquet.toParquet(schema); }
      catch (UnsupportedParquetTypeException unsupported) { throw new InvalidParquetFileException("ARROW:schema contains a type outside the v1 contract", unsupported); }
      return schema;
    } catch (ParquetArrowException failure) { throw failure; }
    catch (RuntimeException | IOException malformed) { throw new InvalidParquetFileException("malformed ARROW:schema IPC message", malformed); }
  }
  private static int decodedBound(int encodedLength) { return (int) Math.min(Integer.MAX_VALUE, ((long) encodedLength + 3L) / 4L * 3L); }
  private static int validateDepth(Schema schema, int depth, int count) throws MetadataLimitException {
    for (org.apache.arrow.vector.types.pojo.Field field : schema.getFields()) count = validateDepth(field, depth, count);
    return count;
  }
  private static int validateDepth(org.apache.arrow.vector.types.pojo.Field field, int depth, int count) throws MetadataLimitException {
    if (depth > 64) throw new MetadataLimitException("ARROW:schema nesting depth exceeds 64");
    if (++count > 10_000) throw new MetadataLimitException("ARROW:schema has more than 10000 fields");
    for (org.apache.arrow.vector.types.pojo.Field child : field.getChildren()) count = validateDepth(child, depth + 1, count);
    return count;
  }
}
