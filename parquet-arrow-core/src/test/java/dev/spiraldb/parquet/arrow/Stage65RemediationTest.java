// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Base64;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/** Regression coverage for the Stage 6.5 untrusted-input and byte-equality fixes. */
class Stage65RemediationTest {
  @Test void binaryMapKeyIdentityUsesContentNotArrayIdentity() throws Exception {
    try (RootAllocator allocator = new RootAllocator(); VarBinaryVector keys = new VarBinaryVector("k", allocator)) {
      keys.allocateNew();
      keys.setSafe(0, new byte[] {0x61});
      keys.setSafe(1, new byte[] {0x61});
      MapKeyEquality.Key first = MapKeyEquality.key(keys, 0, "m.key");
      MapKeyEquality.Key second = MapKeyEquality.key(keys, 1, "m.key");
      assertEquals(first, second);
    }
  }

  @Test void arrowSchemaRejectsTrailingIpcData() throws Exception {
    Schema schema = new Schema(java.util.List.of(new Field("x", FieldType.nullable(new ArrowType.Int(32, true)), java.util.List.of())));
    ArrowSchemaMetadataCodec codec = new ArrowSchemaMetadataCodec();
    String valid = codec.encode(schema);
    byte[] raw = Base64.getDecoder().decode(valid);
    byte[] trailing = java.util.Arrays.copyOf(raw, raw.length + 4);
    String malformed = Base64.getEncoder().encodeToString(trailing);
    assertThrows(InvalidParquetFileException.class, () -> codec.decode(malformed));
  }
}
