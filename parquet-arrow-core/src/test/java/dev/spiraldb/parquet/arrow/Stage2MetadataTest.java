// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

class Stage2MetadataTest {
  private static Field field(String name, ArrowType type) { return new Field(name, FieldType.nullable(type), List.of()); }
  @Test void arrowSchemaCodecRoundTripsAndEnforcesBounds() throws Exception {
    Schema schema = new Schema(List.of(field("a", ArrowType.Utf8.INSTANCE)));
    ArrowSchemaMetadataCodec codec = new ArrowSchemaMetadataCodec();
    assertEquals(schema, codec.decode(codec.encode(schema)));
    assertThrows(InvalidParquetFileException.class, () -> codec.decode("!not-base64!"));
    assertThrows(MetadataLimitException.class, () -> codec.decode("A".repeat(ArrowSchemaMetadataCodec.MAX_ENCODED_BYTES + 1)));
  }
}
