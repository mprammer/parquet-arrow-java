// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0
package dev.spiraldb.parquet.arrow;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

/**
 * An Arrow {@code arrow.parquet.variant} struct is written as a Parquet VARIANT group, and a
 * VARIANT group reads back as that struct with the extension: the logical type crosses the bridge
 * both ways instead of surviving only in the advisory {@code ARROW:schema} hint.
 */
class VariantLogicalTypeTest {
  private static final byte[] METADATA = {0x01, 0x00, 0x00};
  private static final byte[] VALUE = {0x0c, 0x01};

  private static Field binary(String name, boolean nullable) {
    return new Field(name, new FieldType(nullable, ArrowType.Binary.INSTANCE, null), List.of());
  }

  private static Field variant(String name, Map<String, String> metadata, Field... children) {
    return new Field(name, new FieldType(true, ArrowType.Struct.INSTANCE, null, metadata), List.of(children));
  }

  private static final Map<String, String> EXTENSION =
      Map.of(VariantExtension.NAME_KEY, VariantExtension.NAME, "__variant_type", "1");

  private static Path write(Schema schema) throws Exception {
    Path file = Files.createTempFile("pa-variant", ".parquet");
    Files.delete(file);
    try (RootAllocator allocator = new RootAllocator();
         VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      StructVector v = (StructVector) root.getVector("v");
      v.setIndexDefined(0);
      ((VarBinaryVector) v.getChild("metadata")).setSafe(0, METADATA);
      ((VarBinaryVector) v.getChild("value")).setSafe(0, VALUE);
      v.setNull(1);
      ((IntVector) root.getVector("x")).setSafe(0, 1);
      ((IntVector) root.getVector("x")).setSafe(1, 2);
      root.setRowCount(2);
      try (ParquetArrowWriter writer = ParquetArrow.writer(schema).build(file)) {
        writer.writeBatch(root);
        writer.finish();
      }
    }
    return file;
  }

  private static MessageType footerSchema(Path file) throws Exception {
    ParquetReadOptions options = ParquetReadOptions.builder(new PlainParquetConfiguration())
        .withCodecFactory(new CoreCompressionCodecFactory()).build();
    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file), options)) {
      return reader.getFileMetaData().getSchema();
    }
  }

  @Test void variantStructIsWrittenAsVariantGroupAndReadBackWithTheExtension() throws Exception {
    Schema schema = new Schema(List.of(
        variant("v", EXTENSION, binary("metadata", true), binary("value", true)),
        new Field("x", FieldType.nullable(new ArrowType.Int(32, true)), List.of())));
    Path file = write(schema);
    try {
      Type group = footerSchema(file).getType("v");
      assertInstanceOf(LogicalTypeAnnotation.VariantLogicalTypeAnnotation.class, group.getLogicalTypeAnnotation());
      assertEquals(VariantExtension.SPEC_VERSION,
          ((LogicalTypeAnnotation.VariantLogicalTypeAnnotation) group.getLogicalTypeAnnotation()).getSpecVersion());

      try (RootAllocator allocator = new RootAllocator();
           ParquetArrowReader reader = ParquetArrow.reader(allocator).build(file)) {
        assertTrue(reader.loadNextBatch());
        VectorSchemaRoot out = reader.getVectorSchemaRoot();
        Field field = out.getSchema().findField("v");
        // The extension name, and the rest of the writer's field metadata through ARROW:schema.
        assertEquals(EXTENSION, field.getMetadata());
        StructVector v = (StructVector) out.getVector("v");
        assertArrayEquals(METADATA, ((VarBinaryVector) v.getChild("metadata")).get(0));
        assertArrayEquals(VALUE, ((VarBinaryVector) v.getChild("value")).get(0));
        assertTrue(v.isNull(1));
        assertFalse(reader.loadNextBatch());
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test void aPlainStructStaysAPlainGroup() throws Exception {
    Schema schema = new Schema(List.of(
        variant("v", Map.of("other", "x"), binary("metadata", true), binary("value", true)),
        new Field("x", FieldType.nullable(new ArrowType.Int(32, true)), List.of())));
    Path file = write(schema);
    try {
      assertNull(footerSchema(file).getType("v").getLogicalTypeAnnotation());
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test void aVariantGroupAloneNamesTheExtension() throws Exception {
    // No ARROW:schema hint at all: the Parquet logical type is what the reader maps.
    MessageType parquet = Types.buildMessage()
        .optionalGroup().as(LogicalTypeAnnotation.variantType((byte) 1))
            .required(PrimitiveTypeName.BINARY).named("metadata")
            .optional(PrimitiveTypeName.BINARY).named("value")
            .optionalGroup().optional(PrimitiveTypeName.INT64).named("n").named("typed_value")
            .named("v")
        .named("arrow");
    Field field = ParquetToArrow.fromParquet(parquet).findField("v");
    assertEquals(ArrowType.Struct.INSTANCE, field.getType());
    assertEquals(Map.of(VariantExtension.NAME_KEY, VariantExtension.NAME), field.getMetadata());
    assertEquals(List.of("metadata", "value", "typed_value"),
        field.getChildren().stream().map(Field::getName).toList());
  }

  @Test void aVariantGroupThatIsNotTheSpecsShapeIsRejected() {
    MessageType noMetadata = Types.buildMessage()
        .optionalGroup().as(LogicalTypeAnnotation.variantType((byte) 1))
            .required(PrimitiveTypeName.BINARY).named("value")
            .named("v")
        .named("arrow");
    assertThrows(UnsupportedParquetTypeException.class, () -> ParquetToArrow.fromParquet(noMetadata));
    MessageType extra = Types.buildMessage()
        .optionalGroup().as(LogicalTypeAnnotation.variantType((byte) 1))
            .required(PrimitiveTypeName.BINARY).named("metadata")
            .required(PrimitiveTypeName.BINARY).named("value")
            .required(PrimitiveTypeName.INT32).named("other")
            .named("v")
        .named("arrow");
    assertThrows(UnsupportedParquetTypeException.class, () -> ParquetToArrow.fromParquet(extra));
  }

  @Test void variantStorageThatIsNotAVariantGroupIsRefusedBeforeWriting() {
    Schema notStruct = new Schema(List.of(
        new Field("v", new FieldType(true, ArrowType.Binary.INSTANCE, null, EXTENSION), List.of())));
    assertThrows(UnsupportedParquetTypeException.class, () -> ArrowSchemaToParquet.toParquet(notStruct));
    Schema textMetadata = new Schema(List.of(variant("v", EXTENSION,
        new Field("metadata", FieldType.nullable(ArrowType.Utf8.INSTANCE), List.of()), binary("value", true))));
    assertThrows(UnsupportedParquetTypeException.class, () -> ArrowSchemaToParquet.toParquet(textMetadata));
    Schema noValue = new Schema(List.of(variant("v", EXTENSION, binary("metadata", false))));
    assertThrows(UnsupportedParquetTypeException.class, () -> ArrowSchemaToParquet.toParquet(noValue));
  }
}
