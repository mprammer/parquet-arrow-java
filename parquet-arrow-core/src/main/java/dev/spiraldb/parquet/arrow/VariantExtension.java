// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import java.util.Map;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * The Arrow canonical extension {@code arrow.parquet.variant}: a Parquet VARIANT column in Arrow,
 * carried as its storage struct ({@code metadata} and {@code value} binaries, plus
 * {@code typed_value} when shredded) with the extension name in the field metadata. The writer
 * annotates such a struct with Parquet's VARIANT logical type; the reader attaches the extension
 * to a VARIANT-annotated group.
 */
public final class VariantExtension {
  private VariantExtension() {}

  /** The canonical extension name. */
  public static final String NAME = "arrow.parquet.variant";
  /** Arrow's field-metadata key for an extension name. */
  public static final String NAME_KEY = "ARROW:extension:name";
  /** The Variant binary encoding version the writer declares. */
  static final byte SPEC_VERSION = 1;

  /** Whether {@code field} carries the extension. */
  public static boolean isVariant(Field field) {
    Map<String, String> metadata = field.getMetadata();
    return metadata != null && NAME.equals(metadata.get(NAME_KEY));
  }

  /**
   * The field metadata the reader gives a VARIANT-annotated group: the extension name alone. The
   * extension has no parameters, and an absent {@code ARROW:extension:metadata} reads as empty.
   */
  static Map<String, String> fieldMetadata() {
    return Map.of(NAME_KEY, NAME);
  }

  /**
   * Refuses an extension field whose storage is not a VARIANT group: a struct holding a binary
   * {@code metadata}, a binary {@code value} and/or a {@code typed_value}, and nothing else.
   */
  static void requireStorage(Field field, String path) throws UnsupportedParquetTypeException {
    if (field.getType().getTypeID() != ArrowType.ArrowTypeID.Struct)
      reject(path, NAME + " storage must be a struct, not " + field.getType());
    boolean metadata = false, values = false;
    for (Field child : field.getChildren()) {
      switch (child.getName()) {
        case "metadata":
          if (!binary(child.getType())) reject(path + ".metadata", "VARIANT metadata must be binary");
          metadata = true;
          break;
        case "value":
          if (!binary(child.getType())) reject(path + ".value", "VARIANT value must be binary");
          values = true;
          break;
        case "typed_value":
          values = true;
          break;
        default:
          reject(path + "." + child.getName(), "a VARIANT group holds only metadata, value and typed_value");
      }
    }
    if (!metadata) reject(path, "a VARIANT group needs a metadata field");
    if (!values) reject(path, "a VARIANT group needs a value or typed_value field");
  }

  private static boolean binary(ArrowType type) {
    switch (type.getTypeID()) {
      case Binary: case LargeBinary: case BinaryView: return true;
      default: return false;
    }
  }

  private static void reject(String path, String reason) throws UnsupportedParquetTypeException {
    throw new UnsupportedParquetTypeException(path + ": " + reason);
  }
}
