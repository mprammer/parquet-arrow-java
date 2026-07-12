// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

import java.util.Arrays;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

/** Canonical, logical-type-aware identity for v1 MAP keys. */
final class MapKeyEquality {
  private MapKeyEquality() {}

  static Key key(FieldVector vector, int index, String path) throws InvalidArrowValueException {
    ArrowType type = vector.getField().getType();
    ArrowBuf data = vector.getDataBuffer();
    long start;
    int width;
    switch (type.getTypeID()) {
      case Utf8: case Binary: case LargeUtf8: case LargeBinary:
        if (type.getTypeID() == ArrowType.ArrowTypeID.LargeUtf8 || type.getTypeID() == ArrowType.ArrowTypeID.LargeBinary) {
          start = vector.getOffsetBuffer().getLong((long) index * 8);
          long end = vector.getOffsetBuffer().getLong((long) (index + 1) * 8);
          width = checkedWidth(start, end, path);
        } else {
          start = vector.getOffsetBuffer().getInt((long) index * 4);
          long end = vector.getOffsetBuffer().getInt((long) (index + 1) * 4);
          width = checkedWidth(start, end, path);
        }
        break;
      case Bool:
        return new Key(type.toString(), new byte[] {(byte) ((data.getByte(index >>> 3) & (1 << (index & 7))) == 0 ? 0 : 1)});
      case Int:
        start = (long) index * ((ArrowType.Int) type).getBitWidth() / 8;
        width = ((ArrowType.Int) type).getBitWidth() / 8;
        break;
      case Date:
        width = ((ArrowType.Date) type).getUnit() == DateUnit.DAY ? 4 : 8;
        start = (long) index * width;
        break;
      case Time:
        width = ((ArrowType.Time) type).getBitWidth() / 8;
        start = (long) index * width;
        break;
      case Timestamp: case Duration:
        start = (long) index * 8; width = 8; break;
      case FixedSizeBinary:
        width = ((ArrowType.FixedSizeBinary) type).getByteWidth(); start = (long) index * width; break;
      case Decimal:
        width = ((ArrowType.Decimal) type).getBitWidth() / 8; start = (long) index * width; break;
      default:
        throw new InvalidArrowValueException(path + ": unsupported map-key type " + type);
    }
    if (start < 0 || start + width > data.capacity())
      throw new InvalidArrowValueException(path + ": truncated map-key buffer");
    byte[] bytes = new byte[width];
    for (int i = 0; i < width; i++) bytes[i] = data.getByte(start + i);
    return new Key(type.toString(), bytes);
  }

  private static int checkedWidth(long start, long end, String path) throws InvalidArrowValueException {
    if (start < 0 || end < start || end - start > Integer.MAX_VALUE)
      throw new InvalidArrowValueException(path + ": invalid map-key offsets");
    return (int) (end - start);
  }

  static final class Key {
    private final String type;
    private final byte[] bytes;
    Key(String type, byte[] bytes) { this.type = type; this.bytes = bytes; }
    @Override public boolean equals(Object other) {
      return other instanceof Key && type.equals(((Key) other).type) && Arrays.equals(bytes, ((Key) other).bytes);
    }
    @Override public int hashCode() { return 31 * type.hashCode() + Arrays.hashCode(bytes); }
  }
}
