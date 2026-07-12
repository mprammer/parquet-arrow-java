// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow.examples;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import dev.spiraldb.parquet.arrow.ParquetArrow;
import dev.spiraldb.parquet.arrow.ParquetArrowReader;
import dev.spiraldb.parquet.arrow.ParquetArrowWriter;

/** A small Java-17 consumer that writes and reads the same Parquet file. */
public final class RoundTripExample {
  private RoundTripExample() {}

  public static void main(String[] args) throws Exception {
    Path file = args.length == 0 ? Path.of("build/example.parquet") : Path.of(args[0]);
    if (file.getParent() != null) {
      Files.createDirectories(file.getParent());
    }

    Schema schema = new Schema(List.of(
        new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), List.of()),
        new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), List.of())));

    try (RootAllocator allocator = new RootAllocator();
         VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      root.allocateNew();
      ((IntVector) root.getVector("id")).setSafe(0, 1);
      ((IntVector) root.getVector("id")).setSafe(1, 2);
      ((VarCharVector) root.getVector("name")).setSafe(
          0, "alpha".getBytes(StandardCharsets.UTF_8));
      ((VarCharVector) root.getVector("name")).setSafe(
          1, "beta".getBytes(StandardCharsets.UTF_8));
      root.setRowCount(2);

      java.nio.file.Files.deleteIfExists(file); // writer is create-new; make the example re-runnable
      try (ParquetArrowWriter writer = ParquetArrow.writer(schema).build(file)) {
        writer.writeBatch(root);
        writer.finish();
      }

      System.out.println("wrote " + file);
      try (ParquetArrowReader reader = ParquetArrow.reader(allocator).build(file)) {
        while (reader.loadNextBatch()) {
          VectorSchemaRoot batch = reader.getVectorSchemaRoot();
          IntVector ids = (IntVector) batch.getVector("id");
          VarCharVector names = (VarCharVector) batch.getVector("name");
          for (int row = 0; row < batch.getRowCount(); row++) {
            System.out.println("id=" + ids.get(row) + ", name=" + names.getObject(row));
          }
        }
      }
    }
  }
}
