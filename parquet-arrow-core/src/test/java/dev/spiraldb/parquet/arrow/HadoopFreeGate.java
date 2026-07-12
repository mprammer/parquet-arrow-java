// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow;

/** Fresh-JVM launcher used by CI to prove the runtime has no Hadoop classes. */
public final class HadoopFreeGate {
  private HadoopFreeGate() {}
  public static void main(String[] args) throws Exception {
    Stage1GateTest test = new Stage1GateTest();
    test.noHadoopAndAllCodecsRoundTrip();
    test.emptyFileAndOwnership();
    System.out.println("Hadoop-free Stage 1 codec gate passed");
  }
}
