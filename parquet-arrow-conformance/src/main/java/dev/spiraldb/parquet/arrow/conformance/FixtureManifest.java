// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

package dev.spiraldb.parquet.arrow.conformance;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** Immutable, versioned fixture manifest.  Its bytes, not a JVM object graph, are hashed. */
public record FixtureManifest(int version, String generator, String canonicalJson, String sha256) {
  public FixtureManifest {
    if (version != 1) throw new ConformanceFailure("FIXTURE_VERSION", "unsupported manifest version " + version);
    Objects.requireNonNull(generator, "generator");
    Objects.requireNonNull(canonicalJson, "canonicalJson");
    Objects.requireNonNull(sha256, "sha256");
    if (!sha256.equals(sha256(canonicalJson)))
      throw new ConformanceFailure("FIXTURE_HASH", "manifest SHA-256 does not match canonical JSON");
  }

  public static String sha256(String text) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException impossible) {
      throw new AssertionError(impossible);
    }
  }
}
