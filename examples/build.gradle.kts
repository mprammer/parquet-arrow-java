// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

plugins { application }

java { toolchain { languageVersion.set(JavaLanguageVersion.of(17)) } }

dependencies {
    // This is intentionally a downstream project dependency: its source uses
    // only the public core API, as an external consumer would.
    implementation(project(":parquet-arrow-core"))
    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application { mainClass.set("dev.spiraldb.parquet.arrow.examples.RoundTripExample") }

tasks.withType<JavaExec>().configureEach {
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED", "-Dio.netty.tryReflectionSetAccessible=true")
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED", "-Dio.netty.tryReflectionSetAccessible=true")
}
