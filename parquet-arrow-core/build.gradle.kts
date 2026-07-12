// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

import java.util.zip.ZipFile

plugins {
    `java-library`
    `maven-publish`
}

java { toolchain { languageVersion.set(JavaLanguageVersion.of(17)) } }

dependencyLocking {
    // The committed lock state is the release-candidate dependency manifest.
    lockAllConfigurations()
}

dependencies {
    api("org.apache.arrow:arrow-vector:19.0.0")
    api("org.apache.arrow:arrow-memory-core:19.0.0")
    api("org.apache.parquet:parquet-hadoop:1.17.1") {
        exclude(group = "org.apache.hadoop")
    }
    implementation("com.github.luben:zstd-jni:1.5.7-6")
    implementation("org.xerial.snappy:snappy-java:1.1.10.7")
    runtimeOnly("org.apache.arrow:arrow-memory-netty:19.0.0")
    runtimeOnly("org.slf4j:slf4j-nop:2.0.18")
    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED", "-Dio.netty.tryReflectionSetAccessible=true")
}

tasks.register("assertHadoopFreeRuntime") {
    doLast {
        val hadoop = configurations.runtimeClasspath.get().files.flatMap { file ->
            if (file.isDirectory) file.walkTopDown().filter { it.path.contains("org/apache/hadoop") }.toList()
            else emptyList()
        }
        check(hadoop.isEmpty()) { "org.apache.hadoop classes found on core runtimeClasspath: $hadoop" }
        val jars = configurations.runtimeClasspath.get().files.filter { it.extension == "jar" }
        val matches = jars.filter { jar ->
            ZipFile(jar).use { zip ->
                val entries = zip.entries()
                var found = false
                while (entries.hasMoreElements()) {
                    if (entries.nextElement().name.startsWith("org/apache/hadoop/")) { found = true; break }
                }
                found
            }
        }
        check(matches.isEmpty()) { "org.apache.hadoop classes found on core runtimeClasspath: $matches" }
    }
}
tasks.named("check") { dependsOn("assertHadoopFreeRuntime") }

publishing {
    // This repository deliberately has no remote publishing target. The
    // publishToMavenLocal task is the only supported publication workflow.
    repositories { mavenLocal() }
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            pom {
                name.set("parquet-arrow-core")
                description.set("Pure-JVM, Hadoop-free bidirectional Apache Arrow and Parquet bridge")
                url.set("https://github.com/mprammer/parquet-arrow-java")
                licenses {
                    license {
                        name.set("The Apache Software License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                        distribution.set("repo")
                    }
                }
                scm {
                    connection.set("scm:git:https://github.com/mprammer/parquet-arrow-java.git")
                    developerConnection.set("scm:git:ssh://git@github.com/mprammer/parquet-arrow-java.git")
                    url.set("https://github.com/mprammer/parquet-arrow-java")
                }
            }
        }
    }
}
