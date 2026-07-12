// SPDX-FileCopyrightText: 2026 parquet-arrow-java contributors
// SPDX-License-Identifier: Apache-2.0

plugins {
    `java-library`
    `maven-publish`
    id("me.champeau.jmh") version "0.7.3"
}

java { toolchain { languageVersion.set(JavaLanguageVersion.of(17)) } }

dependencies {
    api(project(":parquet-arrow-core"))
    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.apache.arrow:arrow-compression:19.0.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.apache.arrow:arrow-memory-netty:19.0.0")
    testRuntimeOnly("org.slf4j:slf4j-nop:2.0.18")
    jmh("org.openjdk.jmh:jmh-core:1.37")
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED", "-Dio.netty.tryReflectionSetAccessible=true")
}

publishing {
    // Local publication only; no remote repository is configured.
    repositories { mavenLocal() }
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            pom {
                name.set("parquet-arrow-conformance")
                description.set("Conformance fixtures, logical oracle, and benchmarks for parquet-arrow")
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
