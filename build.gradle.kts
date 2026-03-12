plugins {
    `java`
}

// Target Java 17 for Spark runtime compatibility
java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

val hadoopAwsVersion = "3.4.1"
val deltaSparkVersion = "4.0.1"
val scalaVersion = "2.13"
val sedonaVersion = "1.8.1"
// Must match the Spark version in the base Docker image (quay.io/jupyter/pyspark-notebook)
// DO NOT upgrade without also updating: Dockerfile base image SHA, pyproject.toml pyspark/delta-spark,
// and verifying Sedona has a compatible artifact (as of Feb 2026, Sedona only supports up to Spark 4.0)
val sparkVersion = "4.0.1"

repositories {
    mavenCentral()
}

dependencies {
    runtimeOnly("org.apache.hadoop:hadoop-aws:$hadoopAwsVersion")
    runtimeOnly("io.delta:delta-spark_${scalaVersion}:$deltaSparkVersion")
    // Apache Sedona for geospatial data processing (shaded JAR includes all deps including GeoTools)
    runtimeOnly("org.apache.sedona:sedona-spark-shaded-4.0_${scalaVersion}:$sedonaVersion")

    // Spark Connect JAR for compiling interceptor (uses shaded gRPC under org.sparkproject.connect.grpc)
    // This is compileOnly because Spark provides it at runtime
    compileOnly("org.apache.spark:spark-connect_${scalaVersion}:$sparkVersion")
}

// Copy runtime dependencies to libs/
tasks.create<Copy>("copyLibs") {
    from(configurations.runtimeClasspath)
    into("libs")
}

// Build KBase auth interceptor JAR and copy to libs/
tasks.jar {
    archiveBaseName.set("kbase-spark-auth")
    archiveVersion.set("1.0.0")
    destinationDirectory.set(file("libs"))
}

// Make copyLibs depend on jar so we build the interceptor too
tasks.named("copyLibs") {
    dependsOn(tasks.jar)
}