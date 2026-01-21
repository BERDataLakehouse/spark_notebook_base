plugins {
    `java`
}

val hadoopAwsVersion = "3.4.1"
val deltaSparkVersion = "4.0.0"
val scalaVersion = "2.13"
val sedonaVersion = "1.8.0"

repositories {
    mavenCentral()
}

dependencies {
    runtimeOnly("org.apache.hadoop:hadoop-aws:$hadoopAwsVersion")
    runtimeOnly("io.delta:delta-spark_${scalaVersion}:$deltaSparkVersion")
    // Apache Sedona for geospatial data processing (shaded JAR includes all deps including GeoTools)
    runtimeOnly("org.apache.sedona:sedona-spark-shaded-4.0_${scalaVersion}:$sedonaVersion")
}

// Copy runtime dependencies to libs/
tasks.create<Copy>("copyLibs") {
    from(configurations.runtimeClasspath)
    into("libs")
}