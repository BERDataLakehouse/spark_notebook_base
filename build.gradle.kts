plugins {
    `java`
}

// Define all versions as variables for easy updates
// NOTE: ensure Delta Spark jar version matches python pip delta-spark version specified in the Pipfile
// NOTE:  hadoopAwsVersion=="3.4.1" doesn't yet work with hive 4.1

val hadoopAwsVersion = "3.3.4"
val deltaSparkVersion = "4.0.0"
val scalaVersion = "2.13"
val postgresVersion = "42.7.7"
val sparkXmlVersion = "0.18.0"
val hiveVersion = "4.0.0"


repositories {
    mavenCentral()
}

dependencies {
    // AWS support for Hadoop S3A filesystem
    runtimeOnly("org.apache.hadoop:hadoop-aws:$hadoopAwsVersion")

    // Delta Lake support
    runtimeOnly("io.delta:delta-spark_${scalaVersion}:$deltaSparkVersion")

    // PostgreSQL JDBC Driver
    runtimeOnly("org.postgresql:postgresql:$postgresVersion")

    // Essential Hive 4.0.0 client libraries for metastore connectivity
    runtimeOnly("org.apache.hive:hive-metastore:$hiveVersion")
    runtimeOnly("org.apache.hive:hive-common:$hiveVersion")
    runtimeOnly("org.apache.hive:hive-serde:$hiveVersion")
    runtimeOnly("org.apache.spark:spark-hive_${scalaVersion}:4.0.0")

    // Spark-XML connector
    runtimeOnly("com.databricks:spark-xml_${scalaVersion}:$sparkXmlVersion")
}

// Custom task to copy all resolved JARs into one directory.
// This remains unchanged and works perfectly for this setup.
tasks.create<Copy>("copyLibs") {
    from(configurations.runtimeClasspath)
    into("libs")
}