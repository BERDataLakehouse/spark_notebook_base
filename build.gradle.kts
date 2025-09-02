plugins {
    `java`
}

// Define all versions as variables for easy updates
// NOTE: ensure Delta Spark jar version matches python pip delta-spark version specified in the Pipfile
val hadoopAwsVersion = "3.4.1"
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

    // Spark-Redis connector
    // runtimeOnly("com.redislabs:spark-redis_${scalaVersion}:$sparkRedisVersion") deprecated

    // Hive 4.0.0 client libraries for metastore compatibility
    runtimeOnly("org.apache.hive:hive-metastore:$hiveVersion")
    runtimeOnly("org.apache.hive:hive-common:$hiveVersion")
    runtimeOnly("org.apache.hive:hive-serde:$hiveVersion")
    runtimeOnly("org.apache.hive:hive-exec:$hiveVersion")
    runtimeOnly("org.apache.hive:hive-jdbc:$hiveVersion")
    runtimeOnly("org.apache.hive:hive-service-rpc:$hiveVersion")


    // Spark-XML connector (no longer needed in 4.0.0)
    runtimeOnly("com.databricks:spark-xml_${scalaVersion}:$sparkXmlVersion")
}

// Custom task to copy all resolved JARs into one directory.
// This remains unchanged and works perfectly for this setup.
tasks.create<Copy>("copyLibs") {
    from(configurations.runtimeClasspath)
    into("libs")
}