plugins {
    `java`
}

val hadoopAwsVersion = "3.4.1"
val deltaSparkVersion = "4.0.0"
val scalaVersion = "2.13"



repositories {
    mavenCentral()
}

dependencies {
    runtimeOnly("org.apache.hadoop:hadoop-aws:$hadoopAwsVersion")
    runtimeOnly("io.delta:delta-spark_${scalaVersion}:$deltaSparkVersion")
}


tasks.create<Copy>("copyLibs") {
    from(configurations.runtimeClasspath)
    into("libs")
}