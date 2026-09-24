ThisBuild / organization := "io.sentinelaegisforge"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.18"

lazy val root = (project in file("."))
  .settings(
    name := "streaming-engine",
    resolvers += "Confluent" at "https://packages.confluent.io/maven/",
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-encoding",
      "UTF-8",
      "-release:21"
    ),
    javacOptions ++= Seq("--release", "21", "-encoding", "UTF-8"),
    libraryDependencies ++= Seq(
      "org.apache.avro" % "avro" % "1.12.2",
      "org.apache.kafka" % "kafka-clients" % "4.3.1",
      ("io.confluent" % "kafka-avro-serializer" % "8.3.2")
        .exclude("org.apache.kafka", "kafka-clients"),
      "org.apache.spark" %% "spark-sql" % "4.2.0",
      ("org.apache.spark" %% "spark-sql-kafka-0-10" % "4.2.0")
        // Keep the existing, directly pinned Apache Kafka client used by Module A.
        .exclude("org.apache.kafka", "kafka-clients"),
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    dependencyOverrides ++= Seq(
      // Spark 4.2's Scala module requires the Jackson 2.21 line at runtime.
      "com.fasterxml.jackson.core" % "jackson-annotations" % "2.21",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.21.2",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.21.2",
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-csv" % "2.21.2",
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % "2.21.2"
    ),
    Compile / resourceGenerators += Def.task {
      val source =
        baseDirectory.value.getParentFile / "contracts" / "events" / "transaction-event-v1.avsc"
      val target =
        (Compile / resourceManaged).value / "contracts" / "events" / "transaction-event-v1.avsc"
      IO.copyFile(source, target)
      Seq(target)
    }.taskValue,
    Test / parallelExecution := false
  )
