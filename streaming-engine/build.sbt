ThisBuild / organization := "io.sentinelaegisforge"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.18"

lazy val root = (project in file("."))
  .settings(
    name := "streaming-engine",
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
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
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
