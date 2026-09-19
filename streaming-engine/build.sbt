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
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test,
    Test / parallelExecution := false
  )
