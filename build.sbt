ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "stream-zip"
  )

libraryDependencies += "co.fs2" %% "fs2-core" % "3.10.2"
libraryDependencies += "co.fs2" %% "fs2-io" % "3.10.2"
libraryDependencies += "org.typelevel" %% "cats-effect" % "3.5.4"