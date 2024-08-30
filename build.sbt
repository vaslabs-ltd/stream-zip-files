ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "stream-zip"
  )
  .aggregate(
    `zip-partitioner`,
    `dynamo-zip-store`,
    `s3-zip-store`
  )


lazy val `zip-partitioner` = (project in file("zip-partitioner"))
  .settings(
    name := "zip-partitioner"
  )
  .settings(
    libraryDependencies ++= Seq(
        "eu.timepit" %% "refined" % "0.11.2",
        "co.fs2" %% "fs2-core" % "3.10.2",
        "co.fs2" %% "fs2-io" % "3.10.2",
        "org.typelevel" %% "cats-effect" % "3.5.4",
        "org.specs2" %% "specs2-core" % "4.20.7" % "test"
      )
  )

lazy val `dynamo-zip-store` = (project in file("dynamo-zip-store"))
  .settings(
    name := "dynamo-zip-store"
  )
  .dependsOn(`zip-partitioner`, `zip-partitioner-store-test` % Test)
  .settings(
    libraryDependencies ++= Seq(
      "software.amazon.awssdk" % "dynamodb" % "2.26.25",
      "org.specs2" %% "specs2-core" % "4.20.7" % "test"
    )
  )

lazy val `s3-zip-store` = (project in file("s3-zip-store"))
  .settings(
    name := "s3-zip-store"
  )
  .dependsOn(`zip-partitioner`, `zip-partitioner-store-test` % Test)
  .settings(
    libraryDependencies ++= Seq(
      "io.laserdisc" %% "fs2-aws-s3" % "6.1.3",
      "org.specs2" %% "specs2-core" % "4.20.7" % "test",
    )
  )

lazy val `zip-partitioner-store-test` = (project in file("zip-partitioner-store-test"))
  .settings(
    name := "zip-partitioner-store-test"
  ).dependsOn(`zip-partitioner`)
  .settings(
    libraryDependencies ++= Seq(
      "org.specs2" %% "specs2-core" % "4.20.7" % "test"
    )
  )

