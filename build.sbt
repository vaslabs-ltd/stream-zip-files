ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "stream-zip"
  )
  .aggregate(zipPartitioner, dynamoZipStore)

// from a list of files to stream fileArchive
lazy val zipPartitioner = (project in file("zip-partitioner"))
  .settings(
    name := "zip-partitioner"
  )
  .settings(
    libraryDependencies += "co.fs2" %% "fs2-core" % "3.10.2",
    libraryDependencies += "co.fs2" %% "fs2-io" % "3.10.2",
    libraryDependencies += "org.typelevel" %% "cats-effect" % "3.5.4",
    libraryDependencies += "org.specs2" %% "specs2-core" % "4.20.7" % "test"
  )

// list fileArchive save to dynamoDB
// list dynamo hashkeys -> list fileArchive
// list dynamo haskkeys -> stream bytes (zip file)
// single hashkey -> stream of uncompress bytes (nomral file )
lazy val dynamoZipStore = (project in file("dynamo-zip-store"))
  .settings(
    name := "dynamo-zip-store"
  )
  .dependsOn(zipPartitioner)
  .settings(
    // https://mvnrepository.com/artifact/software.amazon.awssdk/aws-sdk-java
    libraryDependencies += "software.amazon.awssdk" % "aws-sdk-java" % "2.26.25" % "provided",
    // https://mvnrepository.com/artifact/software.amazon.awssdk/dynamodb
    libraryDependencies += "software.amazon.awssdk" % "dynamodb" % "2.26.25"

)