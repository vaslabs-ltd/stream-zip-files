ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "stream-zip"
  )
  .dependsOn(zipPartitioner, dynamoZipStore)

libraryDependencies += "co.fs2" %% "fs2-core" % "3.10.2"
libraryDependencies += "co.fs2" %% "fs2-io" % "3.10.2"
libraryDependencies += "org.typelevel" %% "cats-effect" % "3.5.4"
libraryDependencies += "org.specs2" %% "specs2-core" % "4.20.7" % "test"
// https://mvnrepository.com/artifact/net.lingala.zip4j/zip4j
libraryDependencies += "net.lingala.zip4j" % "zip4j" % "2.11.5"


libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.12.732",
  "commons-io" % "commons-io" % "2.16.1"
)


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
    libraryDependencies += "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.12.732"
  )