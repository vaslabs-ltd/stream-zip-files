package dynamo_zip_store_test

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import dynamo_zip_store.DynamoZipStore.{downloadFilesFromDynamoDB, uploadFilesToDynamoDB}
import org.specs2.mutable.Specification
import org.specs2.specification.Scope
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.{DynamoDbAsyncClient, DynamoDbClient}
import zip_partitioner.FileArchive
import zip_partitioner.ZipPartitioner.createStreamArchive
import fs2.Stream

import java.lang.Thread.sleep
import java.net.URI

object DynamoZipStoreSpec extends Specification{

  "DynamoZipStore" should {
    "uploadFilesToDynamoDB and then download them must be the same" in new LocalScope {

      val toUpload: Stream[IO, FileArchive] = Stream.emits(List(FileArchive("test", "test"))).covary[IO]

      uploadFilesToDynamoDB(dynamoDbClient, toUpload, "myDynamoTable", "filename", "data")

      val downloaded = downloadFilesFromDynamoDB(dynamoDbClient, List("test"), "myDynamoTable", "filename", "data")
        .unsafeRunSync()

      println("downloads", downloaded.compile.toList.unsafeRunSync())
      println("uploads", toUpload.compile.toList.unsafeRunSync())

      downloaded must_== toUpload
    }
  }

  "DynamoZipStore" should {
    "full circle test" in new LocalScope {
      val filePaths = List("zip-partitioner/src/files/file1.txt", "zip-partitioner/src/files/file2.txt")
      val listFileArchives = createStreamArchive(filePaths)

      uploadFilesToDynamoDB(dynamoDbClient, listFileArchives, "myDynamoTable", "filename", "data")

      sleep(2000)

      val fileNames = List("file1.txt", "file2.txt")
      val downloaded = downloadFilesFromDynamoDB(dynamoDbClient, fileNames, "myDynamoTable", "filename", "data")
        .unsafeRunSync()
        .compile
        .toList
        .unsafeRunSync()

      downloaded must_== listFileArchives
    }
  }

  trait LocalScope extends Scope {
    val localstackEndpoint = "http://localhost:4566"

    val dynamoDbClient = DynamoDbAsyncClient.builder()
        .endpointOverride(URI.create(localstackEndpoint))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
        .region(Region.EU_WEST_1)
        .build()
    }
}

