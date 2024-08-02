package dynamo_zip_store_test

import cats.effect.unsafe.implicits.global
import dynamo_zip_store.DynamoZipStore.{downloadFilesFromDynamoDB, uploadFilesToDynamoDB}
import org.specs2.mutable.Specification
import org.specs2.specification.Scope
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import zip_partitioner.FileArchive

import java.net.URI

object DynamoZipStoreSpec extends Specification{

  "DynamoZipStore" should {
    "uploadFilesToDynamoDB" in new LocalScope {

      val toUpload = List(FileArchive("test", "test"))
      uploadFilesToDynamoDB(dynamoDbClient, toUpload, "myDynamoTable").unsafeRunSync()

      val downloaded = downloadFilesFromDynamoDB(dynamoDbClient, List("test"), "myDynamoTable")
        .unsafeRunSync()
        .compile
        .toList
        .unsafeRunSync()

      downloaded must_== toUpload
    }
  }

  trait LocalScope extends Scope {
    val localstackEndpoint = "http://localhost:4566"

    val dynamoDbClient = DynamoDbClient.builder()
        .endpointOverride(URI.create(localstackEndpoint))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
        .region(Region.EU_WEST_1) // or any region
        .build()
  }
}
