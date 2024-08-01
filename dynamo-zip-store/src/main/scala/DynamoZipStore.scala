
import cats.effect.{IO, IOApp}
import fs2.Stream
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, GetItemRequest, PutItemRequest}
import zip_partitioner.FileArchive

import java.net.URI
import scala.jdk.CollectionConverters.MapHasAsJava

object DynamoZipStore extends IOApp.Simple {

  val localstackEndpoint = "http://localhost:4566"

  val dynamoDbClient = DynamoDbClient.builder()
    .endpointOverride(URI.create(localstackEndpoint))
    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
    .region(Region.US_EAST_1) // or any region
    .build()

  def uploadFilesToDynamoDB(fileArchives: List[FileArchive], tableName: String): Unit = {

    fileArchives.foreach { fileArchive =>
      val fileName =fileArchive.name
      val fileBytes = fileArchive.compressedData

      val item = Map(
        "fileName" -> AttributeValue.builder().s(fileName).build(),
        "data" -> AttributeValue.builder().s(fileBytes).build()
      ).asJava

      val request = PutItemRequest.builder()
        .tableName(tableName)
        .item(item)
        .build()

      dynamoDbClient.putItem(request)
    }

    dynamoDbClient.close()
  }

  def downloadFilesFromDynamoDB(fileNames: List[String], tableName: String): Stream[IO, FileArchive] = {
    val dynamoDbClient = DynamoDbClient.builder().build()

    Stream.emits(fileNames).evalMap { fileName => IO {
        val key = Map("fileName" -> AttributeValue.builder().s(fileName).build()).asJava

        val request = GetItemRequest.builder()
          .tableName(tableName)
          .key(key)
          .build()

        val response = dynamoDbClient.getItem(request)
        val item = response.item()

        FileArchive(fileName, item.get("data").s())
      }.guarantee(IO(dynamoDbClient.close()))
    }

  }

  override def run: IO[Unit] = {
    IO.unit
  }
}
