
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



  def uploadFilesToDynamoDB(client: DynamoDbClient, fileArchives: List[FileArchive], tableName: String): Unit = {

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

      client.putItem(request)
    }

  }

  def downloadFilesFromDynamoDB(client: DynamoDbClient, fileNames: List[String], tableName: String): IO[Stream[IO, FileArchive]] = IO{

    Stream.emits(fileNames).evalMap { fileName => IO {
        val key = Map("fileName" -> AttributeValue.builder().s(fileName).build()).asJava

        val request = GetItemRequest.builder()
          .tableName(tableName)
          .key(key)
          .build()

        val response = client.getItem(request)
        val item = response.item()

        FileArchive(fileName, item.get("data").s())
      }
    }

  }

  override def run: IO[Unit] = {
    IO.unit
  }
}
