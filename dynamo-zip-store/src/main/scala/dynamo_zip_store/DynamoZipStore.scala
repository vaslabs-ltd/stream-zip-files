package dynamo_zip_store


import cats.effect.IO
import fs2.Stream
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, GetItemRequest, PutItemRequest}
import zip_partitioner.FileArchive

import scala.jdk.CollectionConverters.MapHasAsJava

object DynamoZipStore {



  def uploadFilesToDynamoDB(client: DynamoDbClient, fileArchives: List[FileArchive], tableName: String): IO[Unit] = {

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
    IO.unit
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

}
