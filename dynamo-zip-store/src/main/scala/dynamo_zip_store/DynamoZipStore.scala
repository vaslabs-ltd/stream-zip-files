package dynamo_zip_store


import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import software.amazon.awssdk.services.dynamodb.{DynamoDbAsyncClient, DynamoDbClient}
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, GetItemRequest, PutItemRequest}
import zip_partitioner.FileArchive

import scala.jdk.CollectionConverters.MapHasAsJava

object DynamoZipStore {

  def uploadFilesToDynamoDB(client: DynamoDbAsyncClient, fileArchives: Stream[IO, FileArchive], tableName: String, keyColumnName: String, valueColumnName: String): IO[Unit] = {

    fileArchives.foreach { fileArchive =>
      val fileName =fileArchive.name
      val fileBytes = fileArchive.compressedData

      val item = Map(
        keyColumnName -> AttributeValue.builder().s(fileName).build(),
        valueColumnName -> AttributeValue.builder().s(fileBytes).build()
      ).asJava

      val request = PutItemRequest.builder()
        .tableName(tableName)
        .item(item)
        .build()
      IO.fromCompletableFuture(IO(client.putItem(request))).void
    }.compile.drain
  }

  def downloadFilesFromDynamoDB(client: DynamoDbAsyncClient, fileNames: List[String], tableName: String, keyColumnName: String, valueColumnName: String): IO[Stream[IO, FileArchive]] = IO {

    Stream.emits(fileNames).evalMap { fileName => IO {
        val key = Map(keyColumnName -> AttributeValue.builder().s(fileName).build()).asJava

        val request = GetItemRequest.builder()
          .tableName(tableName)
          .key(key)
          .build()

        val response = IO.fromCompletableFuture(IO(client.getItem(request)))
        val item = response.unsafeRunSync().item()

        FileArchive(fileName, item.get(valueColumnName).s())
      }
    }
  }

}
