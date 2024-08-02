package dynamo_zip_store


import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import software.amazon.awssdk.services.dynamodb.{DynamoDbAsyncClient, DynamoDbClient}
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, GetItemRequest, PutItemRequest}
import zip_partitioner.FileArchive

import scala.jdk.CollectionConverters.MapHasAsJava

object DynamoZipStore {

  def uploadFilesToDynamoDB(client: DynamoDbAsyncClient, fileArchives: Stream[IO, FileArchive], dynamoConfig: DynamoDestinationConfig): Stream[IO, PutItemRequest] = {

    fileArchives.foreach { fileArchive =>
      val fileName =fileArchive.name
      val fileBytes = fileArchive.compressedData

      val item = Map(
        dynamoConfig.keyColumnName -> AttributeValue.builder().s(fileName).build(),
        dynamoConfig.dataColumnName -> AttributeValue.builder().s(fileBytes).build()
      ).asJava

      val request = PutItemRequest.builder()
        .tableName(dynamoConfig.tableName)
        .item(item)
        .build()
      IO.fromCompletableFuture(IO.delay(client.putItem(request))).void
    }
  }

  def downloadFilesFromDynamoDB(client: DynamoDbAsyncClient, fileNames: List[String], dynamoConfig: DynamoDestinationConfig): IO[Stream[IO, FileArchive]] = IO {

    Stream.emits(fileNames).evalMap { fileName => IO {
        val key = Map(dynamoConfig.keyColumnName -> AttributeValue.builder().s(fileName).build()).asJava

        val request = GetItemRequest.builder()
          .tableName(dynamoConfig.tableName)
          .key(key)
          .build()

//        val response = IO.fromCompletableFuture(IO.delay(client.getItem(request))).map {
//          response =>
//            response.item() match {
//              case null => None
//              case item => FileArchive(fileName, item.get(valueColumnName).s())
//            }
//        }.unsafeRunSync()
//      }
        val response = IO.fromCompletableFuture(IO.delay(client.getItem(request)))
        val item = response.map(_.item())

        FileArchive(fileName, item.map(_.get(dynamoConfig.dataColumnName).s()).unsafeRunSync())
      }
    }
  }

  case class DynamoDestinationConfig(
                                      tableName: String,
                                      keyColumnName: String,
                                      dataColumnName: String)

}
