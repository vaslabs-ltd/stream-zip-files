package zip_partitioner.dynamo

import cats.effect.IO
import fs2.Stream
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, GetItemRequest, GetItemResponse, PutItemRequest, PutItemResponse}
import zip_partitioner.FileArchive

import scala.jdk.CollectionConverters.MapHasAsJava

object DynamoZipStore {

  def uploadFilesToDynamoDB(client: DynamoDbAsyncClient, fileArchives: Stream[IO, FileArchive], dynamoConfig: DynamoDestinationConfig): Stream[IO, PutItemResponse] =
    fileArchives.evalMap { fileArchive =>
      val fileName = fileArchive.name
      val fileBytes = fileArchive.compressedData

      val item = Map(
        dynamoConfig.keyColumnName -> AttributeValue.builder().s(fileName).build(),
        dynamoConfig.dataColumnName -> AttributeValue.builder().s(fileBytes).build()
      ).asJava

      val request = PutItemRequest.builder()
        .tableName(dynamoConfig.tableName)
        .item(item)
        .build()

      IO.fromCompletableFuture(IO.delay(client.putItem(request)))
    }


  def downloadFilesFromDynamoDB(client: DynamoDbAsyncClient, fileNames: List[String], dynamoConfig: DynamoDestinationConfig): Stream[IO, FileArchive] = {
    Stream.emits(fileNames).covary[IO].evalMap { fileName =>
      val key = Map(dynamoConfig.keyColumnName -> AttributeValue.builder().s(fileName).build()).asJava

      val request = GetItemRequest.builder()
        .tableName(dynamoConfig.tableName)
        .key(key)
        .build()

      val response: IO[GetItemResponse] = IO.fromCompletableFuture(IO.delay(client.getItem(request)))
      val itemIO: IO[java.util.Map[String, AttributeValue]] = response.map(_.item())
      itemIO.map(
        item => FileArchive(fileName, getItemValue(dynamoConfig.dataColumnName, item))
      )
    }
  }

  private def getItemValue(key: String, item: java.util.Map[String, AttributeValue]): String = {
    item.get(key).s()
  }

  case class DynamoDestinationConfig(
                                      tableName: String,
                                      keyColumnName: String,
                                      dataColumnName: String)

}
