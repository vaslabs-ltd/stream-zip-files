package zip_partitioner.dynamo

import cats.effect.IO
import eu.timepit.refined.types.string.NonEmptyString
import fs2.Stream
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, GetItemRequest, GetItemResponse, PutItemRequest, PutItemResponse}
import zip_partitioner.{FileArchive, ZipPartitioner, ZipStore}

import java.io.InputStream
import scala.jdk.CollectionConverters.MapHasAsJava

class DynamoZipStore(client: DynamoDbAsyncClient, dynamoSourceConfig: DynamoTableConfig, dynamoDestinationConfig: DynamoTableConfig) extends ZipStore[IO] {

  def uploadFilesToDynamoDB(fileArchives: Stream[IO, FileArchive], tableName: NonEmptyString, dynamoConfig: DynamoTableConfig): Stream[IO, PutItemResponse] =
    fileArchives.flatMap { fileArchive =>
      val fileName = fileArchive.identifier

      val fileBytes: Stream[IO, InputStream] =
        fileArchive.data.through(fs2.io.toInputStream)

      fileBytes.map(SdkBytes.fromInputStream)
        .map { fileBytes =>
          Map(
            dynamoConfig.keyColumnName -> AttributeValue.builder().s(fileName).build(),
            dynamoConfig.dataColumnName -> AttributeValue.builder().b(fileBytes).build()
          ).asJava
        }.map {
          item =>
            PutItemRequest.builder()
              .tableName(tableName.value)
              .item(item)
              .build()
        }.evalMap {
          request =>
            IO.fromCompletableFuture(IO.delay(client.putItem(request)))
        }
    }


  private def downloadFilesFromDynamoDB(
                                         fileNames: List[NonEmptyString],
                                         tableName: NonEmptyString,
                                         dynamoTableConfig: DynamoTableConfig,
                                         deflate: Boolean
                                       ): Stream[IO, FileArchive] = {
    Stream.emits(fileNames).covary[IO].evalMap { fileName =>
      val key = Map(dynamoTableConfig.keyColumnName -> AttributeValue.builder().s(fileName.value).build()).asJava

      val request = GetItemRequest.builder()
        .tableName(tableName.value)
        .key(key)
        .build()

      def deflateIfRequested(stream: fs2.Stream[IO, Byte]): Stream[IO, Byte] =
        if (deflate) stream through ZipPartitioner.deflateStream else stream

      val response: IO[GetItemResponse] = IO.fromCompletableFuture(IO.delay(client.getItem(request)))
      val itemIO: IO[java.util.Map[String, AttributeValue]] = response.map(_.item())
      itemIO.map(
        item => FileArchive(fileName.value, deflateIfRequested(getItemValue(dynamoTableConfig.dataColumnName, item)))
      )
    }
  }

  private def getItemValue(key: String, item: java.util.Map[String, AttributeValue]): fs2.Stream[IO, Byte] = {
   val jStream = item.get(key).b().asInputStream()
    fs2.io.readInputStream(IO(jStream), 1024)
  }

  override type TransferOut = PutItemResponse

  override def transfer(from: NonEmptyString, to: NonEmptyString, fileIdentifier: NonEmptyString): Stream[IO, PutItemResponse] =
    uploadFilesToDynamoDB(downloadFilesFromDynamoDB(List(fileIdentifier), from, dynamoSourceConfig, deflate = true), to, dynamoDestinationConfig)

  override def retrieveSingle(tableName: NonEmptyString, fileKey: NonEmptyString): Stream[IO, Byte] =
    downloadFilesFromDynamoDB(List(fileKey), tableName, dynamoDestinationConfig, deflate = false).flatMap(_.inflatedData)

  override def retrieveMultiple(tableName: NonEmptyString, fileKeys: List[NonEmptyString], chunkSize: Int): Stream[IO, Byte] =
    downloadFilesFromDynamoDB(fileKeys, tableName, dynamoDestinationConfig, deflate = false).through(ZipPartitioner.zipPipe(chunkSize))
}

case class DynamoTableConfig(
  keyColumnName: String,
  dataColumnName: String
)
