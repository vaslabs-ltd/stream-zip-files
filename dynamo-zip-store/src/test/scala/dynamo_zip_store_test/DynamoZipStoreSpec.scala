package dynamo_zip_store_test

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import eu.timepit.refined.types.string.NonEmptyString
import org.specs2.mutable.Specification
import org.specs2.specification.Scope
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._
import zip_partitioner.FileArchive
import zip_partitioner.dynamo.{DynamoTableConfig, DynamoZipStore}
import zip_partitioner.test_utils.{RawTestData, StoreTests}

import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.UUID

object DynamoZipStoreSpec extends Specification{

  "DynamoZipStore" should {
    "uploadFilesToDynamoDB and then download them must be the same" in new LocalScope {
      val RawTestData(zipContent, uncompressedFiles) = StoreTests.testStore(dynamoZipStore, uncompressedTableName, compressedTableName, List(fileKey1, fileKey2)).unsafeRunSync()

      zipContent(fileKey1) must_== originalFileContent1
      zipContent(fileKey2) must_== originalFileContent2

      zipContent(fileKey1) must_== uncompressedFiles(fileKey1)
      zipContent(fileKey2) must_== uncompressedFiles(fileKey2)
    }
  }

  trait LocalScope extends Scope {
    val localstackEndpoint = "http://localhost:4566"

    val dynamoDbClient = DynamoDbAsyncClient.builder()
        .endpointOverride(URI.create(localstackEndpoint))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
        .region(Region.EU_WEST_1)
        .build()

    val compressedTableName = NonEmptyString.unsafeFrom(UUID.randomUUID().toString)
    val uncompressedTableName = NonEmptyString.unsafeFrom(UUID.randomUUID().toString)

    val keyColumnName = "fileName"
    val dataColumnName = "data"
    val from = DynamoTableConfig(keyColumnName, dataColumnName)
    val to = DynamoTableConfig(keyColumnName, dataColumnName)
    val dynamoZipStore = new DynamoZipStore(
      client = dynamoDbClient,
      dynamoSourceConfig = from,
      dynamoDestinationConfig = to
    )


    val createTableRequest1 = createDynamoTable(uncompressedTableName, from.keyColumnName)
    val createTableRequest2 = createDynamoTable(compressedTableName, to.keyColumnName)
    dynamoDbClient.createTable(
      createTableRequest1
    ).get()

    dynamoDbClient.createTable(
      createTableRequest2
    ).get()

    val fileKey1: NonEmptyString = NonEmptyString.unsafeFrom(UUID.randomUUID().toString)
    val fileKey2: NonEmptyString = NonEmptyString.unsafeFrom(UUID.randomUUID().toString)

    val originalFileContent1 = "Hello, World"
    val originalFileContent2 = "Hello, World 2"
    def stringStream(value: String): fs2.Stream[IO, Byte] = fs2.Stream.emits(value.getBytes(StandardCharsets.UTF_8))

    dynamoZipStore.uploadFilesToDynamoDB(
      fs2.Stream(
        FileArchive(fileKey1.value, stringStream(originalFileContent1)),
        FileArchive(fileKey2.value, stringStream(originalFileContent2))
      ),
      uncompressedTableName,
      from
    ).compile.drain.unsafeRunSync()

    private def createDynamoTable(table: NonEmptyString, keyColumnName: String) = {
      CreateTableRequest.builder()
        .tableName(table.value)
        .keySchema(
          KeySchemaElement.builder()
            .attributeName(keyColumnName)
            .keyType(KeyType.HASH)
            .build()
        )
        .attributeDefinitions(
          AttributeDefinition.builder()
            .attributeName(keyColumnName)
            .attributeType(ScalarAttributeType.S)
            .build()
        )
        .provisionedThroughput(
          software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput.builder()
            .readCapacityUnits(5L)
            .writeCapacityUnits(5L)
            .build()
        )
        .build()
    }


  }

}

