package dynamo_zip_store_test

import org.specs2.mutable.Specification
import org.specs2.specification.Scope
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

import java.net.URI

object DynamoZipStoreSpec extends Specification{

  "DynamoZipStore" should {
    "uploadFilesToDynamoDB" in new LocalScope {
      1 must_== 1
    }
  }

  trait LocalScope extends Scope {
    val localstackEndpoint = "http://localhost:4566"

      private val dynamoDbClient = DynamoDbClient.builder()
        .endpointOverride(URI.create(localstackEndpoint))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
        .region(Region.US_EAST_1) // or any region
        .build()
  }
}
