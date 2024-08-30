package s3_zip_store_test

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import eu.timepit.refined.types.string.NonEmptyString
import fs2.aws.s3.S3
import fs2.aws.s3.models.Models.{BucketName, FileKey}
import io.laserdisc.pure.s3.tagless.Interpreter
import org.specs2.mutable.Specification
import org.specs2.specification.Scope
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{CreateBucketRequest, CreateBucketResponse}
import zip_partitioner.s3.S3ZipStore
import zip_partitioner.test_utils.{RawTestData, StoreTests}

import java.net.URI
import java.util.UUID

object S3ZipStoreSpec extends Specification {

  "S3ManagerReportDatabaseStorage should transfer multiple files from S3 in a zip and single files inflated" in new LocalScope {
    val RawTestData(zipContent, uncompressedFiles) = StoreTests.testStore(s3ZipStore, uncompressedBucket, compressedBucket, List(fileKey1, fileKey2)).unsafeRunSync()

    zipContent(fileKey1) must_== originalFileContent1
    zipContent(fileKey2) must_== originalFileContent2

    zipContent(fileKey1) must_== uncompressedFiles(fileKey1)
    zipContent(fileKey2) must_== uncompressedFiles(fileKey2)
  }

  def createBucket(bucketName: String, s3AsyncClient: S3AsyncClient): IO[CreateBucketResponse] = {
    val req = CreateBucketRequest.builder()
      .bucket(bucketName)
      .build()
    IO.fromCompletableFuture(
      IO(
        s3AsyncClient.createBucket(req)
      )
    )
  }

  trait LocalScope extends Scope {
    val localstackEndpoint = "http://localhost:4566"

    val s3AsyncClient =
      S3AsyncClient.builder()
        .endpointOverride(URI.create(localstackEndpoint))
        .forcePathStyle(true)
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
        .region(Region.EU_WEST_1)
        .build()


    val uncompressedBucket: NonEmptyString = NonEmptyString.unsafeFrom(UUID.randomUUID().toString)
    val compressedBucket: NonEmptyString = NonEmptyString.unsafeFrom(UUID.randomUUID().toString)
    val fileKey1: NonEmptyString = NonEmptyString.unsafeFrom(UUID.randomUUID().toString)
    val fileKey2: NonEmptyString = NonEmptyString.unsafeFrom(UUID.randomUUID().toString)

    val originalFileContent1 = "Hello, World"
    val originalFileContent2 = "Hello, World 2"

    createBucket(uncompressedBucket.value, s3AsyncClient).unsafeRunSync()
    createBucket(compressedBucket.value, s3AsyncClient).unsafeRunSync()

    val s3Interpreter = Interpreter[IO].create(s3AsyncClient)

    val s3: S3[IO] = S3.create(s3Interpreter)

    fs2.Stream.emits(originalFileContent1.getBytes).covary[IO]
      .through(s3.uploadFile(BucketName(uncompressedBucket), FileKey(fileKey1)))
      .compile
      .drain
      .unsafeRunSync()

    fs2.Stream.emits(originalFileContent2.getBytes).covary[IO]
      .through(s3.uploadFile(BucketName(uncompressedBucket), FileKey(fileKey2)))
      .compile
      .drain
      .unsafeRunSync()

    val s3ZipStore = new S3ZipStore(s3AsyncClient)

  }
}
