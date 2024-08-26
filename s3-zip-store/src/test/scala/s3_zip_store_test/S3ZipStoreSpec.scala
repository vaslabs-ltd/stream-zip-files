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
import software.amazon.awssdk.services.s3.{S3AsyncClient, S3AsyncClientBuilder}
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import zip_partitioner.s3.S3ZipStore

import java.io.ByteArrayInputStream
import java.net.URI
import java.util.UUID
import java.util.zip.{Inflater, ZipFile, ZipInputStream}

object S3ZipStoreSpec extends Specification {

  "S3ManagerReportDatabaseStorage should transfer a file to S3 deflated" in new LocalScope {

    // retrieve the file from the compressed bucket inflated
    val getFileFromStorage = s3RetrieveSingle(fileKey1)

    originalFileContent1 must_== getFileFromStorage

    //get multiple as zip, unzip and find same result
    val getFileFromStorageAsZipBytes = s3RetrieveMultiple(List(fileKey1))

    val zipInputStream = new ZipInputStream(new ByteArrayInputStream(getFileFromStorageAsZipBytes.toArray))

    val fileContent = getNextFileContent(zipInputStream)

    fileContent must_== originalFileContent1
  }

  "S3ManagerReportDatabaseStorage should transfer multiple files to S3 deflated" in new LocalScope {

    val getFileFromStorage = s3RetrieveMultiple(List(fileKey1, fileKey2))

    val zipInputStream = new ZipInputStream(new ByteArrayInputStream(getFileFromStorage.toArray))

    val fileContents = getAllFileContents(zipInputStream)

    fileContents(fileKey1.value) must_== originalFileContent1
    fileContents(fileKey2.value) must_== originalFileContent2
  }

  private def getNextFileContent(zipInputStream: ZipInputStream) = {
    val zipEntry = zipInputStream.getNextEntry

    val dataOfZipEntry = zipInputStream.readAllBytes()
    val inflate = new Inflater()
    inflate.setInput(dataOfZipEntry)
    val data = new Array[Byte](1024)
    val read = inflate.inflate(data)

    val fileContent = new String(data, 0, read)
    fileContent
  }

  private def getAllFileContents(zipInputStream: ZipInputStream): Map[String, String] = {
    val fileContents = scala.collection.mutable.Map[String, String]()
    var zipEntry = zipInputStream.getNextEntry

    while (zipEntry != null) {
      val dataOfZipEntry = zipInputStream.readAllBytes()
      val inflate = new Inflater()
      inflate.setInput(dataOfZipEntry)
      val data = new Array[Byte](1024)
      val read = inflate.inflate(data)
      val fileContent = new String(data, 0, read)
      fileContents += (zipEntry.getName -> fileContent)
      zipEntry = zipInputStream.getNextEntry
    }

    fileContents.toMap
  }

  def createBucket(bucketName: String, s3AsyncClient: S3AsyncClient) = {
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

    def s3Transfer(fileIdentity: NonEmptyString): Unit = S3ZipStore.transfer(
      from = uncompressedBucket,
      to = compressedBucket,
      fileIdentifier = fileIdentity,
      s3AsyncClient = s3AsyncClient
    ).compile.drain.unsafeRunSync()

    s3Transfer(fileKey1)
    s3Transfer(fileKey2)

    def s3RetrieveSingle(fileIdentity: NonEmptyString): String = S3ZipStore.retrieveSingle(
      compressedBucket = compressedBucket,
      fileKey = fileIdentity,
      s3AsyncClient = s3AsyncClient
    ).through(fs2.text.utf8.decode).compile.string.unsafeRunSync()

    def s3RetrieveMultiple(fileIdentities: List[NonEmptyString]): Vector[Byte] = S3ZipStore.retrieveMultiple(
      compressedBucket = compressedBucket,
      fileKeys = fileIdentities,
      s3AsyncClient = s3AsyncClient
    ).compile.toVector.unsafeRunSync()

  }
}
