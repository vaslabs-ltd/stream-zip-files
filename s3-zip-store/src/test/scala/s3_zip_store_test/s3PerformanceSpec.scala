package s3_zip_store_test

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import eu.timepit.refined.types.string.NonEmptyString
import fs2.aws.s3.S3
import fs2.aws.s3.models.Models.{BucketName, FileKey}
import fs2.io.file.{Files, Path}
import io.laserdisc.pure.s3.tagless.Interpreter
import org.specs2.mutable.Specification
import org.specs2.specification.Scope
import s3_zip_store_test.S3ZipStoreSpec.createBucket
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import zip_partitioner.s3.S3ZipStore

import java.net.URI
import java.util.UUID

object s3PerformanceSpec extends Specification {

  "S3ManagerReportDatabaseStorage " in new LocalScope {

    fileKeys.map(fileKey => {
      s3ZipStore.transfer(uncompressedBucket, compressedBucket, fileKey).compile.drain.unsafeRunSync()
    })

    s3ZipStore.retrieveMultiple(compressedBucket, fileKeys)
      .through(fs2.io.file.Files.forIO.writeAll(Path("s3-zip-store/src/test/resources/zip.zip")))
      .compile.drain.unsafeRunSync()

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

    def readResourceFile(fileName: String): fs2.Stream[IO, Byte] = {
      Files[IO].readAll(Path(fileName))
    }

    createBucket(uncompressedBucket.value, s3AsyncClient).unsafeRunSync()
    createBucket(compressedBucket.value, s3AsyncClient).unsafeRunSync()

    val s3Interpreter = Interpreter[IO].create(s3AsyncClient)

    val s3: S3[IO] = S3.create(s3Interpreter)

    val writeFileKeys = for  {
      i <- 1 to 1000
      fileKey = NonEmptyString.unsafeFrom(s"fileKey$i")
      _ = readResourceFile(s"s3-zip-store/src/test/resources/file_$i.txt")
        .through(s3.uploadFile(BucketName(uncompressedBucket), FileKey(fileKey)))
        .compile
        .drain
        .unsafeRunSync()
    } yield fileKey

    val fileKeys = writeFileKeys.toList

    println(fileKeys)
    val s3ZipStore = new S3ZipStore(s3AsyncClient)

  }
}

