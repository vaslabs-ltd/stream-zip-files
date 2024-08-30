package zip_partitioner.s3

import cats.effect.IO
import eu.timepit.refined.types.string.NonEmptyString
import fs2.aws.s3.S3
import fs2.aws.s3.models.Models.{BucketName, ETag, FileKey}
import io.laserdisc.pure.s3.tagless.Interpreter
import software.amazon.awssdk.services.s3.S3AsyncClient
import zip_partitioner.{FileArchive, ZipStore}
import zip_partitioner.ZipPartitioner.{deflateStream, inflateStream, zipPipe}

class S3ZipStore(s3AsyncClient: S3AsyncClient) extends ZipStore[IO] {

  override type TransferOut = ETag

  def transfer(from: NonEmptyString, to: NonEmptyString, fileIdentifier: NonEmptyString): fs2.Stream[IO, ETag] =
    withS3(s3AsyncClient).flatMap { s3 =>
      val body = s3.readFile(BucketName(from), FileKey(fileIdentifier))
      body.through(deflateStream)
        .through(s3.uploadFile(BucketName(to), FileKey(fileIdentifier)))
    }

  def retrieveSingle(compressedBucket: NonEmptyString, fileKey: NonEmptyString): fs2.Stream[IO, Byte] =
    withS3(s3AsyncClient).flatMap { s3 =>
      val body = s3.readFile(BucketName(compressedBucket), FileKey(fileKey))
      body.through(inflateStream)
    }

  // create a zip file in the form of a byte stream out of several file keys retrieved from s3
  def retrieveMultiple(compressedBucket: NonEmptyString, fileKeys: List[NonEmptyString], streamSize: Int): fs2.Stream[IO, Byte] =
    withS3(s3AsyncClient).flatMap { s3 =>
      fs2.Stream.emits(fileKeys).map { fileKey =>
        val body = s3.readFile(BucketName(compressedBucket), FileKey(fileKey))

        FileArchive(fileKey.value, body)
      }.through(zipPipe(streamSize))
    }

  def retrieveMultiple(compressedBucket: NonEmptyString, fileKeys: List[NonEmptyString]): fs2.Stream[IO, Byte] = {
    val defaultBufferSize = 1024*512
    retrieveMultiple(compressedBucket, fileKeys, defaultBufferSize)
  }


  private def withS3(s3Client: S3AsyncClient): fs2.Stream[IO, S3[IO]] = fs2.Stream.emit {
      val s3 = S3.create(Interpreter[IO].create(s3Client))
      s3
  }
}
