package zip_partitioner.s3

import cats.effect.{IO, Resource}
import eu.timepit.refined.types.string.NonEmptyString
import fs2.Pipe
import fs2.aws.s3.S3
import fs2.aws.s3.models.Models.{BucketName, FileKey}
import fs2.compression.{DeflateParams, InflateParams, ZLibParams}
import io.laserdisc.pure.s3.tagless.Interpreter
import software.amazon.awssdk.services.s3.S3AsyncClient

import java.util.zip.{ZipEntry, ZipOutputStream}

object S3ZipStore {
//  private type RIO[R, A] = ReaderT[IO, R, A]

  def transfer(from: NonEmptyString, to: NonEmptyString, fileIdentifier: NonEmptyString, s3AsyncClient: S3AsyncClient): fs2.Stream[IO, Unit] =
    withS3App(s3AsyncClient).flatMap { s3 =>
      val body = s3.readFile(BucketName(from), FileKey(fileIdentifier))
      body.through(deflateStream)
        .through(s3.uploadFile(BucketName(to), FileKey(fileIdentifier)))
        .map(_ => ())
    }

  def retrieveSingle(compressedBucket: NonEmptyString, fileKey: NonEmptyString, s3AsyncClient: S3AsyncClient): fs2.Stream[IO, Byte] =
    withS3App(s3AsyncClient).flatMap { s3 =>
      val body = s3.readFile(BucketName(compressedBucket), FileKey(fileKey))
      body.through(inflateStream)
    }

  // create a zip file in the form of a byte stream out of several file keys retrieved from s3
  def retrieveMultiple(compressedBucket: NonEmptyString, fileKeys: List[NonEmptyString], s3AsyncClient: S3AsyncClient): fs2.Stream[IO, Byte] =
    withS3App(s3AsyncClient).flatMap { s3 =>
      fs2.Stream.emits(fileKeys).map { fileKey =>
        val body = s3.readFile(BucketName(compressedBucket), FileKey(fileKey))

        FileArchive(fileKey.value, body)
      }.through(zipPipe)
    }

  private case class FileArchive(identifier: String, data: fs2.Stream[IO, Byte]) {
    def asZipEntry = new ZipEntry(identifier)
  }

  private def rioDelay[A](a: => A): IO[A] = (IO.delay(a))

  private def zipPipe: Pipe[IO, FileArchive, Byte] = { fileArchives: fs2.Stream[IO, FileArchive] =>
    fs2.io.readOutputStream[IO](1024*10) {
      outputStream =>
        Resource.fromAutoCloseable(rioDelay(new ZipOutputStream(outputStream))).use { zipOut =>
          val writeOutput = fs2.io.writeOutputStream[IO](IO(zipOut), closeAfterUse = false)
          fileArchives.evalMap { archive: FileArchive =>
            rioDelay(zipOut.putNextEntry(archive.asZipEntry)) >>
              archive.data.through(writeOutput).compile.drain >>
              rioDelay(zipOut.closeEntry())
          }.compile.drain
        }
    }
  }

  private def withS3App(s3Client: S3AsyncClient): fs2.Stream[IO, S3[IO]] = fs2.Stream.emit {
      val s3 = S3.create(Interpreter[IO].create(s3Client))
      s3
  }

  private def deflateStream: Pipe[IO, Byte, Byte] =
    fs2.compression.Compression.forLiftIO[IO].deflate(DeflateParams.apply(
      header = ZLibParams.Header.ZLIB
    ))

  private def inflateStream: Pipe[IO, Byte, Byte] =
    fs2.compression.Compression.forLiftIO[IO].inflate(
      InflateParams(
        header = ZLibParams.Header.ZLIB
      )
    )
}
