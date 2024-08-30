package zip_partitioner

import cats.effect.{IO, Resource}
import fs2.Pipe
import fs2.compression.{DeflateParams, InflateParams, ZLibParams}
import zip_partitioner.ZipPartitioner.inflateStream

import java.util.zip.{ZipEntry, ZipOutputStream}

object ZipPartitioner {

  def deflateStream: Pipe[IO, Byte, Byte] =
    fs2.compression.Compression.forIO.deflate(DeflateParams.apply(
      header = ZLibParams.Header.ZLIB
    ))

  def inflateStream: Pipe[IO, Byte, Byte] =
    fs2.compression.Compression.forIO.inflate(
      InflateParams(
        header = ZLibParams.Header.ZLIB
      )
    )

  def zipPipe(chunkSize: Int): Pipe[IO, FileArchive, Byte] = { fileArchives: fs2.Stream[IO, FileArchive] =>
    fs2.io.readOutputStream[IO](chunkSize) {
      outputStream =>
        Resource.fromAutoCloseable(IO.delay(new ZipOutputStream(outputStream))).use { zipOut =>
          val writeOutput = fs2.io.writeOutputStream[IO](IO(zipOut), closeAfterUse = false)
          fileArchives.evalMap { archive: FileArchive =>
            IO.delay(zipOut.putNextEntry(archive.asZipEntry)) >>
              archive.inflatedData.through(writeOutput).compile.drain >>
              IO.delay(zipOut.closeEntry())
          }.compile.drain
        }
    }
  }

}

case class FileArchive(identifier: String, data: fs2.Stream[IO, Byte]) {
  def asZipEntry = {
    val zipEntry = new ZipEntry(identifier)
    zipEntry
  }
  def inflatedData: fs2.Stream[IO, Byte] = data.through(inflateStream)
}