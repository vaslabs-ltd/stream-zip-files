package stream_zip

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, IOApp}
import fs2.Stream
import fs2.io.file.{Files, Path}

import java.io.{ByteArrayOutputStream, File, FileOutputStream}
import java.util.Base64
import java.util.zip.{Deflater, Inflater, ZipOutputStream}
import stream_zip.DynamoHelper
import zip_partitioner.{FileArchive, ZipPartitioner}

object ZipBuilder extends IOApp.Simple {

  val saveTo = "src/files/zipFile.zip"

  def createZipFile(saveTo: String, fileArchive: Stream[IO, FileArchive]): IO[Unit] = {
    val streamBytes: Stream[IO, (String, Array[Byte])] = fileArchive.map(serializeFileArchive).covary[IO]
    val zipFile = new FileOutputStream(saveTo)
    val zipOut = new ZipOutputStream(zipFile)

    streamBytes.evalMap { case (fileName, byteArray) => IO {
        val entry = new java.util.zip.ZipEntry(fileName)
        zipOut.putNextEntry(entry)
        zipOut.write(byteArray)
        zipOut.closeEntry()
      }
    }.compile.drain.guarantee(IO(zipOut.close())).unsafeRunSync()
    IO.unit
  }

  private def serializeFileArchive(file: FileArchive): (String, Array[Byte]) = {
    val name = file.name
    val compressedData = Base64.getDecoder.decode(file.compressedData)

    val buffer = new ByteArrayOutputStream()
    buffer.write(decompress(compressedData))
    (name, buffer.toByteArray)
  }

  def readFileCompressedDataAndCreateZip(files: Stream[IO, FileArchive]): IO[Unit] = {
    createZipFile(saveTo, files)
  }

  private def decompress (bytes: Array[Byte]): Array[Byte] = {
    val inflater = new Inflater()
    inflater.setInput(bytes)

    val outputStream = new ByteArrayOutputStream()
    val buffer = new Array[Byte](bytes.length * 2)  // not sure for the size

    while (!inflater.finished) {
      val decompressedSize = inflater.inflate(buffer)
      outputStream.write(buffer, 0, decompressedSize)
    }

    outputStream.toByteArray
  }

  override def run: IO[Unit] = {

    val filePaths = List("src/files/file1.txt", "src/files/file2.txt", "src/files/file3.pdf", "src/files/large_file1.txt", "src/files/large_file2.txt")
    readFileCompressedDataAndCreateZip(ZipPartitioner.createStreamArchive(filePaths))
    IO.unit
  }
}

