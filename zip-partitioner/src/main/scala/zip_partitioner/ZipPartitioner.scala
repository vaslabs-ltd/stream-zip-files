package zip_partitioner

import cats.effect.{IO, IOApp}
import cats.effect.unsafe.implicits.global
import fs2.Stream
import fs2.io.file.{Files, Path}

import java.io.ByteArrayOutputStream
import java.util.Base64
import java.util.zip.Deflater

object ZipPartitioner extends IOApp.Simple {

  private var myDB: List[FileArchive] = List.empty

  def readFile(path: String): Stream[IO, Byte] = {
    Files[IO].readAll(Path(path))
  }

  def encodeBase64(bytes: Array[Byte]): String = {
    Base64.getEncoder.encodeToString(bytes)
  }

  def compress (bytes: Array[Byte]): Array[Byte] = {
    val deflater = new Deflater()
    deflater.setInput(bytes)
    deflater.finish

    val outputStream = new ByteArrayOutputStream()
    val buffer = new Array[Byte](bytes.length)

    while (!deflater.finished) {
      val compressedSize = deflater.deflate(buffer)
      outputStream.write(buffer, 0, compressedSize)
    }

    outputStream.toByteArray
  }

  private def createFileArchive(path: String): FileArchive = {
    val bytes = readFile(path).compile.to(Array).unsafeRunSync()
    val compressed = compress(bytes)
    val fa = FileArchive(path.split('/').last, encodeBase64(compressed))
    myDB = myDB :+ fa
    fa
  }

  private def readAndEncode(paths: List[String]): Stream[IO, FileArchive] = {
    Stream.emits(paths).map(createFileArchive)
  }

  def createStreamArchive(filePaths: List[String]): Stream[IO, FileArchive] = {
    val encodeStream = readAndEncode(filePaths)
    encodeStream
  }

  override def run: IO[Unit] = {
    val filePaths = List("src/files/file1.txt", "src/files/file2.txt", "src/files/file3.pdf", "src/files/large_file1.txt", "src/files/large_file2.txt")
    createStreamArchive(filePaths).compile.drain
  }
}

case class FileArchive (
                         name: String,
                         compressedData: String
                       )
