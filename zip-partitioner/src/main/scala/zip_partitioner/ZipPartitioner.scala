package zip_partitioner

import cats.effect.{IO, IOApp}
import cats.effect.unsafe.implicits.global
import fs2.Stream
import fs2.io.file.{Files, Path}

import java.io.{ByteArrayOutputStream, FileOutputStream}
import java.util.Base64
import java.util.zip.{Deflater, Inflater, ZipOutputStream}

object ZipPartitioner {

  private var myDB: List[FileArchive] = List.empty

  private def readFile(path: String): Stream[IO, Byte] = {
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

}

object ZipBuilder extends IOApp.Simple {

  private val saveTo = "zip-partitioner/src/files/zipFile.zip"

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
    createZipFile(saveTo, ZipPartitioner.createStreamArchive(filePaths))
    IO.unit
  }
}

case class FileArchive (
                         name: String,
                         compressedData: String
                       )
