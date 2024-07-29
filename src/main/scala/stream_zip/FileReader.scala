package stream_zip

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, IOApp}
import fs2.Stream
import fs2.io.file.{Files, Path}

import java.io.{ByteArrayOutputStream, File, FileOutputStream}
import java.util.Base64
import java.util.zip.{Deflater, Inflater, ZipOutputStream}

object MultiFileReader extends IOApp.Simple {

  private var myDB: List[FileArchive] = List.empty

  private def readFile(path: String): Stream[IO, Byte] = {
    Files[IO].readAll(Path(path))
  }

  private def encodeBase64(bytes: Array[Byte]): String = {
    Base64.getEncoder.encodeToString(bytes)
  }

  private def createFileArchive(path: String): FileArchive = {
    val bytes = readFile(path).compile.to(Array).unsafeRunSync()
    val compressed = compress(bytes)
//    println(s"Original size: ${bytes.length}")
//    println(s"Compressed size: ${compressed.length}")
    val fa = FileArchive(path, encodeBase64(compressed), encodeBase64(bytes))
    myDB = myDB :+ fa
    fa
  }

  private def createZipFile(saveTo: String, fileArchive: Stream[IO, FileArchive]): IO[Unit] = {
//    println("PRINT-1-fileArchive in createZipFile fun", fileArchive.compile.toList.unsafeRunSync())
    val streamBytes: Stream[IO, (String, Array[Byte])] = fileArchive.map(serializeFileArchive).covary[IO]
//    println("PRINT-4-streamBytes in createZipFile fun", streamBytes.compile.toList.unsafeRunSync())
    val zipFile = new FileOutputStream(saveTo)
    val zipOut = new ZipOutputStream(zipFile)

    streamBytes.evalMap { case (fileName, byteArray) => IO {
        println("PRINT-5-bytes in createZipFile fun", byteArray)
        val entry = new java.util.zip.ZipEntry(fileName)
        zipOut.putNextEntry(entry)
        zipOut.write(byteArray)
        zipOut.closeEntry()
      }
    }.compile.drain.guarantee(IO(zipOut.close())).unsafeRunSync()
    IO.unit
  }
//  private def createZipFile(saveTo: File, fileArchive: Stream[IO, Array[Byte]]): Unit = ???

  private def serializeFileArchive(file: FileArchive): (String, Array[Byte]) = {
//    println("PRINT-2-file in serializeFileArchive: ", file)
    val name = file.name
    val compressedData = Base64.getDecoder.decode(file.compressedData)
//    val uncompressedData = Base64.getDecoder.decode(file.uncompressedData)
//
//    val nameSize = name.length.toByte
//    val compressedDataSize = compressedData.length.toByte
//    val uncompressedDataSize = uncompressedData.length.toByte

//    val buffer = new ByteArrayOutputStream()
//    buffer.write(nameSize)
//    buffer.write(name)
//    buffer.write(compressedDataSize)
//    buffer.write(compressedData)
//    buffer.write(uncompressedDataSize)
//    buffer.write(uncompressedData)
//
//    buffer.toByteArray


    val buffer = new ByteArrayOutputStream()
    buffer.write(decompress(compressedData))
    println("PRINT-3-buffer size in serializeFileArchive: ", buffer.size())
    (name, buffer.toByteArray)
  }

  private def readFileCompressedDataAndCreateZip(files: Stream[IO, FileArchive]): IO[Unit] = {
    createZipFile("src/files/zipFile.zip", files)
  }

  private def readAndEncode(paths: List[String]): Stream[IO, FileArchive] = {
    Stream.emits(paths).map(createFileArchive)
  }

//  def readInt(stream: ByteArrayInputStream): Int = {
//    val bytes = new Array()
//    stream.read(bytes)
//    ByteBuffer.wrap(bytes).getInt
//  }

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

  private def compress (bytes: Array[Byte]): Array[Byte] = {
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


  override def run: IO[Unit] = {
    val filePaths = List("src/files/file1.txt", "src/files/file2.txt", "src/files/file3.pdf", "src/files/large_file1.txt", "src/files/large_file2.txt")
    val encodeStream = readAndEncode(filePaths)
    encodeStream.compile.drain.unsafeRunSync()
//    println("myDB: ", myDB)

    val streamDB = Stream.emits(myDB).covary[IO]
    readFileCompressedDataAndCreateZip(streamDB)
    IO.unit
  }
}

case class FileArchive (
                       name: String,
                       compressedData: String,
                       uncompressedData: String
                       )

