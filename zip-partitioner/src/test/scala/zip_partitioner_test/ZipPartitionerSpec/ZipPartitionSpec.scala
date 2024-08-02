package zip_partitioner_test.ZipPartitionerSpec

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import fs2.io.file.{Files, Path}
import zip_partitioner.ZipPartitioner.{compress, createStreamArchive, encodeBase64}
import org.specs2.mutable.Specification
import org.specs2.specification.Scope
import zip_partitioner.FileArchive
import zip_partitioner.ZipBuilder.createZipFile

import java.io.{File, FileInputStream, FileOutputStream, IOException}
import java.security.MessageDigest
import java.util.zip.{ZipEntry, ZipInputStream}

object ZipPartitionerSpec extends Specification {

  "ZipPartitioner" should {
    "createStreamArchive" in {
      val filePaths = List("zip-partitioner/src/files/file1.txt", "zip-partitioner/src/files/file2.txt")
      val result = createStreamArchive(filePaths)
      result.compile.toList.unsafeRunSync().size must_== 2
    }
  }

  "ZipBuilder" should {
    "createZipFile" in new LocalScope {
      val stream = createStreamArchive(paths)
      createZipFile(saveTo, stream).unsafeRunSync()
      val exsistingFiles = hashFilesInDirectory("zip-partitioner/src/files/testFiles").unsafeRunSync()

      unzip(saveTo, "zip-partitioner/src/files/testFilesAfterZip")
      val unzipFiles = hashFilesInDirectory("zip-partitioner/src/files/testFilesAfterZip").unsafeRunSync()

      mapsAreConsistent(exsistingFiles, unzipFiles) must_== true

      val randomMap: Map[String, String] = Map("random" -> "random")
      mapsAreConsistent(exsistingFiles, randomMap) must_== false
    }
  }

  trait LocalScope extends Scope {
    val paths = List("zip-partitioner/src/files/testFiles/file1.txt", "zip-partitioner/src/files/testFiles/file2.txt")

    val saveTo = "zip-partitioner/src/files/testFilesAfterZip/zipFile.zip"

    def computeHash(file: Path, algorithm: String = "SHA-256"): IO[String] = {
      Files[IO].readAll(file)
        .through(fs2.hash.digest[IO](MessageDigest.getInstance(algorithm)))
        .compile
        .to(Array)
        .map(bytes => bytes.map("%02x".format(_)).mkString)
    }

    def listFiles(directory: Path): Stream[IO, Path] = {
      Files[IO].walk(directory).filter(Files[IO].isRegularFile(_).unsafeRunSync())
    }

    def hashFilesInDirectory(directory: String): IO[Map[String, String]] = {
      val dirPath = Path(directory)
      listFiles(dirPath).evalMap { file =>
        computeHash(file).flatMap { hash =>
          IO((file.toString.split('/').last, hash))
        }
      }.compile.to(Map)
    }


    def unzip(zipFilePath: String, destDir: String): Unit = {
      val buffer = new Array[Byte](1024)

      // Ensure the destination directory exists
      val dir = new File(destDir)
      if (!dir.exists()) dir.mkdirs()

      val zis = new ZipInputStream(new FileInputStream(zipFilePath))

      try {
        var zipEntry: ZipEntry = zis.getNextEntry

        while (zipEntry != null) {
          val newFile: File = newFileF(new File(destDir), zipEntry)

          if (zipEntry.isDirectory) {
            if (!newFile.isDirectory && !newFile.mkdirs()) {
              throw new IOException("Failed to create directory " + newFile)
            }
          } else {
            // Fix for Windows-created archives
            val parent = newFile.getParentFile
            if (!parent.isDirectory && !parent.mkdirs()) {
              throw new IOException("Failed to create directory " + parent)
            }

            // Write file content
            val fos = new FileOutputStream(newFile)
            try {
              var len = zis.read(buffer)
              while (len > 0) {
                fos.write(buffer, 0, len)
                len = zis.read(buffer)
              }
            } finally {
              fos.close()
            }
          }

          zipEntry = zis.getNextEntry
        }
      } finally {
        zis.closeEntry()
        zis.close()
      }
    }

    def newFileF(destinationDir: File, zipEntry: ZipEntry): File = {
      val destFile = new File(destinationDir, zipEntry.getName)

      val destDirPath = destinationDir.getCanonicalPath
      val destFilePath = destFile.getCanonicalPath

      if (!destFilePath.startsWith(destDirPath + File.separator)) {
        throw new IOException("Entry is outside of the target dir: " + zipEntry.getName)
      }

      destFile
    }

    def mapsAreConsistent(smallMap: Map[String, String], largeMap: Map[String, String]): Boolean = {
      smallMap.forall { case (key, value) =>
        largeMap.get(key).contains(value)
      }
    }

  }
}

//List(
// FileArchive(src/files/file1.txt,eJxVijEKBEEQAnP/Mk+43xxFj8ENHSx+/+jdaEUsBcsv0XxzNvXLvg6VMoUYOhsHR1SeWSnG6JPGS/dq4UtYCzrY7M6EaS2sKWYg5vwHu4kyVw==),
// FileArchive(src/files/file2.txt,eJwdicENADEMwv7ZpWvRiz8RL9Y/NRI2SFzCTQ9plBIZEbfIghYUl+iNCwkPcj9t6lWd3bw9MrXnqaflk/kBaTgstA==))
// does not contain
// FileArchive(src/files/file1.txt,eJxLy8xJNQQABeYB0g==),
// FileArchive(src/files/file2.txt,eJxLy8xJNQIABecB0w==)
// but contains
// FileArchive(src/files/file2.txt,eJwdicENADEMwv7ZpWvRiz8RL9Y/NRI2SFzCTQ9plBIZEbfIghYUl+iNCwkPcj9t6lWd3bw9MrXnqaflk/kBaTgstA==),
// FileArchive(src/files/file1.txt,eJxVijEKBEEQAnP/Mk+43xxFj8ENHSx+/+jdaEUsBcsv0XxzNvXLvg6VMoUYOhsHR1SeWSnG6JPGS/dq4UtYCzrY7M6EaS2sKWYg5vwHu4kyVw==) (ZipPartitionSpec.scala:15)
//  [inf