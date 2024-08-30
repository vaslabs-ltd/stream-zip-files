package zip_partitioner.test_utils

import cats.effect.kernel.Async
import cats.syntax.all._
import eu.timepit.refined.types.string.NonEmptyString
import fs2.Collector
import zip_partitioner.ZipStore

import java.util.zip.ZipInputStream

object StoreTests {

  def testStore[F[_] : Async](
                       store: ZipStore[F],
                       uncompressedContainer: NonEmptyString,
                       compressedContainer: NonEmptyString,
                       files: List[NonEmptyString]
                     )(implicit compiler: fs2.Compiler[F, F]): F[RawTestData] = {
    val transferAll = fs2.Stream.emits(files).covary[F].flatMap {
      uncompressedFile =>
        store.transfer(uncompressedContainer, compressedContainer, uncompressedFile)
    }.compile.drain

    val retrieveAll: F[Map[NonEmptyString, String]] = store.retrieveMultiple(compressedContainer, files, 1024).through(
      fs2.io.toInputStream
    ).map(new ZipInputStream(_)).map(checkFiles(files, _)).compile.lastOrError

    val retrieveSingleAll: F[Map[NonEmptyString, String]] = files.map {
      file: NonEmptyString =>
        val bytesF: F[Array[Byte]] = store.retrieveSingle(compressedContainer, file).compile.to(Array)
        bytesF.map(bytes => file -> new String(bytes))
    }.sequence.map(_.toMap)

    for {
      _ <- transferAll
      zip <- retrieveAll
      allFilesUncompressed <- retrieveSingleAll
    } yield RawTestData(zip, allFilesUncompressed)
  }

  private def checkFiles(files: List[NonEmptyString], zip: ZipInputStream): Map[NonEmptyString, String] =
    files.map { _ =>
      val entry = zip.getNextEntry.getName
      val content = zip.readAllBytes()
      (NonEmptyString.unsafeFrom(entry), new String(content))
    }.toMap
}

case class RawTestData(zipFileContent: Map[NonEmptyString, String], uncompressedFileContent: Map[NonEmptyString, String])
