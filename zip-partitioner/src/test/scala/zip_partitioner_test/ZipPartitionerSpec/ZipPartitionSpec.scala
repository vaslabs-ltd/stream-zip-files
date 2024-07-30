package zip_partitioner_test.ZipPartitionerSpec
import cats.effect.unsafe.implicits.global
import zip_partitioner.ZipPartitioner.{compress, createStreamArchive, encodeBase64}
import org.specs2.mutable.Specification
import zip_partitioner.FileArchive

object ZipPartitionerSpec extends Specification {

  "ZipPartitioner" should {
    "createStreamArchive" in {
      val filePaths = List("src/files/file1.txt", "src/files/file2.txt")
      val result = createStreamArchive(filePaths)
      result.compile.toList.unsafeRunSync().size must_== 2

//      result.compile.toList.unsafeRunSync() must_==(
      //        FileArchive("src/files/file1.txt", encodeBase64(compress("file1".getBytes))),
      //        FileArchive("src/files/file2.txt", encodeBase64(compress("file2".getBytes)))
      //      )
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