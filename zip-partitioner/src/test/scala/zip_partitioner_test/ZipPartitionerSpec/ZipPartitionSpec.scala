package zip_partitioner_test.ZipPartitionerSpec

import cats.effect.unsafe.implicits.global
import org.specs2.mutable.Specification
import zip_partitioner.ZipPartitioner

import java.nio.charset.StandardCharsets
import java.util.UUID

object ZipPartitionerSpec extends Specification {

  "ZipPartitioner" should {
    "createStreamArchive" in {
      val initialData = UUID.randomUUID().toString
      fs2.Stream.emits(initialData.getBytes(StandardCharsets.UTF_8))
        .through(ZipPartitioner.deflateStream)
        .through(ZipPartitioner.inflateStream)
        .through(fs2.text.utf8.decode)
        .compile.lastOrError.unsafeRunSync() must_== initialData
    }
  }
}