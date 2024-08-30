package zip_partitioner

import eu.timepit.refined.types.string.NonEmptyString

trait ZipStore[F[_]] {

  type TransferOut

  def transfer(from: NonEmptyString, to: NonEmptyString, fileIdentifier: NonEmptyString): fs2.Stream[F, TransferOut]

  def retrieveSingle(compressedBucket: NonEmptyString, fileKey: NonEmptyString): fs2.Stream[F, Byte]

  // create a zip file in the form of a byte stream out of several file keys retrieved from s3
  def retrieveMultiple(compressedBucket: NonEmptyString, fileKeys: List[NonEmptyString], streamSize: Int): fs2.Stream[F, Byte]

}
