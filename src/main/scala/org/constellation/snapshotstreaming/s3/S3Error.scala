package org.constellation.snapshotstreaming.s3

trait S3Error extends Throwable

case class HeightDuplicatedInBucket(height: Long, bucket: String, objects: Set[String]) extends S3Error {
  override def getMessage: String = s"Height $height exists more then once in $bucket. Result: $objects"
}

case class HashInconsistency(height: Long, snapshotInfoObject: String, snapshotObject: String) extends S3Error {
  override def getMessage: String =
    s"Hashes of SnapshotInfo ($snapshotInfoObject) and Snapshot ($snapshotObject) are different for height $height"
}
