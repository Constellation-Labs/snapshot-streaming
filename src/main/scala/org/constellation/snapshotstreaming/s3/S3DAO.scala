package org.constellation.snapshotstreaming.s3

import cats.data.OptionT
import cats.effect.{Concurrent, IO}
import cats.implicits._
import com.amazonaws.services.s3.model.{
  ListObjectsV2Request,
  ListObjectsV2Result,
  S3ObjectSummary
}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.util.IOUtils
import fs2._
import org.constellation.consensus.StoredSnapshot
import org.constellation.domain.snapshot.SnapshotInfo
import org.constellation.snapshotstreaming.serializer.Serializer

import scala.collection.JavaConverters._

case class S3SummariesResult(height: Long,
                             snapshot: S3ObjectSummary,
                             snapshotInfo: S3ObjectSummary)

case class S3DeserializedResult(height: Long,
                                snapshot: StoredSnapshot,
                                snapshotInfo: SnapshotInfo)

case class S3DAO[F[_]: RaiseThrowable](client: AmazonS3)(
  bucket: String,
  serializer: Serializer
)(implicit val F: Concurrent[F]) {

  private val prefix = "snapshots/"
  private val snapshotSuffix = "snapshot"
  private val snapshotInfoSuffix = "snapshot_info"
  private val logger = Slf4jLogger.getLogger[F]

  def getBucketName: String = bucket

  def get(height: Long): Stream[F, S3DeserializedResult] =
    for {
      summaries <- getObjectSummaries(height)
      deserialized <- deserializeResult(summaries)
    } yield deserialized

  private def getObjectSummaries(height: Long): Stream[F, S3SummariesResult] =
    for {
      data <- Stream.eval(
        F.delay(
          client.listObjectsV2(
            new ListObjectsV2Request()
              .withPrefix(s"$prefix$height")
              .withBucketName(bucket)
              .withMaxKeys(2) // Limit to snapshot and snapshot_info
          )
        )
      )
      summaries = data.getObjectSummaries.asScala

      snapshot <- Stream.emit(
        summaries
          .find(_.getKey.endsWith(snapshotSuffix))
          .get
      )
      snapshotInfo <- Stream.emit(
        summaries
          .find(_.getKey.endsWith(snapshotInfoSuffix))
          .get
      )
    } yield S3SummariesResult(height, snapshot, snapshotInfo)

  private def deserializeResult(
    result: S3SummariesResult
  ): Stream[F, S3DeserializedResult] =
    for {
      snapshot <- getObjectDeserialized[StoredSnapshot](result.snapshot.getKey)
      snapshotInfo <- getObjectDeserialized[SnapshotInfo](
        result.snapshotInfo.getKey
      )
    } yield S3DeserializedResult(result.height, snapshot, snapshotInfo)

  private def getObjectDeserialized[A](
    key: String
  )(implicit F: Concurrent[F]): Stream[F, A] =
    for {
      is <- Stream.eval(F.delay {
        client.getObject(bucket, key).getObjectContent
      })
      consumed <- Stream.bracket(F.delay {
        IOUtils.toByteArray(is)
      })(
        _ =>
          F.delay {
            is.close()
        }
      )
      deserialized <- Stream.eval(F.delay {
        serializer.deserialize[A](consumed)
      })
    } yield deserialized

}
