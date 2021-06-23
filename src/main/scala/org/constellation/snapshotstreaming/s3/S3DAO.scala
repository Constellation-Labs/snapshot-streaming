package org.constellation.snapshotstreaming.s3

import java.util.Date
import cats.effect.Concurrent
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{GetObjectRequest, ListObjectsV2Request, S3ObjectSummary}
import com.amazonaws.util.IOUtils
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import fs2._
import org.constellation.schema.GenesisObservation
import org.constellation.schema.snapshot.{SnapshotInfo, StoredSnapshot}
import org.constellation.snapshotstreaming.serializer.{KryoSerializer, Serializer}

import scala.collection.JavaConverters._

case class S3SummariesResult(height: Long, snapshot: S3ObjectSummary, snapshotInfo: S3ObjectSummary)

case class S3DeserializedResult(height: Long, snapshot: StoredSnapshot, snapshotInfo: SnapshotInfo, lastModified: Date)

case class S3GenesisDeserializedResult(genesisObservation: GenesisObservation, lastModified: Date)

case class S3DAO[F[_]: RaiseThrowable](client: AmazonS3)(
  bucket: String
)(implicit val F: Concurrent[F]) {

  private val prefix = "snapshots/"
  private val snapshotSuffix = "snapshot"
  private val snapshotInfoSuffix = "snapshot_info"
  private val genesisKey = "genesis/genesis"
  private val logger = Slf4jLogger.getLogger[F]

  def getBucketName: String = bucket

  def get(height: Long): Stream[F, S3DeserializedResult] =
    for {
      summaries <- getObjectSummaries(height)
      deserialized <- deserializeResult(summaries)
    } yield deserialized

  def getGenesis(): Stream[F, S3GenesisDeserializedResult] =
    for {
      data <- Stream.eval(
        F.delay(
          client.getObject(
            new GetObjectRequest(bucket, genesisKey)
          )
        )
      )
      lastModified = data.getObjectMetadata.getLastModified
      is <- Stream.eval(
        F.delay(
          data.getObjectContent
        )
      )
      consumed <- Stream.bracket(
        F.delay(
          IOUtils.toByteArray(is)
        )
      )(_ => F.delay(is.close()))
      genesis <- Stream.eval(
        F.delay(
          KryoSerializer.deserializeCast[GenesisObservation](consumed)
        )
      )
    } yield S3GenesisDeserializedResult(genesis, lastModified)

  private def getObjectSummaries(height: Long): Stream[F, S3SummariesResult] =
    for {
      data <- Stream.eval(
        F.delay(
          client.listObjectsV2(
            new ListObjectsV2Request()
              .withPrefix(s"$prefix$height-")
              .withBucketName(bucket)
          )
        )
      )
      summaries = data.getObjectSummaries.asScala

      _ <- if (summaries.size > 2) {
        Stream.raiseError(HeightDuplicatedInBucket(height, bucket, summaries.map(_.getKey).toSet))
      } else Stream.emit(())

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

      _ <- if (!areHashesTheSame(snapshot.getKey, snapshotInfo.getKey)) {
        Stream.raiseError(HashInconsistency(height, snapshotInfo.getKey, snapshot.getKey))
      } else Stream.emit(())

    } yield S3SummariesResult(height, snapshot, snapshotInfo)

  private def areHashesTheSame(snapshotKey: String, snapshotInfoKey: String): Boolean =
    snapshotKey.slice(0, snapshotKey.lastIndexOf(snapshotSuffix)) == snapshotInfoKey
      .slice(0, snapshotInfoKey.lastIndexOf(snapshotInfoSuffix))

  private def deserializeResult(
    result: S3SummariesResult
  ): Stream[F, S3DeserializedResult] =
    for {
      snapshot <- getObjectDeserialized[StoredSnapshot](result.snapshot.getKey)
      snapshotInfo <- getObjectDeserialized[SnapshotInfo](
        result.snapshotInfo.getKey
      )
      lastModified = Seq(
        result.snapshot.getLastModified,
        result.snapshotInfo.getLastModified
      ).max
    } yield S3DeserializedResult(result.height, snapshot, snapshotInfo, lastModified)

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
        KryoSerializer.deserializeCast[A](consumed)
      })
    } yield deserialized

}
