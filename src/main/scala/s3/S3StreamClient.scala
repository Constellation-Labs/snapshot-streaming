package s3

import cats.data.OptionT
import cats.effect.Concurrent
import cats.implicits._
import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result, S3ObjectSummary}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.util.IOUtils
import fs2._
import org.constellation.consensus.StoredSnapshot
import serializer.Serializer

import scala.collection.JavaConverters._

case class S3StreamClient[F[_]](
  region: String,
  bucket: String,
  serializer: Serializer
)(implicit val F: Concurrent[F]) {

  private val prefix = "snapshots/"
  private val suffix = "snapshot"

  def getSnapshot(height: Long): Stream[F, StoredSnapshot] =
    for {
      summary <- getSnapshotObjectSummary(height)
      snapshot <- getObjectDeserialized[StoredSnapshot](summary.getKey)
    } yield snapshot

  def getSnapshotObjectSummary(height: Long): Stream[F, S3ObjectSummary] =
    for {
      client <- getClient
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
      snapshot <- Stream.emit(
        data.getObjectSummaries.asScala.find(_.getKey.endsWith(suffix)).get
      )

    } yield snapshot

  def getSnapshotObjectsSummaries: Stream[F, List[S3ObjectSummary]] =
    for {
      client <- getClient
      request <- Stream.emit {
        new ListObjectsV2Request()
          .withPrefix(prefix)
          .withBucketName(bucket)
      }
      firstChunk <- Stream.eval(F.delay(client.listObjectsV2(request)))
      allChunks <- Stream.emit(firstChunk) ++ Stream.unfoldEval(
        continuation(firstChunk)
      )(next(client, request))
      snapshots <- Stream.emit(
        allChunks.getObjectSummaries.asScala.toList
          .filter(_.getKey.endsWith(suffix))
      )
    } yield snapshots

  def getObjectDeserialized[A](key: String)(implicit F: Concurrent[F]): Stream[F, A] =
    for {
      client <- getClient
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
      deserialized <- Stream.eval(F.delay { serializer.deserialize[A](consumed) })
    } yield deserialized

  private def continuation(response: ListObjectsV2Result): Option[String] =
    Some(response).filter(_.isTruncated).map(_.getContinuationToken)

  private def next(client: AmazonS3, request: ListObjectsV2Request)(
    continuationToken: Option[String]
  ): F[Option[(ListObjectsV2Result, Option[String])]] =
    (for {
      continuationToken <- OptionT.fromOption[F](continuationToken)
      continuationRequest = request.withContinuationToken(continuationToken)
      response <- OptionT.liftF(F.delay {
        client.listObjectsV2(continuationRequest)
      })
      nextContinuationToken = continuation(response)
    } yield (response, nextContinuationToken)).value

  private def getClient(implicit F: Concurrent[F]): Stream[F, AmazonS3] =
    Stream.bracket {
      AmazonS3ClientBuilder
        .standard()
        .withRegion(region)
        .build()
        .pure[F]
    }(
      c =>
        F.delay {
          c.shutdown()
      }
    )
}
