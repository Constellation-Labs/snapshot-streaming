package org.constellation.snapshotstreaming.output

import cats.effect.{Concurrent, ConcurrentEffect, Resource}
import cats.implicits._
import fs2.Stream
import org.constellation.consensus.StoredSnapshot
import org.constellation.snapshotstreaming.Configuration
import org.constellation.snapshotstreaming.mapper.StoredSnapshotMapper
import org.http4s.implicits._
import org.constellation.snapshotstreaming.schema.{
  CheckpointBlock,
  Snapshot,
  Transaction
}
import org.http4s.{
  EntityEncoder,
  HeaderKey,
  MediaType,
  Method,
  Request,
  Response,
  Uri
}
import org.slf4j.{Logger, LoggerFactory}
import org.http4s.client.blaze._
import org.http4s.client._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.headers.{`Content-Length`, `Content-Type`}

import scala.concurrent.ExecutionContext.global

class ElasticSearchSender[F[_]: Concurrent: ConcurrentEffect](
  storedSnapshotMapper: StoredSnapshotMapper,
  config: Configuration
) {

  val logger: Logger = LoggerFactory.getLogger(getClass)
  val client: Resource[F, Client[F]] = BlazeClientBuilder[F](global).resource

  def mapAndSendToElasticSearch(
    storedSnapshot: StoredSnapshot
  ): Stream[F, Unit] = {
    val snapshot = storedSnapshotMapper.mapSnapshot(storedSnapshot)
    val checkpointBlocks =
      storedSnapshotMapper.mapCheckpointBlock(storedSnapshot)
    val transactions = storedSnapshotMapper.mapTransaction(storedSnapshot)

    for {
      _ <- Stream.eval(sendSnapshot(snapshot.hash, snapshot))
      _ <- Stream.eval {
        checkpointBlocks.toList.traverse(b => sendCheckpointBlock(b.hash, b))
      }
      _ <- Stream.eval {
        transactions.toList.traverse(t => sendTransaction(t.hash, t))
      }
    } yield ()
  }

  private def sendSnapshot(id: String, snapshot: Snapshot) =
    sendToElasticSearch(id, config.elasticsearchSnapshotsIndex, snapshot)

  private def sendCheckpointBlock(id: String,
                                  checkpointBlock: CheckpointBlock) =
    sendToElasticSearch(
      id,
      config.elasticsearchCheckpointBlocksIndex,
      checkpointBlock
    )

  private def sendTransaction(id: String, transaction: Transaction) =
    sendToElasticSearch(id, config.elasticsearchTransactionsIndex, transaction)

  // TODO: Make it Stream[F, Response[F]] maybe and use parJoinUnbounded
  private def sendToElasticSearch[T](id: String, index: String, entity: T)(
    implicit w: EntityEncoder[F, T]
  ): F[Unit] =
    for {
      r <- client.use { c =>
        val request = Request[F]()
          .withUri(
            Uri.unsafeFromString(
              s"${config.elasticsearchUrl}/$index/_doc/$id?op_type=index"
            )
          )
          .withEntity(entity)
          .withContentType(`Content-Type`(MediaType.application.json))
          .removeHeader(`Content-Length`)
          .withMethod(Method.PUT)

        c.expect[Unit](request)
      }
    } yield r
}
