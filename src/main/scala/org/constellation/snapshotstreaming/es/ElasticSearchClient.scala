package org.constellation.snapshotstreaming.es

import cats.Parallel
import cats.effect.{Concurrent, ConcurrentEffect, Resource}
import cats.implicits._
import fs2.Stream
import org.constellation.consensus.StoredSnapshot
import org.constellation.domain.snapshot.SnapshotInfo
import org.constellation.snapshotstreaming.Configuration
import org.constellation.snapshotstreaming.mapper.{
  AddressBalance,
  SnapshotInfoMapper,
  StoredSnapshotMapper
}
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

class ElasticSearchClient[F[_]: Concurrent: ConcurrentEffect: Parallel](
  storedSnapshotMapper: StoredSnapshotMapper,
  snapshotInfoMapper: SnapshotInfoMapper,
  config: Configuration
) {

  val logger: Logger = LoggerFactory.getLogger(getClass)
  val client: Resource[F, Client[F]] = BlazeClientBuilder[F](global).resource

  def mapAndSendToElasticSearch(storedSnapshot: StoredSnapshot,
                                snapshotInfo: SnapshotInfo): Stream[F, Unit] = {
    val snapshot = storedSnapshotMapper.mapSnapshot(storedSnapshot)
    val checkpointBlocks =
      storedSnapshotMapper.mapCheckpointBlock(storedSnapshot)
    val transactions = storedSnapshotMapper.mapTransaction(storedSnapshot)
    val balances = snapshotInfoMapper.mapAddressBalances(snapshotInfo)

    for {
      _ <- Stream
        .emits(transactions)
        .map(t => sendTransaction(t.hash, t))
        .parJoinUnbounded

      _ <- Stream
        .emits(checkpointBlocks)
        .map(b => sendCheckpointBlock(b.hash, b))
        .parJoinUnbounded

      _ <- Stream(
        sendSnapshot(snapshot.hash, snapshot),
        sendBalances(snapshot.hash, balances)
      ).parJoinUnbounded
    } yield ()
  }

  private def sendSnapshot(hash: String, snapshot: Snapshot) =
    sendToElasticSearch(hash, config.elasticsearchSnapshotsIndex, snapshot)

  private def sendCheckpointBlock(checkpointHash: String,
                                  checkpointBlock: CheckpointBlock) =
    sendToElasticSearch(
      checkpointHash,
      config.elasticsearchCheckpointBlocksIndex,
      checkpointBlock
    )

  private def sendTransaction(transactionHash: String,
                              transaction: Transaction) =
    sendToElasticSearch(
      transactionHash,
      config.elasticsearchTransactionsIndex,
      transaction
    )

  private def sendBalances(snapshotHash: String,
                           balances: Map[String, AddressBalance]) =
    sendToElasticSearch(
      snapshotHash,
      config.elasticsearchBalancesIndex,
      balances
    )

  private def sendToElasticSearch[T](id: String, index: String, entity: T)(
    implicit w: EntityEncoder[F, T]
  ): Stream[F, Response[F]] =
    for {
      client <- BlazeClientBuilder[F](global).stream
      request = Request[F]()
        .withUri(
          Uri.unsafeFromString(
            s"${config.elasticsearchUrl}/$index/_doc/$id?op_type=index"
          )
        )
        .withEntity(entity)
        .withContentType(`Content-Type`(MediaType.application.json))
        .removeHeader(`Content-Length`)
        .withMethod(Method.PUT)
      response <- client.stream(request)
    } yield response
}
