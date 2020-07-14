package org.constellation.snapshotstreaming.es

import cats.Parallel
import cats.effect.{Concurrent, ConcurrentEffect, IO}
import cats.implicits._
import cats.effect.implicits._
import fs2.Stream
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.consensus.StoredSnapshot
import org.constellation.domain.snapshot.SnapshotInfo
import org.constellation.snapshotstreaming.Configuration
import org.constellation.snapshotstreaming.mapper.{
  AddressBalance,
  SnapshotInfoMapper,
  StoredSnapshotMapper
}
import org.constellation.snapshotstreaming.s3.S3DeserializedResult
import org.constellation.snapshotstreaming.schema.{
  CheckpointBlock,
  Snapshot,
  Transaction
}
import org.http4s._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.client._
import org.http4s.headers.{`Content-Length`, `Content-Type`}

case class ElasticSearchDAO[F[_]: ConcurrentEffect: Parallel](client: Client[F])(
  storedSnapshotMapper: StoredSnapshotMapper,
  snapshotInfoMapper: SnapshotInfoMapper,
  config: Configuration,
)(implicit F: Concurrent[F]) {

  val logger = Slf4jLogger.getLogger[F]

  def mapAndSendToElasticSearch(
    s3Result: S3DeserializedResult
  ): Stream[F, Unit] = {
    val snapshot =
      storedSnapshotMapper.mapSnapshot(s3Result.snapshot, s3Result.lastModified)
    val checkpointBlocks =
      storedSnapshotMapper.mapCheckpointBlock(
        s3Result.snapshot,
        s3Result.lastModified
      )
    val transactions = storedSnapshotMapper.mapTransaction(
      s3Result.snapshot,
      s3Result.lastModified
    )
    val balances = snapshotInfoMapper.mapAddressBalances(s3Result.snapshotInfo)

    for {
      _ <- Stream.eval(
        transactions
          .grouped(config.maxParallelRequests)
          .toList
          .traverse(
            part =>
              part
                .map(t => sendTransaction(client)(t.hash, t, snapshot))
                .toList
                .parSequence
          )
      )
      _ <- Stream.eval(
        checkpointBlocks
          .grouped(config.maxParallelRequests)
          .toList
          .traverse(
            part =>
              part
                .map(t => sendCheckpointBlock(client)(t.hash, t, snapshot))
                .toList
                .parSequence
          )
      )
      _ <- Stream.eval(sendBalances(client)(snapshot.hash, balances))
      _ <- Stream.eval(sendSnapshot(client)(snapshot.hash, snapshot))
    } yield ()
  }

  private def sendSnapshot(client: Client[F])(hash: String,
                                              snapshot: Snapshot) =
    sendToElasticSearch(client)(
      hash,
      config.elasticsearchSnapshotsIndex,
      snapshot
    ).flatTap(
        _ =>
          logger.debug(s"[snapshot -> ES] $hash (height ${snapshot.height}) OK")
      )
      .handleErrorWith(e => {
        logger
          .error(e)(s"[cb -> ES] $hash (height ${snapshot.height}) ERROR") >> F
          .raiseError(e)
      })

  private def sendCheckpointBlock(client: Client[F])(
    checkpointHash: String,
    checkpointBlock: CheckpointBlock,
    snapshot: Snapshot
  ) =
    sendToElasticSearch(client)(
      checkpointHash,
      config.elasticsearchCheckpointBlocksIndex,
      checkpointBlock
    ).flatTap(
        _ =>
          logger.debug(
            s"[cb -> ES] $checkpointHash for snapshot ${snapshot.hash} OK"
        )
      )
      .handleErrorWith(e => {
        logger.error(e)(
          s"[cb -> ES] $checkpointHash for snapshot ${snapshot.hash} ERROR"
        ) >> F.raiseError(e)
      })

  private def sendTransaction(client: Client[F])(transactionHash: String,
                                                 transaction: Transaction,
                                                 snapshot: Snapshot): F[Unit] =
    sendToElasticSearch(client)(
      transactionHash,
      config.elasticsearchTransactionsIndex,
      transaction
    ).flatTap(
        _ =>
          logger.debug(s"[tx -> ES] $transactionHash for snapshot $snapshot OK")
      )
      .handleErrorWith(
        e => {
          logger
            .error(e)(
              s"[tx -> ES] $transactionHash for snapshot $snapshot ERROR"
            ) >> F.raiseError(e)
        }
      )

  private def sendBalances(
    client: Client[F]
  )(snapshotHash: String, balances: Map[String, AddressBalance]) =
    sendToElasticSearch(client)(
      snapshotHash,
      config.elasticsearchBalancesIndex,
      balances
    ).flatTap(
        _ => logger.debug(s"[balances -> ES] for snapshot $snapshotHash OK")
      )
      .handleErrorWith(e => {
        logger
          .error(e)(s"[balances -> ES] for snapshot $snapshotHash ERROR") >> F
          .raiseError(e)
      })

  private def sendToElasticSearch[T](client: Client[F])(
    id: String,
    index: String,
    entity: T
  )(implicit w: EntityEncoder[F, T]): F[Unit] = {
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

    client.status(request).void
  }

}
