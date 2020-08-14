package org.constellation.snapshotstreaming.es

import cats.Parallel
import cats.effect.{Concurrent, ConcurrentEffect}
import cats.implicits._
import com.sksamuel.elastic4s.ElasticApi.updateById
import com.sksamuel.elastic4s.ElasticDsl.{bulk, _}
import com.sksamuel.elastic4s.circe._
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.requests.bulk.BulkResponse
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties, Response}
import fs2.Stream
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.consensus.{StoredSnapshot, Snapshot => OriginalSnapshot}
import org.constellation.domain.snapshot.SnapshotInfo
import org.constellation.primitives.Schema.{CheckpointCache, Height}
import org.constellation.schema.Id
import org.constellation.snapshotstreaming.Configuration
import org.constellation.snapshotstreaming.mapper.{AddressBalance, SnapshotInfoMapper, StoredSnapshotMapper}
import org.constellation.snapshotstreaming.s3.{S3DeserializedResult, S3GenesisDeserializedResult}
import org.constellation.snapshotstreaming.schema.{CheckpointBlock, Snapshot, Transaction}

import scala.collection.SortedMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.util.{Failure, Success}

case class ElasticSearchDAO[F[_]: ConcurrentEffect: Parallel](
  storedSnapshotMapper: StoredSnapshotMapper,
  snapshotInfoMapper: SnapshotInfoMapper,
  config: Configuration,
)(implicit F: Concurrent[F]) {

  val eclient: ElasticClient = ElasticClient(
    JavaClient(ElasticProperties(s"${config.elasticsearchUrl}:${config.elasticsearchPort}"))
  )

  val logger = Slf4jLogger.getLogger[F]

  def mapGenesisAndSendToElasticSearch(
                                        s3Result: S3GenesisDeserializedResult
                                      ): Stream[F, Response[BulkResponse]] = {
    val genesisCb = s3Result.genesisObservation.genesis
    val initialDistributionCb = s3Result.genesisObservation.initialDistribution
    val initialDistribution2Cb = s3Result.genesisObservation.initialDistribution2
    val checkpointBlocks = Seq(
      genesisCb,
      initialDistributionCb,
      initialDistribution2Cb
    )
    val cbHashes = checkpointBlocks.map(_.baseHash)
    val checkpointCaches = Seq(
      CheckpointCache(genesisCb, height = Height(0L, 0L).some),
      CheckpointCache(initialDistributionCb, height = Height(1L, 1L).some),
      CheckpointCache(initialDistribution2Cb, height = Height(1L, 1L).some)
    )

    val storedSnapshot =
      StoredSnapshot(
        snapshot = OriginalSnapshot("", cbHashes, SortedMap.empty[Id, Double]),
        checkpointCache = checkpointCaches
      )

    val mockedS3SnapshotResult =
      S3DeserializedResult(
        height = 1L,
        snapshot = storedSnapshot,
        snapshotInfo = SnapshotInfo(storedSnapshot),
        s3Result.lastModified
      )

    mapAndSendToElasticSearch(mockedS3SnapshotResult)
  }

  def mapAndSendToElasticSearch(
    s3Result: S3DeserializedResult
  ): Stream[F, Response[BulkResponse]] = {
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


    Stream.eval {
      F.async[Response[BulkResponse]] { cb =>
        eclient.execute {
          bulkSendToElasticSearch(transactions, checkpointBlocks, snapshot, balances)
        }.onComplete {
          case Success(a) => cb(Right(a))
          case Failure(e) => cb(Left(e))
        }
      }
    }
  }

  private def bulkSendToElasticSearch[T](
    transactions: Seq[Transaction],
    checkpointBlocks: Seq[CheckpointBlock],
    snapshot: Snapshot,
    balances: Map[String, AddressBalance],
  ) =
    bulk(
      transactions.map(t => updateById(config.elasticsearchTransactionsIndex, t.hash).docAsUpsert(t))
        ++ checkpointBlocks.map(b => updateById(config.elasticsearchCheckpointBlocksIndex, b.hash).docAsUpsert(b))
        ++ Seq(updateById(config.elasticsearchBalancesIndex, snapshot.hash).docAsUpsert(balances))
        ++ Seq(updateById(config.elasticsearchSnapshotsIndex, snapshot.hash).docAsUpsert(snapshot))
    )
}
