package org.constellation.snapshotstreaming.opensearch

import java.util.Date
import cats.effect.Async
import cats.syntax.flatMap._
import cats.syntax.functor._
import org.tessellation.kryo.KryoSerializer
import com.sksamuel.elastic4s.ElasticApi.updateById
import com.sksamuel.elastic4s.circe._
import com.sksamuel.elastic4s.requests.update.UpdateRequest
import org.constellation.snapshotstreaming.SnapshotProcessor.GlobalSnapshotWithState
import org.constellation.snapshotstreaming.opensearch.mapper.{CurrencySnapshotMapper, GlobalSnapshotMapper}
import org.constellation.snapshotstreaming.opensearch.schema._
import org.constellation.snapshotstreaming.Configuration
import org.tessellation.currency.schema.currency.CurrencyTransaction
import org.tessellation.schema.transaction.DAGTransaction

trait UpdateRequestBuilder[F[_]] {

  def bulkUpdateRequests(
    globalSnapshotWithState: GlobalSnapshotWithState,
    timestamp: Date
  ): F[Seq[Seq[UpdateRequest]]]

}

object UpdateRequestBuilder {

  def make[F[_]: Async: KryoSerializer](config: Configuration): UpdateRequestBuilder[F] =
    make(GlobalSnapshotMapper.make(), CurrencySnapshotMapper.make(), config)

  def make[F[_]: Async](globalMapper: GlobalSnapshotMapper[F], currencyMapper: CurrencySnapshotMapper[F], config: Configuration): UpdateRequestBuilder[F] =
    new UpdateRequestBuilder[F] {

      def bulkUpdateRequests(
        globalSnapshotWithState: GlobalSnapshotWithState,
        timestamp: Date
      ): F[Seq[Seq[UpdateRequest]]] =
        for {
          _ <- Async[F].unit
          GlobalSnapshotWithState(globalSnapshot, snapshotInfo, currencySnapshots) = globalSnapshotWithState

          mappedGlobalData <- globalMapper.mapGlobalSnapshot(globalSnapshot, snapshotInfo, timestamp)
          (snapshot, blocks, transactions, balances) = mappedGlobalData

          mappedCurrencyData <- currencyMapper.mapCurrencySnapshots(currencySnapshots, timestamp)
          (currSnapshot, currBlocks, currTransactions, currBalances) = mappedCurrencyData

        } yield updateRequests(snapshot, blocks, transactions, balances, currSnapshot, currBlocks, currTransactions, currBalances).grouped(config.bulkSize).toSeq

      def updateRequests[T](
       snapshot: Snapshot,
       blocks: Seq[Block],
       transactions: Seq[Transaction[DAGTransaction]],
       balances: Seq[AddressBalance],
       currencySnapshots: Seq[CurrencyData[Snapshot]],
       currencyBlocks: Seq[CurrencyData[Block]],
       currencyTransactions: Seq[CurrencyData[Transaction[CurrencyTransaction]]],
       currencyBalances: Seq[CurrencyData[AddressBalance]]
      ): Seq[UpdateRequest] =
        Seq(updateById(config.snapshotsIndex, snapshot.hash).docAsUpsert(snapshot)) ++
          blocks.map(block => updateById(config.blocksIndex, block.hash).docAsUpsert(block)) ++
          transactions.map(transaction =>
            updateById(config.transactionsIndex, transaction.hash).docAsUpsert(transaction)
          ) ++
          balances.map(balance => updateById(config.balancesIndex, balance.docId).docAsUpsert(balance)) ++
          currencySnapshots.map { case cd @ CurrencyData(identifier, data) =>
            val id = s"$identifier${data.hash}"
            updateById(config.currencySnapshotsIndex, id).docAsUpsert(cd)
          } ++
          currencyBlocks.map { case cd @ CurrencyData(identifier, data) =>
            val id = s"$identifier${data.hash}"
            updateById(config.currencyBlocksIndex, id).docAsUpsert(cd)
          } ++
          currencyTransactions.map { case cd @ CurrencyData(identifier, data) =>
            val id = s"$identifier${data.hash}"
            updateById(config.currencyTransactionsIndex, id).docAsUpsert(cd)
          } ++
          currencyBalances.map { case cd @ CurrencyData(identifier, data) =>
            val id = s"$identifier${data.docId}"
            updateById(config.currencyBalancesIndex, id).docAsUpsert(cd)
          }

    }

}
