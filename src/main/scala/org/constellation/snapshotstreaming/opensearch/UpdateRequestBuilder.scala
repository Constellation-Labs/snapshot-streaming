package org.constellation.snapshotstreaming.opensearch

import cats.effect.Async
import cats.syntax.all._
import io.constellationnetwork.security.Hasher
import com.sksamuel.elastic4s.ElasticApi.updateById
import com.sksamuel.elastic4s.circe._
import com.sksamuel.elastic4s.requests.update.UpdateRequest
import org.constellation.snapshotstreaming.OpenSearchConfig
import org.constellation.snapshotstreaming.schema.{AddressBalance, Block, CurrencyData, CurrencySnapshot, FeeTransaction, Snapshot, Transaction}
import org.constellation.snapshotstreaming.schema.schema.{GlobalData, MetagraphData}

case class UpdateRequests(
  sequentialRequests: Seq[Seq[UpdateRequest]],
  parallelRequests: List[List[UpdateRequest]]
)

trait UpdateRequestBuilder {

  def bulkUpdateRequests(
    mappedGlobalData: GlobalData,
    mappedCurrencyData: MetagraphData
  ): UpdateRequests

}

object UpdateRequestBuilder {

  def make(config: OpenSearchConfig): UpdateRequestBuilder =
    new UpdateRequestBuilder {

      def bulkUpdateRequests(
        mappedGlobalData: GlobalData,
        mappedCurrencyData: MetagraphData
      ): UpdateRequests = {

        val GlobalData(snapshot, blocks, transactions, balances, proofs, _, _, _) =
          mappedGlobalData

        val MetagraphData(
          currIncrementalSnapshots,
          currBlocks,
          currTransactions,
          currFeeTransactions,
          currBalances,
          _,
          _,
          _
        ) =
          mappedCurrencyData

        val parallelRequests = updateParallelRequests(
          blocks,
          transactions,
          balances,
          Seq(), // there's no full currSnapshot, TODO : validate
          currIncrementalSnapshots,
          currBlocks,
          currTransactions,
          currFeeTransactions,
          currBalances
        ).grouped(config.bulkSize).toList

        val sequentialRequests = updateSequentialRequests(
          snapshot
        ).grouped(config.bulkSize).toSeq
        UpdateRequests(
          sequentialRequests,
          parallelRequests
        )
      }

      def updateSequentialRequests(
        snapshot: Snapshot
      ): Seq[UpdateRequest] =
        Seq(updateById(config.indexes.snapshots, snapshot.hash).docAsUpsert(snapshot))

      def updateParallelRequests(
        blocks: Seq[Block],
        transactions: Seq[Transaction],
        balances: Seq[AddressBalance],
        currencySnapshots: Seq[CurrencyData[Snapshot]],
        currencyIncrementalSnapshots: Seq[CurrencyData[CurrencySnapshot]],
        currencyBlocks: Seq[CurrencyData[Block]],
        currencyTransactions: Seq[CurrencyData[Transaction]],
        currencyFeeTransactions: Seq[CurrencyData[FeeTransaction]],
        currencyBalances: Seq[CurrencyData[AddressBalance]]
      ): List[UpdateRequest] = {

        def balanceId(b: AddressBalance) = s"${b.address}${b.snapshotOrdinal}"

        blocks.toList.map(block => updateById(config.indexes.blocks, block.hash).docAsUpsert(block)) ++
          transactions.map(transaction =>
            updateById(config.indexes.transactions, transaction.hash).docAsUpsert(transaction)
          ) ++
          balances.map(balance => updateById(config.indexes.balances, balanceId(balance)).docAsUpsert(balance)) ++
          currencySnapshots.map { case cd @ CurrencyData(identifier, data) =>
            val id = s"$identifier${data.hash}"
            updateById(config.indexes.currency.snapshots, id).docAsUpsert(cd)
          } ++
          currencyIncrementalSnapshots.map { case cd @ CurrencyData(identifier, data) =>
            val id = s"$identifier${data.hash}"
            updateById(config.indexes.currency.snapshots, id).docAsUpsert(cd)
          } ++
          currencyBlocks.map { case cd @ CurrencyData(identifier, data) =>
            val id = s"$identifier${data.hash}"
            updateById(config.indexes.currency.blocks, id).docAsUpsert(cd)
          } ++
          currencyTransactions.map { case cd @ CurrencyData(identifier, data) =>
            val id = s"$identifier${data.hash}"
            updateById(config.indexes.currency.transactions, id).docAsUpsert(cd)
          } ++
          currencyFeeTransactions.map { case cd @ CurrencyData(identifier, data) =>
            val id = s"$identifier${data.hash}"
            updateById(config.indexes.currency.feeTransactions, id).docAsUpsert(cd)
          } ++
          currencyBalances.map { case cd @ CurrencyData(identifier, data) =>
            val id = s"$identifier${balanceId(data)}"
            updateById(config.indexes.currency.balances, id).docAsUpsert(cd)
          }
      }

    }

}
