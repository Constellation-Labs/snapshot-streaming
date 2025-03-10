package org.constellation.snapshotstreaming.mapper

import cats.effect.Async
import cats.syntax.all._
import org.tessellation.json.JsonSerializer
import org.tessellation.schema.address.Address
import org.tessellation.schema.balance.Balance
import org.tessellation.security.Hasher
import org.constellation.snapshotstreaming.SnapshotProcessor.GlobalSnapshotWithState
import org.constellation.snapshotstreaming.schema.schema.MetagraphData
import org.constellation.snapshotstreaming.schema.{AddressBalance, Block, CurrencyData, FeeTransaction, Transaction, CurrencySnapshot => OSCurrencySnapshot}

import java.time.LocalDateTime
import scala.collection.immutable.SortedMap

trait CurrencySnapshotMapper[F[_]] {

  def mapCurrencySnapshots(
    globalSnapshotWithState: GlobalSnapshotWithState,
    timestamp: LocalDateTime,
    txHasher: Hasher[F],
    hasher: Hasher[F]
  ): F[
    MetagraphData
  ]

}

object CurrencySnapshotMapper {

  def make[F[_]: Async: JsonSerializer](
  ): CurrencySnapshotMapper[F] =
    make(CurrencyFullSnapshotMapper.make(), CurrencyIncrementalSnapshotMapper.make())

  private def make[F[_]: Async](
    fullMapper: CurrencyFullSnapshotMapper[F],
    incrementalMapper: CurrencyIncrementalSnapshotMapper[F]
  ): CurrencySnapshotMapper[F] =
    new CurrencySnapshotMapper[F] {

      type Acc = (
        Seq[CurrencyData[OSCurrencySnapshot]],
        Seq[CurrencyData[Block]],
        Seq[CurrencyData[Transaction]],
        Seq[CurrencyData[FeeTransaction]],
        Seq[CurrencyData[AddressBalance]],
        Map[Address, SortedMap[Address, Balance]]
      )

      type CurrencySnapshotMapperResult = MetagraphData

      def mapCurrencySnapshots(
        globalSnapshotWithState: GlobalSnapshotWithState,
        timestamp: LocalDateTime,
        txHasher: Hasher[F],
        hasher: Hasher[F]
      ): F[CurrencySnapshotMapperResult] = {

        val GlobalSnapshotWithState(_, maybePrevLastSnapshots, _, currencySnapshots, _) =
          globalSnapshotWithState

        val maybeLastSnapshots = maybePrevLastSnapshots.map(_.lastCurrencySnapshots)
        val initialAccBalances = maybeLastSnapshots.map {
          _.map {
            case (identifier, Left(full))        => identifier -> full.info.balances
            case (identifier, Right((_, state))) => identifier -> state.balances
          }
        }.getOrElse(SortedMap.empty[Address, SortedMap[Address, Balance]])

        val initialAcc: Acc = (
          Seq.empty[CurrencyData[OSCurrencySnapshot]],
          Seq.empty[CurrencyData[Block]],
          Seq.empty[CurrencyData[Transaction]],
          Seq.empty[CurrencyData[FeeTransaction]],
          Seq.empty[CurrencyData[AddressBalance]],
          initialAccBalances
        )

        currencySnapshots.toList.flatMap { case (i, s) => s.toList.map((i, _)) }
          .foldLeftM[F, Acc](initialAcc) {
            case (
                  (aggSnap, aggBlocks, aggTxs, aggFeeTxs, aggBalances, aggLastBalances),
                  (identifier, fullOrIncremental)
                ) =>
              val identifierStr = identifier.value.value

              fullOrIncremental match {
                case Left(full) =>
                  for {
                    snapshot <- fullMapper
                      .mapSnapshot(full, timestamp, hasher)
                      .map(CurrencyData(identifierStr, _))
                    blocks <- fullMapper
                      .mapBlocks(full, timestamp, txHasher, hasher)
                      .map(_.map(CurrencyData(identifierStr, _)))
                    transactions <- fullMapper
                      .mapTransactions(full, timestamp, txHasher, hasher)
                      .map(_.map(CurrencyData(identifierStr, _)))
                    balances = fullMapper
                      .mapBalances(full, full.info.balances, timestamp)
                      .map(CurrencyData(identifierStr, _))
                  } yield (
                    aggSnap :+ snapshot,
                    aggBlocks ++ blocks,
                    aggTxs ++ transactions,
                    aggFeeTxs,
                    aggBalances ++ balances,
                    aggLastBalances + (identifier -> full.info.balances)
                  )

                case Right((incremental, info, binary)) =>
                  for {
                    snapshot <- incrementalMapper
                      .mapSnapshot(incremental, binary, info, timestamp, hasher)
                      .map(CurrencyData(identifierStr, _))
                    blocks <- incrementalMapper
                      .mapBlocks(incremental, timestamp, txHasher, hasher)
                      .map(_.map(CurrencyData(identifierStr, _)))
                    transactions <- incrementalMapper
                      .mapTransactions(incremental, timestamp, txHasher, hasher)
                      .map(_.map(CurrencyData(identifierStr, _)))
                    feeTransactions <- incrementalMapper
                      .mapFeeTransactions(incremental, timestamp, hasher)
                      .map(_.map(CurrencyData(identifierStr, _)))
                    prevBalances = aggLastBalances.get(identifier)
                    filteredBalances = incrementalMapper.balanceDiff(
                      incremental,
                      prevBalances,
                      info
                    )
                    balances = incrementalMapper
                      .mapBalances(incremental, filteredBalances, timestamp)
                      .map(CurrencyData(identifierStr, _))
                  } yield (
                    aggSnap :+ snapshot,
                    aggBlocks ++ blocks,
                    aggTxs ++ transactions,
                    aggFeeTxs ++ feeTransactions,
                    aggBalances ++ balances,
                    aggLastBalances + (identifier -> filteredBalances)
                  )
              }
          }
          .map { case (aggSnap, aggBlocks, aggTxs, aggFeeTxs, aggBalances, _) =>
            MetagraphData(aggSnap, aggBlocks, aggTxs, aggFeeTxs, aggBalances)
          }
      }

    }

}
