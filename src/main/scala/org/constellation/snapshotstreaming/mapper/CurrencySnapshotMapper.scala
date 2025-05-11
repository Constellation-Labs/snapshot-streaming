package org.constellation.snapshotstreaming.mapper

import cats.effect.Async
import cats.syntax.all._
import org.constellation.snapshotstreaming.ReindexerSnapshotProcessor.GlobalSnapshotWithState
import org.constellation.snapshotstreaming.schema.schema.{MetagraphData, toIncremental}
import org.constellation.snapshotstreaming.schema.{AddressBalance, Block, CurrencyData, FeeTransaction, Transaction, CurrencySnapshot => OSCurrencySnapshot}
import org.tessellation.json.JsonSerializer
import org.tessellation.schema.address.Address
import org.tessellation.schema.balance.Balance
import org.tessellation.security.Hasher
import org.constellation.snapshotstreaming.schema.schema.MetagraphData
import org.constellation.snapshotstreaming.schema.{
  AddressBalance,
  Block,
  CurrencyData,
  CurrencySnapshot => OSCurrencySnapshot,
  FeeTransaction,
  Transaction
}

import java.time.LocalDateTime
import scala.collection.immutable.SortedMap

trait CurrencySnapshotMapper[F[_]] {

  def mapCurrencySnapshots(
    globalSnapshotWithState: GlobalSnapshotWithState,
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
          Map[Address, Map[Address, Balance]],
        )

      type CurrencySnapshotMapperResult = MetagraphData

      def mapCurrencySnapshots(
        globalSnapshotWithState: GlobalSnapshotWithState,
        txHasher: Hasher[F],
        hasher: Hasher[F]
      ): F[CurrencySnapshotMapperResult] = {

        val GlobalSnapshotWithState(globalSnapshot, maybePrevLastSnapshots, _, currencySnapshots, timestamp) =
          globalSnapshotWithState

        val maybeLastSnapshots = maybePrevLastSnapshots.map(_.lastCurrencySnapshots)
        val initialAccBalances = maybeLastSnapshots.map {
          _.map {
            case (identifier, Left(full))        => identifier -> full.info.balances
            case (identifier, Right((_, state))) => identifier -> state.balances
          }
        }.getOrElse(SortedMap.empty[Address, SortedMap[Address, Balance]])

        val initialAcc: Acc = (
          Seq.empty,
          Seq.empty,
          Seq.empty,
          Seq.empty,
          Seq.empty,
          initialAccBalances
        )

        currencySnapshots.toList.flatMap { case (i, s) => s.toList.map((i, _)) }
          .foldLeftM[F, Acc](initialAcc) {
            case (
              (aggCurrencySnap, aggBlocks, aggTxs, aggFeeTxs, aggChangedBalances, aggLastBalances),
              (identifier, fullOrIncremental)
              ) =>
              val identifierStr = identifier.value.value
              def toCurrency[A](a:A) = CurrencyData(identifierStr, a)

              fullOrIncremental match {
                case Left(full) =>
                  for {
                    snapshot <- fullMapper
                      .mapSnapshot(globalSnapshot.hash, full, timestamp, hasher)
                      .map(full => CurrencyData(identifierStr, toIncremental(globalSnapshot.hash, full)))
                    blocks <- fullMapper
                      .mapBlocks(full, timestamp, txHasher, hasher)
                      .map(_.map(CurrencyData(identifierStr, _)))
                    transactions <- fullMapper
                      .mapTransactions(full, timestamp, txHasher, hasher)
                      .map(_.map(CurrencyData(identifierStr, _)))
                    prevBalances = aggLastBalances.get(identifier)
                    onlyUpdatedBalances = fullMapper.balanceDiff(
                      full,
                      prevBalances,
                      full.info.balances
                    )
                    changedAddressBalances = fullMapper
                      .mapBalances(full, onlyUpdatedBalances, timestamp)
                      .map(CurrencyData(identifierStr, _))
                  } yield (
                    aggCurrencySnap :+ snapshot,
                    aggBlocks ++ blocks,
                    aggTxs ++ transactions,
                    aggFeeTxs,
                    aggChangedBalances ++ changedAddressBalances,
                    aggLastBalances + (identifier -> full.info.balances)
                  )

                case Right((incremental, info, binary)) =>
                  for {
                    snapshot <- incrementalMapper
                      .mapSnapshot(globalSnapshot.hash, incremental, binary, info, timestamp, hasher)
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
                    //in theory, we won't need this
                    onlyUpdatedBalances = incrementalMapper.balanceDiff(
                      incremental,
                      prevBalances,
                      info.balances
                    )
                    changedAddressBalances = incrementalMapper
                      .mapBalances(incremental, onlyUpdatedBalances, timestamp)
                      .map(CurrencyData(identifierStr, _))
                  } yield (
                    aggCurrencySnap :+ snapshot,
                    aggBlocks ++ blocks,
                    aggTxs ++ transactions,
                    aggFeeTxs ++ feeTransactions,
                    aggChangedBalances ++ changedAddressBalances,
                    updateBalanceMap(identifier, aggLastBalances, onlyUpdatedBalances)
                  )
              }
          }
          .map { case (aggCurrencySnap, aggBlocks, aggTxs, aggFeeTxs, changedBalances, newBalanceState) =>
            MetagraphData(globalSnapshot.hash.value, aggCurrencySnap, aggBlocks, aggTxs, aggFeeTxs, changedBalances, newBalanceState)
          }
      }

    }

  def updateBalanceMap(identifier: Address, previous: Map[Address, Map[Address, Balance]], changedBalances: Map[Address, Balance] ) =
    previous.updatedWith(identifier)( _.map(_ ++ changedBalances).orElse(changedBalances.some))

}

