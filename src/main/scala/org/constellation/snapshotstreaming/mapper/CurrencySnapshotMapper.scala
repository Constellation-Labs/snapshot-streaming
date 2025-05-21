package org.constellation.snapshotstreaming.mapper

import cats.effect.Async
import cats.syntax.all._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.security.Hasher
import org.constellation.snapshotstreaming.SnapshotProcessor.GlobalSnapshotWithState
import org.constellation.snapshotstreaming.schema.AllowSpends.{AllowSpend, AllowSpendExpiration, SpendTransaction}
import org.constellation.snapshotstreaming.schema.TokenLocks.{TokenLock, TokenUnlock}
import org.constellation.snapshotstreaming.schema.schema.{MetagraphData, toIncremental}
import org.constellation.snapshotstreaming.schema.{AddressBalance, Block, CurrencyData, FeeTransaction, Snapshot, Transaction, CurrencySnapshot => OSCurrencySnapshot}

import java.time.LocalDateTime
import scala.collection.immutable.SortedMap


trait CurrencySnapshotMapper[F[_]] {

  def mapCurrencySnapshots(
    globalSnapshotWithState: GlobalSnapshotWithState,
    timestamp              : LocalDateTime,
    txHasher               : Hasher[F],
    hasher                 : Hasher[F]
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
          Seq[CurrencyData[AllowSpend]],
          Seq[CurrencyData[SpendTransaction]],
          Seq[CurrencyData[AllowSpendExpiration]],
          Seq[CurrencyData[TokenLock]],
          Seq[CurrencyData[TokenUnlock]],
          Map[Address, SortedMap[Address, Balance]],
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
          Seq.empty,
          Seq.empty,
          Seq.empty,
          Seq.empty,
          Seq.empty,
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
              (aggCurrencySnap, aggBlocks, aggTxs, aggFeeTxs, aggBalances, aggAllowSpends, aggSpendTxs, aggSpendExpirations, aggTokenLocks, aggTokenUnlocks, aggLastBalances),
              (identifier, fullOrIncremental)
              ) =>
              val identifierStr = identifier.value.value
              def toCurrency[A](a:A) = CurrencyData(identifierStr, a)

              fullOrIncremental match {
                case Left(full) =>
                  for {
                    snapshot <- fullMapper
                      .mapSnapshot(full, timestamp, hasher)
                      .map(full => CurrencyData(identifierStr, toIncremental(full)))
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
                    aggCurrencySnap :+ snapshot,
                    aggBlocks ++ blocks,
                    aggTxs ++ transactions,
                    aggFeeTxs,
                    aggBalances ++ balances,
                    aggAllowSpends,
                    aggSpendTxs,
                    aggSpendExpirations,
                    aggTokenLocks,
                    aggTokenUnlocks,
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
                    allowSpends <- incrementalMapper
                      .mapAllowSpends(incremental, timestamp, hasher)
                    artifacts <- incrementalMapper.mapArtifacts(incremental, hasher)
                    (spendsTx, tokenUnlocks, spendExpirations) = artifacts
                    tokenLocks <- incrementalMapper
                      .mapTokenLocks(incremental, timestamp, hasher)
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
                    aggCurrencySnap :+ snapshot,
                    aggBlocks ++ blocks,
                    aggTxs ++ transactions,
                    aggFeeTxs ++ feeTransactions,
                    aggBalances ++ balances,
                    aggAllowSpends ++ allowSpends.map(toCurrency),
                    aggSpendTxs ++ spendsTx.map(toCurrency),
                    aggSpendExpirations ++ spendExpirations.map(toCurrency),
                    aggTokenLocks ++ tokenLocks.map(toCurrency),
                    aggTokenUnlocks ++ tokenUnlocks.map(toCurrency),
                    aggLastBalances + (identifier -> filteredBalances)
                  )
              }
          }
          .map { case (aggCurrencySnap, aggBlocks, aggTxs, aggFeeTxs, aggBalances, aggAllowSpends, aggSpendTxs, aggSpendExpirations, aggTokenLocks, aggTokenUnlocks, _) =>
            MetagraphData(aggCurrencySnap, aggBlocks, aggTxs, aggFeeTxs, aggBalances, aggAllowSpends, aggSpendTxs, aggSpendExpirations, aggTokenLocks, aggTokenUnlocks)
          }
      }

    }

}
