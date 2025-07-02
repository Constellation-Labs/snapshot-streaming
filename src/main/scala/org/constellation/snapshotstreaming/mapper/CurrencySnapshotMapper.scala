package org.constellation.snapshotstreaming.mapper

import cats.effect.Async
import cats.syntax.all._
import org.constellation.snapshotstreaming.schema.schema.{MetagraphData, toIncremental}
import org.constellation.snapshotstreaming.schema.{AddressBalance, Block, CurrencyData, FeeTransaction, Transaction, CurrencySnapshot => OSCurrencySnapshot}
import org.tessellation.currency.schema.currency.CurrencyIncrementalSnapshot
import org.tessellation.json.JsonSerializer
import org.tessellation.schema.address.Address
import org.tessellation.schema.balance.Balance
import org.tessellation.security.signature.Signed
import org.tessellation.security.{Hashed, Hasher}
import org.tessellation.statechannel.StateChannelSnapshotBinary

import java.time.LocalDateTime
import scala.collection.immutable.SortedMap

trait CurrencySnapshotMapper[F[_]] {

  def mapCurrencySnapshots(
    currencySnapshots: List[(Address, Hashed[CurrencyIncrementalSnapshot], Signed[StateChannelSnapshotBinary])],
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
    make( CurrencyIncrementalSnapshotMapper.make())

  private def make[F[_]: Async](
    incrementalMapper: CurrencyIncrementalSnapshotMapper[F]
  ): CurrencySnapshotMapper[F] =
    new CurrencySnapshotMapper[F] {

      type Acc = (
        Seq[CurrencyData[OSCurrencySnapshot]],
        Seq[CurrencyData[Block]],
        Seq[CurrencyData[Transaction]],
        Seq[CurrencyData[FeeTransaction]],
      )

      type CurrencySnapshotMapperResult = MetagraphData

      def mapCurrencySnapshots(
        currencySnapshots: List[(Address, Hashed[CurrencyIncrementalSnapshot], Signed[StateChannelSnapshotBinary])],
        timestamp: LocalDateTime,
        txHasher: Hasher[F],
        hasher: Hasher[F]
      ): F[CurrencySnapshotMapperResult] = {

        val initialAcc: Acc = (
          Seq.empty,
          Seq.empty,
          Seq.empty,
          Seq.empty,
        )

        currencySnapshots
          .foldLeftM[F, Acc](initialAcc) {
            case (
                  (
                    aggCurrencySnap,
                    aggBlocks,
                    aggTxs,
                    aggFeeTxs,
                  ),
                  (identifier, incremental, binary)
                ) =>
              val identifierStr = identifier.value.value
              def toCurrency[A](a: A) = CurrencyData(identifierStr, a)

                  for {
                    snapshot <- incrementalMapper
                      .mapSnapshot(incremental, binary, timestamp, hasher)
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


                  } yield (
                    aggCurrencySnap :+ snapshot,
                    aggBlocks ++ blocks,
                    aggTxs ++ transactions,
                    aggFeeTxs ++ feeTransactions
                  )
              }
          }
          .map {
            case (
                  aggCurrencySnap,
                  aggBlocks,
                  aggTxs,
                  aggFeeTxs,
                ) =>
              MetagraphData(
                aggCurrencySnap,
                aggBlocks,
                aggTxs,
                aggFeeTxs,
              )
          }

    }

}
