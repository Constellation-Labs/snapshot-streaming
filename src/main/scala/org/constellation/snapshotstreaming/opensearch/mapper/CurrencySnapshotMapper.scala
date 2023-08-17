package org.constellation.snapshotstreaming.opensearch.mapper

import cats.data.NonEmptyList
import cats.effect.Async
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.foldable._
import org.constellation.snapshotstreaming.opensearch.schema.{AddressBalance, Block, CurrencyData, Snapshot, Transaction}
import org.tessellation.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshot, CurrencySnapshotInfo}
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.address.Address
import org.tessellation.security.Hashed

import java.util.Date

trait CurrencySnapshotMapper[F[_]] {
  def mapCurrencySnapshots(
    snapshots: Map[Address, NonEmptyList[Either[Hashed[CurrencySnapshot], (Hashed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]]],
    timestamp: Date
  ): F[
    (
      Seq[CurrencyData[Snapshot]],
      Seq[CurrencyData[Block]],
      Seq[CurrencyData[Transaction]],
      Seq[CurrencyData[AddressBalance]]
    )
  ]
}

object CurrencySnapshotMapper {
  def make[F[_]: Async: KryoSerializer](): CurrencySnapshotMapper[F] =
    make(CurrencyFullSnapshotMapper.make(), CurrencyIncrementalSnapshotMapper.make())

  private def make[F[_] : Async : KryoSerializer](
    fullMapper: CurrencyFullSnapshotMapper[F],
    incrementalMapper: CurrencyIncrementalSnapshotMapper[F]
  ): CurrencySnapshotMapper[F] =
    new CurrencySnapshotMapper[F] {
      def mapCurrencySnapshots(
        snapshots: Map[Address, NonEmptyList[Either[Hashed[CurrencySnapshot], (Hashed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]]],
        timestamp: Date
      ): F[
        (
          Seq[CurrencyData[Snapshot]],
            Seq[CurrencyData[Block]],
            Seq[CurrencyData[Transaction]],
            Seq[CurrencyData[AddressBalance]]
          )
      ] =
        snapshots.toList.flatMap { case (i, s) => s.toList.map((i, _)) }
          .foldLeftM((Seq.empty[CurrencyData[Snapshot]], Seq.empty[CurrencyData[Block]], Seq.empty[CurrencyData[Transaction]], Seq.empty[CurrencyData[AddressBalance]])) {
            case ((aggSnap, aggBlocks, aggTxs, aggBalances), (identifier, fullOrIncremental)) =>
              val identifierStr = identifier.value.value

              fullOrIncremental match {
                case Left(full) =>
                  for {
                    snapshot <- fullMapper.mapSnapshot(full, timestamp)
                      .map(CurrencyData(identifierStr, _))
                    blocks <- fullMapper.mapBlocks(full, timestamp)
                      .map(_.map(CurrencyData(identifierStr, _)))
                    transactions <- fullMapper.mapTransactions(full, timestamp)
                      .map(_.map(CurrencyData(identifierStr, _)))
                    balances = fullMapper.mapBalances(full, full.info.balances, timestamp)
                      .map(CurrencyData(identifierStr, _))
                  } yield (aggSnap :+ snapshot, aggBlocks ++ blocks, aggTxs ++ transactions, aggBalances ++ balances)

                case Right((incremental, info)) =>
                  for {
                    snapshot <- incrementalMapper.mapSnapshot(incremental, timestamp)
                      .map(CurrencyData(identifierStr, _))
                    blocks <- incrementalMapper.mapBlocks(incremental, timestamp)
                      .map(_.map(CurrencyData(identifierStr, _)))
                    transactions <- incrementalMapper.mapTransactions(incremental, timestamp)
                      .map(_.map(CurrencyData(identifierStr, _)))
                    filteredBalances = incrementalMapper.snapshotReferredBalancesInfo(incremental, info)
                    balances = incrementalMapper.mapBalances(incremental, filteredBalances, timestamp)
                      .map(CurrencyData(identifierStr, _))
                  } yield (aggSnap :+ snapshot, aggBlocks ++ blocks, aggTxs ++ transactions, aggBalances ++ balances)
              }
          }
    }
}
