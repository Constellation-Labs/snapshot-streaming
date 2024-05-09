package org.constellation.snapshotstreaming.opensearch.mapper

import java.util.Date
import cats.data.NonEmptyList
import cats.effect.Async
import cats.syntax.flatMap._
import cats.syntax.foldable._
import cats.syntax.functor._
import org.tessellation.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshot, CurrencySnapshotInfo}
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.address.Address
import org.tessellation.security.{Hashed, Hasher}
import org.constellation.snapshotstreaming.opensearch.schema._
import org.tessellation.security.signature.Signed
import org.tessellation.statechannel.StateChannelSnapshotBinary

trait CurrencySnapshotMapper[F[_]] {

  def mapCurrencySnapshots(
    snapshots: Map[Address, NonEmptyList[Either[Hashed[
      CurrencySnapshot
    ], (Hashed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo, Signed[StateChannelSnapshotBinary])]]],
    timestamp: Date,
    txHasher: Hasher[F],
    hasher: Hasher[F]
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

  private def make[F[_]: Async: KryoSerializer](
    fullMapper: CurrencyFullSnapshotMapper[F],
    incrementalMapper: CurrencyIncrementalSnapshotMapper[F]
  ): CurrencySnapshotMapper[F] =
    new CurrencySnapshotMapper[F] {

      def mapCurrencySnapshots(
        snapshots: Map[Address, NonEmptyList[
          Either[Hashed[CurrencySnapshot], (Hashed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo, Signed[StateChannelSnapshotBinary])]
        ]],
        timestamp: Date,
        txHasher: Hasher[F],
        hasher: Hasher[F]
      ): F[
        (
          Seq[CurrencyData[Snapshot]],
          Seq[CurrencyData[Block]],
          Seq[CurrencyData[Transaction]],
          Seq[CurrencyData[AddressBalance]]
        )
      ] =
        snapshots.toList.flatMap { case (i, s) => s.toList.map((i, _)) }
          .foldLeftM(
            (
              Seq.empty[CurrencyData[Snapshot]],
              Seq.empty[CurrencyData[Block]],
              Seq.empty[CurrencyData[Transaction]],
              Seq.empty[CurrencyData[AddressBalance]]
            )
          ) { case ((aggSnap, aggBlocks, aggTxs, aggBalances), (identifier, fullOrIncremental)) =>
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
                } yield (aggSnap :+ snapshot, aggBlocks ++ blocks, aggTxs ++ transactions, aggBalances ++ balances)

              case Right((incremental, info, _)) =>
                for {
                  snapshot <- incrementalMapper
                    .mapSnapshot(incremental, timestamp, hasher)
                    .map(CurrencyData(identifierStr, _))
                  blocks <- incrementalMapper
                    .mapBlocks(incremental, timestamp, txHasher, hasher)
                    .map(_.map(CurrencyData(identifierStr, _)))
                  transactions <- incrementalMapper
                    .mapTransactions(incremental, timestamp, txHasher, hasher)
                    .map(_.map(CurrencyData(identifierStr, _)))
                  filteredBalances = incrementalMapper.snapshotReferredBalancesInfo(incremental, info)
                  balances = incrementalMapper
                    .mapBalances(incremental, filteredBalances, timestamp)
                    .map(CurrencyData(identifierStr, _))
                } yield (aggSnap :+ snapshot, aggBlocks ++ blocks, aggTxs ++ transactions, aggBalances ++ balances)
            }
          }

    }

}
