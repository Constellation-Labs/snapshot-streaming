package org.constellation.snapshotstreaming.opensearch.mapper

import java.util.Date
import cats.data.NonEmptyList
import cats.data.OptionT
import cats.effect.Async
import cats.syntax.all._
import org.tessellation.currency.schema.currency.CurrencyIncrementalSnapshot
import org.tessellation.currency.schema.currency.CurrencySnapshot
import org.tessellation.currency.schema.currency.CurrencySnapshotInfo
import org.constellation.snapshotstreaming.opensearch.schema.{CurrencySnapshot => OSCurrencySnapshot}
import org.tessellation.schema.address.Address
import org.tessellation.security.Hashed
import org.tessellation.security.Hasher
import org.constellation.snapshotstreaming.opensearch.schema._
import org.constellation.snapshotstreaming.storage.LastCurrencySnapshotStorage
import org.tessellation.json.JsonSerializer
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.balance.Balance
import org.tessellation.security.signature.Signed
import org.tessellation.statechannel.StateChannelSnapshotBinary

import scala.collection.immutable.SortedMap

trait CurrencySnapshotMapper[F[_]] {

  def mapCurrencySnapshots(
    snapshots: Map[Address, NonEmptyList[
      Either[Hashed[
        CurrencySnapshot
      ], (Hashed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo, Signed[StateChannelSnapshotBinary])]
    ]],
    timestamp: Date,
    txHasher: Hasher[F],
    hasher: Hasher[F]
  ): F[
    (
      Seq[CurrencyData[Snapshot]],
      Seq[CurrencyData[OSCurrencySnapshot]],
      Seq[CurrencyData[Block]],
      Seq[CurrencyData[Transaction]],
      Seq[CurrencyData[AddressBalance]]
    )
  ]

}

object CurrencySnapshotMapper {

  def make[F[_]: Async: JsonSerializer: KryoSerializer](
    lastCurrencySnapshotStorage: LastCurrencySnapshotStorage[F]
  ): CurrencySnapshotMapper[F] =
    make(CurrencyFullSnapshotMapper.make(), CurrencyIncrementalSnapshotMapper.make(), lastCurrencySnapshotStorage)

  private def make[F[_]: Async](
    fullMapper: CurrencyFullSnapshotMapper[F],
    incrementalMapper: CurrencyIncrementalSnapshotMapper[F],
    lastCurrencySnapshotStorage: LastCurrencySnapshotStorage[F]
  ): CurrencySnapshotMapper[F] =
    new CurrencySnapshotMapper[F] {

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
          Seq[CurrencyData[OSCurrencySnapshot]],
          Seq[CurrencyData[Block]],
          Seq[CurrencyData[Transaction]],
          Seq[CurrencyData[AddressBalance]]
        )
      ] = snapshots.toList.flatMap { case (i, s) => s.toList.map((i, _)) }
        .foldLeftM(
          (
            Seq.empty[CurrencyData[Snapshot]],
            Seq.empty[CurrencyData[OSCurrencySnapshot]],
            Seq.empty[CurrencyData[Block]],
            Seq.empty[CurrencyData[Transaction]],
            Seq.empty[CurrencyData[AddressBalance]],
            Map.empty[Address, SortedMap[Address, Balance]]
          )
        ) {
          case (
                (aggSnap, aggCurrencyIncrementalSnap, aggBlocks, aggTxs, aggBalances, aggLastBalances),
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
                  aggCurrencyIncrementalSnap,
                  aggBlocks ++ blocks,
                  aggTxs ++ transactions,
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
                  prevBalances <- OptionT
                    .fromOption(aggLastBalances.get(identifier))
                    .orElseF(lastCurrencySnapshotStorage.getLastBalances(identifier, hasher))
                    .value
                  filteredBalances = incrementalMapper.balanceDiff(
                    incremental,
                    prevBalances,
                    info
                  )
                  balances = incrementalMapper
                    .mapBalances(incremental, filteredBalances, timestamp)
                    .map(CurrencyData(identifierStr, _))
                } yield (
                  aggSnap,
                  aggCurrencyIncrementalSnap :+ snapshot,
                  aggBlocks ++ blocks,
                  aggTxs ++ transactions,
                  aggBalances ++ balances,
                  aggLastBalances + (identifier -> filteredBalances)
                )
            }
        }
        .map { case (aggSnap, aggCurrencyIncrementalSnap, aggBlocks, aggTxs, aggBalances, _) =>
          (aggSnap, aggCurrencyIncrementalSnap, aggBlocks, aggTxs, aggBalances)
        }

    }

}
