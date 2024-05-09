package org.constellation.snapshotstreaming.opensearch.mapper

import java.util.Date

import cats.effect.Async
import cats.syntax.flatMap._
import cats.syntax.functor._

import scala.collection.immutable.SortedSet

import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, transaction}
import org.tessellation.security.{Hashed, Hasher, HasherSelector}

abstract class GlobalSnapshotMapper[F[_]: Async: KryoSerializer: HasherSelector]
    extends SnapshotMapper[F, GlobalIncrementalSnapshot] {

  def mapGlobalSnapshot(
    globalSnapshot: Hashed[GlobalIncrementalSnapshot],
    info: GlobalSnapshotInfo,
    timestamp: Date,
    txHasher: Hasher[F]
  ) = {

    val hasher = HasherSelector[F].getForOrdinal(globalSnapshot.ordinal)
    for {
      snapshot <- mapSnapshot(globalSnapshot, timestamp, hasher)
      blocks <- mapBlocks(globalSnapshot, timestamp, txHasher, hasher)
      transactions <- mapTransactions(globalSnapshot, timestamp, txHasher, hasher)
      filteredBalances = snapshotReferredBalancesInfo(
        globalSnapshot.signed.value,
        info
      )
      balances = mapBalances(globalSnapshot, filteredBalances, timestamp)
    } yield (snapshot, blocks, transactions, balances)
  }

}

object GlobalSnapshotMapper {

  def make[F[_]: Async: KryoSerializer: HasherSelector](): GlobalSnapshotMapper[F] =
    new GlobalSnapshotMapper[F] {

      def fetchRewards(snapshot: GlobalIncrementalSnapshot): SortedSet[transaction.RewardTransaction] =
        snapshot.rewards

    }

}
