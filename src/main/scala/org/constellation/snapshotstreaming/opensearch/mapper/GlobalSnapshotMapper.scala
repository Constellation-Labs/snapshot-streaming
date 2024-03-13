package org.constellation.snapshotstreaming.opensearch.mapper

import java.util.Date

import cats.effect.Async
import cats.syntax.flatMap._
import cats.syntax.functor._

import scala.collection.immutable.SortedSet

import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, transaction}
import org.tessellation.security.{Hashed, Hasher}

abstract class GlobalSnapshotMapper[F[_]: Async: KryoSerializer: Hasher] extends SnapshotMapper[F, GlobalIncrementalSnapshot] {

  def mapGlobalSnapshot(globalSnapshot: Hashed[GlobalIncrementalSnapshot], info: GlobalSnapshotInfo, timestamp: Date) =
    for {
      snapshot <- mapSnapshot(globalSnapshot, timestamp)
      blocks <- mapBlocks(globalSnapshot, timestamp)
      transactions <- mapTransactions(globalSnapshot, timestamp)
      filteredBalances = snapshotReferredBalancesInfo(
        globalSnapshot.signed.value,
        info
      )
      balances = mapBalances(globalSnapshot, filteredBalances, timestamp)
    } yield (snapshot, blocks, transactions, balances)
}

object GlobalSnapshotMapper {

  def make[F[_] : Async : KryoSerializer: Hasher](): GlobalSnapshotMapper[F] =
    new GlobalSnapshotMapper[F] {

      def fetchRewards(snapshot: GlobalIncrementalSnapshot): SortedSet[transaction.RewardTransaction] =
        snapshot.rewards
    }

}
