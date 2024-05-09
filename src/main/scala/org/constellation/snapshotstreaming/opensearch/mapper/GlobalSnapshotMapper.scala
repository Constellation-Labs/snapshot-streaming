package org.constellation.snapshotstreaming.opensearch.mapper

import java.util.Date
import cats.effect.Async
import cats.syntax.flatMap._
import cats.syntax.functor._
import shapeless.syntax.std.tuple._

import scala.collection.immutable.SortedSet
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.GlobalIncrementalSnapshot
import org.tessellation.schema.GlobalSnapshotInfo
import org.tessellation.schema.transaction
import org.tessellation.security.Hashed
import org.tessellation.security.Hasher
import org.tessellation.security.HasherSelector

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

      def extractSnapshotReferredAddresses(snapshot: GlobalIncrementalSnapshot): SnapshotReferredAddresses = {
        val transactions = snapshot.blocks.flatMap(_.block.transactions.toSortedSet)
        val source = transactions.map(_.source)
        val destination = transactions.map(_.destination)
        SnapshotReferredAddresses(source, destination)
      }
    }

}
