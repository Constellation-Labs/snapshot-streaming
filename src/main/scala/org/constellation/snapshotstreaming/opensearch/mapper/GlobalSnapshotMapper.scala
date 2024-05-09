package org.constellation.snapshotstreaming.opensearch.mapper

import java.util.Date
import cats.effect.Async
import cats.syntax.all._
import org.tessellation.syntax.sortedCollection._
import eu.timepit.refined.auto._
import org.constellation.snapshotstreaming.opensearch.schema._
import eu.timepit.refined.auto._
import org.constellation.snapshotstreaming.opensearch.schema.Snapshot
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

  def mapSnapshot(snapshot: Hashed[GlobalIncrementalSnapshot], timestamp: Date, hasher: Hasher[F]): F[Snapshot]

  def mapGlobalSnapshot(
    globalSnapshot: Hashed[GlobalIncrementalSnapshot],
    info: GlobalSnapshotInfo,
    timestamp: Date,
    txHasher: Hasher[F],
    hasher: Hasher[F]
  ) = {

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

      def mapSnapshot(snapshot: Hashed[GlobalIncrementalSnapshot], timestamp: Date, hasher: Hasher[F]): F[Snapshot] =
        snapshot.blocks.unsorted.map(_.block).map(hashBlock(_, hasher)).toList.sequence.map { blocksHashes =>
          Snapshot(
            hash = snapshot.hash.value,
            ordinal = snapshot.ordinal.value.value,
            height = snapshot.height.value,
            subHeight = snapshot.subHeight.value,
            lastSnapshotHash = snapshot.lastSnapshotHash.value,
            blocks = blocksHashes.toSet,
            rewards = fetchRewards(snapshot).unsorted.map(reward =>
              RewardTransaction(
                reward.destination.value,
                reward.amount.value
              )
            ),
            timestamp = timestamp
          )
        }
    }

}
