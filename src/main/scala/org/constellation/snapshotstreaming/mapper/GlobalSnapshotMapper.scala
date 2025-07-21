package org.constellation.snapshotstreaming.mapper

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import org.constellation.snapshotstreaming.schema.schema.GlobalData
import org.constellation.snapshotstreaming.schema.{RewardTransaction, Snapshot, TransactionReference}
import org.tessellation.schema.{GlobalIncrementalSnapshot, transaction}
import org.tessellation.security.{Hashed, Hasher}

import java.time.LocalDateTime
import scala.collection.immutable.{SortedMap, SortedSet}

abstract class GlobalSnapshotMapper[F[_]: Async] extends SnapshotMapper[F, GlobalIncrementalSnapshot] {

  def mapSnapshot(snapshot: Hashed[GlobalIncrementalSnapshot], timestamp: LocalDateTime, hasher: Hasher[F]): F[Snapshot]

  def mapGlobalSnapshot(
                         globalSnapshot: Hashed[GlobalIncrementalSnapshot],
    timestamp: LocalDateTime,
    txHasher: Hasher[F],
    hasher: Hasher[F]
  ): F[GlobalData] = {

    for {
      snapshot <- mapSnapshot(globalSnapshot, timestamp, hasher)
      blocks <- mapBlocks(globalSnapshot, timestamp, txHasher, hasher)
      transactions <- mapTransactions(globalSnapshot, timestamp, txHasher, hasher)


    } yield GlobalData(
      snapshot,
      blocks,
      transactions,
      globalSnapshot.signed.proofs.toSortedSet.toSeq,
    )
  }

  private def flatten[K, T](bag: Option[SortedMap[K, Iterable[T]]]) = bag.toSeq.flatMap(_.toSeq.flatMap {
    case (k, values) => values.toSeq.map((k, _))
  })


}

object GlobalSnapshotMapper {

  def make[F[_]: Async](): GlobalSnapshotMapper[F] =
    new GlobalSnapshotMapper[F] {

      def fetchRewards(snapshot: GlobalIncrementalSnapshot): SortedSet[transaction.RewardTransaction] =
        snapshot.rewards

      def extractSnapshotReferredAddresses(snapshot: GlobalIncrementalSnapshot): SnapshotReferredAddresses = {
        val transactions = snapshot.blocks.flatMap(_.block.transactions.toSortedSet)
        val source = transactions.map(_.source)
        val destination = transactions.map(_.destination)
        SnapshotReferredAddresses(source, destination)
      }

      def mapSnapshot(
        snapshot: Hashed[GlobalIncrementalSnapshot],
        timestamp: LocalDateTime,
        hasher: Hasher[F]
      ): F[Snapshot] =
        snapshot.blocks.unsorted.map(_.block).map(hashBlock(_, hasher)).toList.sequence.map { blocksHashes =>
          Snapshot(
            hash = snapshot.hash.value,
            ordinal = snapshot.ordinal.value.value,
            height = snapshot.height.value,
            subHeight = snapshot.subHeight.value,
            lastSnapshotHash = snapshot.lastSnapshotHash.value,
            epochProgress = snapshot.epochProgress.value,
            blocks = blocksHashes.toSet,
            rewards = fetchRewards(snapshot).toSeq.map(reward =>
              RewardTransaction(
                reward.destination.value,
                reward.amount.value
              )
            ),
            version = snapshot.version.version,
            timestamp = timestamp
          )
        }

    }

}
