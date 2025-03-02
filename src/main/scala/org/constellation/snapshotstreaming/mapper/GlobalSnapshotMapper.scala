package org.constellation.snapshotstreaming.mapper

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, transaction}
import io.constellationnetwork.security.{Hashed, Hasher}
import org.constellation.snapshotstreaming.SnapshotProcessor.GlobalSnapshotWithState
import org.constellation.snapshotstreaming.schema.schema.GlobalData
import org.constellation.snapshotstreaming.schema.{RewardTransaction, Snapshot}

import java.time.LocalDateTime
import scala.collection.immutable.SortedSet
//import io.constellationnetwork.kryo.KryoSerializer

abstract class GlobalSnapshotMapper[F[_]: Async]
    extends SnapshotMapper[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo] {

  def mapSnapshot(snapshot: Hashed[GlobalIncrementalSnapshot], timestamp: LocalDateTime, hasher: Hasher[F]): F[Snapshot]

  def mapGlobalSnapshot(
    globalSnapshotWithState: GlobalSnapshotWithState,
    timestamp: LocalDateTime,
    txHasher: Hasher[F],
    hasher: Hasher[F]
  ): F[GlobalData] = {
    val GlobalSnapshotWithState(globalSnapshot, maybePrevSnapshotInfo, snapshotInfo, _, ts) =
      globalSnapshotWithState
    for {
      snapshot <- mapSnapshot(globalSnapshot, timestamp, hasher)
      blocks <- mapBlocks(globalSnapshot, timestamp, txHasher, hasher)
      transactions <- mapTransactions(globalSnapshot, timestamp, txHasher, hasher)
      filteredBalances = balanceDiff(
        globalSnapshot.signed.value,
        maybePrevSnapshotInfo.map(prev => prev.balances),
        snapshotInfo
      )
      balances = mapBalances(globalSnapshot, filteredBalances, timestamp)
    } yield GlobalData(snapshot, blocks, transactions, balances, globalSnapshot.signed.proofs.toSortedSet.toSeq)
  }

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
        for {
          blockHashes <- snapshot.blocks.unsorted.map(_.block).map(hashBlock(_, hasher)).toList.sequence
          rewards = fetchRewards(snapshot).unsorted.map(reward =>
            RewardTransaction(
              reward.destination.value,
              reward.amount.value
            )
          )
        } yield Snapshot(
          hash = snapshot.hash.value,
          ordinal = snapshot.ordinal.value.value,
          height = snapshot.height.value,
          subHeight = snapshot.subHeight.value,
          lastSnapshotHash = snapshot.lastSnapshotHash.value,
          epochProgress = snapshot.epochProgress.value,
          blocks = blockHashes.toSet,
          rewards = rewards,
          version = snapshot.version.version,
          timestamp = timestamp
        )

    }

}
