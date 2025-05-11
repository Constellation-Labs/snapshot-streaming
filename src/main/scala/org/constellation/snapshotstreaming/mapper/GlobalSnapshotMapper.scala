package org.constellation.snapshotstreaming.mapper

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import org.constellation.snapshotstreaming.ReindexerSnapshotProcessor.GlobalSnapshotWithState
import org.constellation.snapshotstreaming.schema.schema.{GlobalData, SignatureProof}
import org.constellation.snapshotstreaming.schema.{RewardTransaction, Snapshot}
import org.tessellation.schema.address.Address
import org.tessellation.schema.balance.Balance
import org.tessellation.schema.{GlobalIncrementalSnapshot, transaction}
import org.tessellation.security.{Hashed, Hasher}

import java.time.LocalDateTime
import scala.collection.immutable.SortedSet

abstract class GlobalSnapshotMapper[F[_]: Async] extends SnapshotMapper[F, GlobalIncrementalSnapshot] {

  def mapSnapshot(snapshot: Hashed[GlobalIncrementalSnapshot], timestamp: LocalDateTime, hasher: Hasher[F]): F[Snapshot]

  def mapGlobalSnapshot(
    globalSnapshotWithState: GlobalSnapshotWithState,
    txHasher: Hasher[F],
    hasher: Hasher[F]
  ): F[GlobalData] = {
    val GlobalSnapshotWithState(globalSnapshot, maybePrevSnapshotInfo, snapshotInfo, currencySnapshots, timestamp) =
      globalSnapshotWithState
    for {
      snapshot <- mapSnapshot(globalSnapshot, timestamp, hasher)
      blocks <- mapBlocks(globalSnapshot, timestamp, txHasher, hasher)
      transactions <- mapTransactions(globalSnapshot, timestamp, txHasher, hasher)
      prevBalances = maybePrevSnapshotInfo.map(prev => prev.balances).getOrElse(Map[Address, Balance]())
      filteredBalances = balanceDiff(
        globalSnapshot.signed.value,
        maybePrevSnapshotInfo.map(prev => prev.balances),
        snapshotInfo.balances
      )
      balances = mapBalances(globalSnapshot, filteredBalances, timestamp)

    } yield {
      GlobalData(
        snapshot,
        blocks,
        transactions,
        balances,
        globalSnapshot.signed.proofs.toSortedSet.toSeq.map(SignatureProof.from(globalSnapshot.hash, _)),
        currencySnapshots.values.map(_ => ()).size,
        prevBalances ++ filteredBalances
      )
    }
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
        snapshot.blocks.unsorted.map(_.block).map(hashBlock(_, hasher)).toList.sequence.map { blocksHashes =>
          Snapshot(
            hash = snapshot.hash.value,
            ordinal = snapshot.ordinal.value.value,
            height = snapshot.height.value,
            subHeight = snapshot.subHeight.value,
            lastSnapshotHash = snapshot.lastSnapshotHash.value,
            epochProgress = snapshot.epochProgress.value,
            blocks = blocksHashes.toSet,
            rewards = fetchRewards(snapshot).unsorted.map(reward =>
              RewardTransaction(
                snapshot.hash.value,
                reward.destination.value,
                reward.amount.value
              )
            ),
            version = snapshot.version.version,
            metagraphSnapshotsCount = snapshot.stateChannelSnapshots.values.map( _=> ()).size,
            timestamp = timestamp
          )
        }

    }

}
