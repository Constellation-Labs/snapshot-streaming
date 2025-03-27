package org.constellation.snapshotstreaming.mapper

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import org.tessellation.currency.schema.currency.{CurrencySnapshot => OriginalCurrencySnapshot, CurrencySnapshotInfo}
import org.tessellation.schema.transaction
import org.tessellation.security.{Hashed, Hasher}
import org.constellation.snapshotstreaming.schema.{CurrencySnapshot, RewardTransaction}
import org.tessellation.security.hash.Hash

import java.time.LocalDateTime
import scala.collection.immutable.SortedSet

abstract class CurrencyFullSnapshotMapper[F[_]: Async]
    extends SnapshotMapper[F, OriginalCurrencySnapshot, CurrencySnapshotInfo] {

  def mapSnapshot(
    globalSnapshotHash: Hash,
    snapshot: Hashed[OriginalCurrencySnapshot],
    timestamp: LocalDateTime,
    hasher: Hasher[F]
  ): F[CurrencySnapshot]

}

object CurrencyFullSnapshotMapper {

  def make[F[_]: Async](): CurrencyFullSnapshotMapper[F] =
    new CurrencyFullSnapshotMapper[F] {

      def fetchRewards(snapshot: OriginalCurrencySnapshot): SortedSet[transaction.RewardTransaction] =
        snapshot.rewards

      def extractSnapshotReferredAddresses(snapshot: OriginalCurrencySnapshot): SnapshotReferredAddresses = {
        val transactions = snapshot.blocks.flatMap(_.block.transactions.toSortedSet)
        val source = transactions.map(_.source)
        val destination = transactions.map(_.destination)
        SnapshotReferredAddresses(source, destination)
      }

      def mapSnapshot(
        globalSnapshotHash: Hash,
        snapshot: Hashed[OriginalCurrencySnapshot],
        timestamp: LocalDateTime,
        hasher: Hasher[F]
      ): F[CurrencySnapshot] =
        for {
          blocksHashes <- snapshot.blocks.unsorted.map(_.block).map(hashBlock(_, hasher)).toList.sequence
          rewards = fetchRewards(snapshot).unsorted.map(reward =>
            RewardTransaction(
              snapshot.hash.value,
              reward.destination.value,
              reward.amount.value
            )
          )
        } yield CurrencySnapshot(
          globalSnapshotHash.value,
          hash = snapshot.hash.value,
          ordinal = snapshot.ordinal.value.value,
          height = snapshot.height.value,
          subHeight = snapshot.subHeight.value,
          lastSnapshotHash = snapshot.lastSnapshotHash.value,
          blocks = blocksHashes.toSet,
          rewards = rewards,
          epochProgress = snapshot.epochProgress.value,
          timestamp = timestamp,
          sizeInKB = 0,
          version = snapshot.version.version
        )

    }

}
