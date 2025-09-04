package org.constellation.snapshotstreaming.mapper

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.constellationnetwork.currency.schema.currency.{CurrencySnapshotInfo, CurrencySnapshot => OriginalCurrencySnapshot}
import io.constellationnetwork.schema.transaction
import io.constellationnetwork.security.{Hashed, Hasher}
import org.constellation.snapshotstreaming.schema.{RewardTransaction, Snapshot}

import java.time.LocalDateTime
import scala.collection.immutable.SortedSet


abstract class CurrencyFullSnapshotMapper[F[_]: Async] extends SnapshotMapper[F, OriginalCurrencySnapshot] {
  def mapSnapshot(snapshot: Hashed[OriginalCurrencySnapshot], timestamp: LocalDateTime, hasher: Hasher[F]): F[Snapshot]
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

      def mapSnapshot(snapshot: Hashed[OriginalCurrencySnapshot], timestamp: LocalDateTime, hasher: Hasher[F]): F[Snapshot] =
        for {
          blocksHashes <- snapshot.blocks.unsorted.map(_.block).map(hashBlock(_, hasher)).toList.sequence
        } yield Snapshot(
          hash = snapshot.hash.value,
          ordinal = snapshot.ordinal.value.value,
          height = snapshot.height.value,
          subHeight = snapshot.subHeight.value,
          lastSnapshotHash = snapshot.lastSnapshotHash.value,
          blocks = blocksHashes.toSet,
          rewards = fetchRewards(snapshot).toSeq.map(reward =>
            RewardTransaction(
              reward.destination.value,
              reward.amount.value
            )
          ),
          epochProgress = snapshot.epochProgress.value,
          timestamp = timestamp,
          version = snapshot.version.version
        )
    }

}