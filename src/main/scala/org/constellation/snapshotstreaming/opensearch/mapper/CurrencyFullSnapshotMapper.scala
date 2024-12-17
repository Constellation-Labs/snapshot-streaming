package org.constellation.snapshotstreaming.opensearch.mapper

import cats.effect.Async
import cats.syntax.all._
import org.constellation.snapshotstreaming.opensearch.schema.Snapshot

import scala.collection.immutable.SortedSet
import io.constellationnetwork.currency.schema.currency.{CurrencySnapshot => OriginalCurrencySnapshot}
import io.constellationnetwork.schema.transaction
import io.constellationnetwork.security.Hashed
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.syntax.sortedCollection._
import eu.timepit.refined.auto._
import io.estatico.newtype.ops._
import org.constellation.snapshotstreaming.opensearch.schema._
import io.constellationnetwork.currency.schema.currency.CurrencySnapshotInfo

import java.util.Date

abstract class CurrencyFullSnapshotMapper[F[_]: Async] extends SnapshotMapper[F, OriginalCurrencySnapshot, CurrencySnapshotInfo] {
  def mapSnapshot(snapshot: Hashed[OriginalCurrencySnapshot], timestamp: Date, hasher: Hasher[F]): F[Snapshot]
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

      def mapSnapshot(snapshot: Hashed[OriginalCurrencySnapshot], timestamp: Date, hasher: Hasher[F]): F[Snapshot] =
        for {
          blocksHashes <- snapshot.blocks.unsorted.map(_.block).map(hashBlock(_, hasher)).toList.sequence
        } yield Snapshot(
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
