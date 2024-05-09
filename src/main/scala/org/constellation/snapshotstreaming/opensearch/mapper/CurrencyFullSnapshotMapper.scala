package org.constellation.snapshotstreaming.opensearch.mapper

import cats.effect.Async

import scala.collection.immutable.SortedSet
import org.tessellation.currency.schema.currency.CurrencySnapshot
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.transaction

abstract class CurrencyFullSnapshotMapper[F[_]: Async: KryoSerializer] extends SnapshotMapper[F, CurrencySnapshot] {}

object CurrencyFullSnapshotMapper {

  def make[F[_]: Async: KryoSerializer](): CurrencyFullSnapshotMapper[F] =
    new CurrencyFullSnapshotMapper[F] {

      def fetchRewards(snapshot: CurrencySnapshot): SortedSet[transaction.RewardTransaction] =
        snapshot.rewards

      def extractSnapshotReferredAddresses(snapshot: CurrencySnapshot): SnapshotReferredAddresses = {
        val transactions = snapshot.blocks.flatMap(_.block.transactions.toSortedSet)
        val source = transactions.map(_.source)
        val destination = transactions.map(_.destination)
        SnapshotReferredAddresses(source, destination)
      }
    }

}
