package org.constellation.snapshotstreaming.opensearch.mapper

import cats.effect.Async
import org.tessellation.currency.schema.currency.CurrencySnapshot
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.transaction

import scala.collection.immutable.SortedSet

abstract class CurrencyFullSnapshotMapper[F[_]: Async: KryoSerializer] extends SnapshotMapper[F, CurrencySnapshot] {}

object CurrencyFullSnapshotMapper {

  def make[F[_]: Async: KryoSerializer](): CurrencyFullSnapshotMapper[F] =
    new CurrencyFullSnapshotMapper[F] {

      def fetchRewards(snapshot: CurrencySnapshot): SortedSet[transaction.RewardTransaction] =
        snapshot.rewards
    }
}
