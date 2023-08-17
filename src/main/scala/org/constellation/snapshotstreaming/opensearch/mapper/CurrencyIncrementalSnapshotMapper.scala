package org.constellation.snapshotstreaming.opensearch.mapper

import cats.effect.Async
import org.tessellation.currency.schema.currency.CurrencyIncrementalSnapshot
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.transaction.{RewardTransaction => OriginalRewardTransaction}

import scala.collection.immutable.SortedSet

abstract class CurrencyIncrementalSnapshotMapper[F[_]: Async: KryoSerializer] extends SnapshotMapper[F, CurrencyIncrementalSnapshot]

object CurrencyIncrementalSnapshotMapper {

  def make[F[_]: Async: KryoSerializer](): CurrencyIncrementalSnapshotMapper[F] =
    new CurrencyIncrementalSnapshotMapper[F] {

      def fetchRewards(snapshot: CurrencyIncrementalSnapshot): SortedSet[OriginalRewardTransaction] =
        snapshot.rewards
    }
}
