package org.constellation.snapshotstreaming.opensearch.mapper

import cats.effect.Async

import scala.collection.immutable.SortedSet

import org.tessellation.currency.schema.currency.CurrencyIncrementalSnapshot
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.transaction.{RewardTransaction => OriginalRewardTransaction}
import org.tessellation.security.Hasher

abstract class CurrencyIncrementalSnapshotMapper[F[_]: Async: KryoSerializer: Hasher] extends SnapshotMapper[F, CurrencyIncrementalSnapshot]

object CurrencyIncrementalSnapshotMapper {

  def make[F[_]: Async: KryoSerializer: Hasher](): CurrencyIncrementalSnapshotMapper[F] =
    new CurrencyIncrementalSnapshotMapper[F] {

      def fetchRewards(snapshot: CurrencyIncrementalSnapshot): SortedSet[OriginalRewardTransaction] =
        snapshot.rewards
    }
}
