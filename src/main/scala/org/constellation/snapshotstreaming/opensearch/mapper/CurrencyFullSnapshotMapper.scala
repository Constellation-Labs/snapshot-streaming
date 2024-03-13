package org.constellation.snapshotstreaming.opensearch.mapper

import cats.effect.Async

import scala.collection.immutable.SortedSet

import org.tessellation.currency.schema.currency.CurrencySnapshot
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.transaction
import org.tessellation.security.Hasher

abstract class CurrencyFullSnapshotMapper[F[_]: Async: KryoSerializer: Hasher] extends SnapshotMapper[F, CurrencySnapshot] {}

object CurrencyFullSnapshotMapper {

  def make[F[_]: Async: KryoSerializer: Hasher](): CurrencyFullSnapshotMapper[F] =
    new CurrencyFullSnapshotMapper[F] {

      def fetchRewards(snapshot: CurrencySnapshot): SortedSet[transaction.RewardTransaction] =
        snapshot.rewards
    }
}
