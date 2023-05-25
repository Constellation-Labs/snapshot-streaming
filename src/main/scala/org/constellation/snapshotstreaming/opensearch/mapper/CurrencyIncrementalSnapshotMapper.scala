package org.constellation.snapshotstreaming.opensearch.mapper

import cats.Monad
import cats.effect.Async
import cats.syntax.functor._
import org.tessellation.currency.schema.currency.{CurrencyBlock, CurrencyIncrementalSnapshot, CurrencyTransaction}
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.transaction.{RewardTransaction => OriginalRewardTransaction}
import org.tessellation.security.signature.Signed

import scala.collection.immutable.SortedSet

abstract class CurrencyIncrementalSnapshotMapper[F[_]: Monad] extends SnapshotMapper[F, CurrencyTransaction, CurrencyBlock, CurrencyIncrementalSnapshot]

object CurrencyIncrementalSnapshotMapper {

  def make[F[_] : Async : KryoSerializer](): CurrencyIncrementalSnapshotMapper[F] =
    new CurrencyIncrementalSnapshotMapper[F] {

      def hashBlock(block: Signed[CurrencyBlock]): F[String] =
        block.toHashed.map(_.proofsHash.value)

      def hashTransaction(transaction: Signed[CurrencyTransaction]): F[String] =
        transaction.toHashed.map(_.hash.value)

      def fetchRewards(snapshot: CurrencyIncrementalSnapshot): SortedSet[OriginalRewardTransaction] =
        snapshot.rewards
    }
}
