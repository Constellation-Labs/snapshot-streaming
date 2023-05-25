package org.constellation.snapshotstreaming.opensearch.mapper

import cats.Monad
import cats.effect.Async
import cats.syntax.functor._
import org.tessellation.currency.schema.currency.{CurrencyBlock, CurrencySnapshot, CurrencyTransaction}
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.transaction
import org.tessellation.security.signature.Signed

import scala.collection.immutable.SortedSet

abstract class CurrencyFullSnapshotMapper[F[_]: Monad] extends SnapshotMapper[F, CurrencyTransaction, CurrencyBlock, CurrencySnapshot] {}

object CurrencyFullSnapshotMapper {

  def make[F[_]: Async: KryoSerializer](): CurrencyFullSnapshotMapper[F] =
    new CurrencyFullSnapshotMapper[F] {
      override def hashBlock(block: Signed[CurrencyBlock]): F[String] =
        block.toHashed.map(_.proofsHash.value)

      override def hashTransaction(transaction: Signed[CurrencyTransaction]): F[String] =
        transaction.toHashed.map(_.hash.value)

      override def fetchRewards(snapshot: CurrencySnapshot): SortedSet[transaction.RewardTransaction] =
        snapshot.rewards
    }
}
