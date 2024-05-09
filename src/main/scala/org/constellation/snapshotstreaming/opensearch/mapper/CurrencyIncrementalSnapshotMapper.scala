package org.constellation.snapshotstreaming.opensearch.mapper

import cats.effect.Async

import scala.collection.immutable.SortedSet
import org.tessellation.currency.schema.currency.CurrencyIncrementalSnapshot
import org.tessellation.currency.schema.feeTransaction.FeeTransaction
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.transaction.{RewardTransaction => OriginalRewardTransaction}
import org.tessellation.security.signature.Signed

abstract class CurrencyIncrementalSnapshotMapper[F[_]: Async: KryoSerializer]
    extends SnapshotMapper[F, CurrencyIncrementalSnapshot]

object CurrencyIncrementalSnapshotMapper {

  def make[F[_]: Async: KryoSerializer](): CurrencyIncrementalSnapshotMapper[F] =
    new CurrencyIncrementalSnapshotMapper[F] {

      def fetchRewards(snapshot: CurrencyIncrementalSnapshot): SortedSet[OriginalRewardTransaction] =
        snapshot.rewards

      def extractSnapshotReferredAddresses(snapshot: CurrencyIncrementalSnapshot): SnapshotReferredAddresses = {
        val transactions = snapshot.blocks.flatMap(_.block.transactions.toSortedSet)
        val feeTransactions = snapshot.feeTransactions.getOrElse(SortedSet.empty[Signed[FeeTransaction]])
        val source = transactions.map(_.source) ++ feeTransactions.map(_.source)
        val destination = transactions.map(_.destination) ++ feeTransactions.map(_.destination)
        SnapshotReferredAddresses(source, destination)
      }
    }

}
