package org.constellation.snapshotstreaming.opensearch.mapper

import cats.Monad
import cats.effect.Async
import cats.syntax.flatMap._
import cats.syntax.functor._
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.block.DAGBlock
import org.tessellation.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, snapshot, transaction}
import org.tessellation.schema.transaction.DAGTransaction
import org.tessellation.security.Hashed
import org.tessellation.security.signature.Signed

import java.util.Date
import scala.collection.immutable.SortedSet

abstract class GlobalSnapshotMapper[F[_]: Monad] extends SnapshotMapper[F, DAGTransaction, DAGBlock, GlobalIncrementalSnapshot] {

  def mapGlobalSnapshot(globalSnapshot: Hashed[GlobalIncrementalSnapshot], info: GlobalSnapshotInfo, timestamp: Date) =
    for {
      snapshot <- mapSnapshot(globalSnapshot, timestamp)
      blocks <- mapBlocks(globalSnapshot, timestamp)
      transactions <- mapTransactions(globalSnapshot, timestamp)
      filteredBalances = snapshotReferredBalancesInfo(
        globalSnapshot.signed.value,
        info
      )
      balances = mapBalances(globalSnapshot, filteredBalances, timestamp)
    } yield (snapshot, blocks, transactions, balances)
}

object GlobalSnapshotMapper {

  def make[F[_] : Async : KryoSerializer](): GlobalSnapshotMapper[F] =
    new GlobalSnapshotMapper[F] {

      def hashBlock(block: Signed[DAGBlock]): F[String] =
        block.toHashed.map(_.proofsHash.value)

      def hashTransaction(transaction: Signed[DAGTransaction]): F[String] =
        transaction.toHashed.map(_.hash.value)

      def fetchRewards(snapshot: GlobalIncrementalSnapshot): SortedSet[transaction.RewardTransaction] =
        snapshot.rewards
    }

}
