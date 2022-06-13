package org.constellation.snapshotstreaming.opensearch.mapper

import java.util.Date

import cats.effect.Async
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._

import org.tessellation.dag.domain.block.{DAGBlock => OriginalDAGBlock}
import org.tessellation.dag.snapshot.GlobalSnapshot
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.transaction.{
  Transaction => OriginalTransaction,
  TransactionReference => OriginalTransactionReference
}
import org.tessellation.security.signature.Signed

import eu.timepit.refined.auto.autoUnwrap
import org.constellation.snapshotstreaming.opensearch.schema._

trait GlobalSnapshotMapper[F[_]] {
  def mapSnapshot(globalSnapshot2: Signed[GlobalSnapshot], timestamp: Date): F[Snapshot]
  def mapBlocks(globalSnapshot2: Signed[GlobalSnapshot], timestamp: Date): F[Seq[Block]]
  def mapTransactions(globalSnapshot: Signed[GlobalSnapshot], timestamp: Date): F[Seq[Transaction]]
  def mapBalances(globalSnapshot: Signed[GlobalSnapshot], timestamp: Date): F[Seq[AddressBalance]]
}

object GlobalSnapshotMapper {

  def make[F[_]: Async: KryoSerializer](): GlobalSnapshotMapper[F] =
    new GlobalSnapshotMapper[F] {

      def mapSnapshot(globalSnapshot: Signed[GlobalSnapshot], timestamp: Date): F[Snapshot] = for {
        snapshotHash <- hashSnapshot(globalSnapshot)
        blocksHashes <- globalSnapshot.blocks.unsorted.map(_.block).map(hashBlock).toList.sequence
      } yield Snapshot(
        hash = snapshotHash,
        ordinal = globalSnapshot.ordinal.value.value,
        height = globalSnapshot.height.value,
        subHeight = globalSnapshot.subHeight.value,
        lastSnapshotHash = globalSnapshot.lastSnapshotHash.value,
        blocks = blocksHashes.toSet,
        rewards = globalSnapshot.rewards.unsorted.map(reward =>
          RewardTransaction(
            reward.destination.value,
            reward.amount.value
          )
        ),
        timestamp = timestamp
      )

      def hashSnapshot(globalSnapshot: Signed[GlobalSnapshot]) =
        globalSnapshot.toHashed.map(_.hash.value)

      def mapBlocks(globalSnapshot: Signed[GlobalSnapshot], timestamp: Date): F[Seq[Block]] = for {
        snapshotHash <- hashSnapshot(globalSnapshot)
        blocks <- globalSnapshot.blocks.unsorted
          .map(_.block)
          .map(mapBlock(snapshotHash, globalSnapshot.ordinal.value.value, timestamp))
          .toList
          .sequence
      } yield blocks

      def mapBlock(snapshotHash: String, snapshotOrdinal: Long, timestamp: Date)(
        block: Signed[OriginalDAGBlock]
      ): F[Block] =
        for {
          blockHash <- hashBlock(block)
          transactionsHashes <- block.value.transactions.unsorted.map(hashTransaction).toList.sequence
        } yield Block(
          hash = blockHash,
          height = block.height.value,
          parent = block.parent.map(br => BlockReference(br.hash.value, br.height.value)).toList.toSet,
          transactions = transactionsHashes.toSet,
          snapshotHash = snapshotHash,
          snapshotOrdinal = snapshotOrdinal,
          timestamp = timestamp
        )

      def hashBlock(nodeBlock: Signed[OriginalDAGBlock]): F[String] =
        nodeBlock.toHashed.map(_.proofsHash.value)

      def mapTransactions(globalSnapshot: Signed[GlobalSnapshot], timestamp: Date) = for {
        snapshotHash <- hashSnapshot(globalSnapshot)
        transactions <- globalSnapshot.blocks.unsorted
          .map(_.block)
          .map(mapTransactionsFromBlock(snapshotHash, globalSnapshot.ordinal.value.value, timestamp))
          .toList
          .sequence
      } yield transactions.flatten

      def mapTransactionsFromBlock(snapshotHash: String, snapshotOrdinal: Long, timestamp: Date)(
        block: Signed[OriginalDAGBlock]
      ) = for {
        blockHash <- hashBlock(block)
        transactions <- block.transactions.unsorted
          .map(mapTransaction(blockHash, snapshotHash, snapshotOrdinal, timestamp))
          .toList
          .sequence
      } yield transactions

      def mapTransaction(blockHash: String, snapshotHash: String, snapshotOrdinal: Long, timestamp: Date)(
        transaction: Signed[OriginalTransaction]
      ): F[Transaction] = for {
        transactionHash <- hashTransaction(transaction)
      } yield Transaction(
        hash = transactionHash,
        amount = transaction.amount.value,
        source = transaction.source.value,
        destination = transaction.destination.value,
        fee = transaction.fee.value,
        parent = mapTransactionRef(transaction.parent),
        salt = transaction.salt.value,
        blockHash = blockHash,
        snapshotHash = snapshotHash,
        snapshotOrdinal = snapshotOrdinal,
        transactionOriginal = transaction,
        timestamp = timestamp
      )

      def hashTransaction(nodeTransaction: Signed[OriginalTransaction]): F[String] =
        nodeTransaction.toHashed.map(_.hash.value)

      def mapTransactionRef(nodeRef: OriginalTransactionReference): TransactionReference =
        TransactionReference(nodeRef.hash.value, nodeRef.ordinal.value)

      def mapBalances(globalSnapshot: Signed[GlobalSnapshot], timestamp: Date): F[Seq[AddressBalance]] =
        for {
          snapshotHash <- hashSnapshot(globalSnapshot)
        } yield globalSnapshot.info.balances.toSeq.map { case (address, balance) =>
          AddressBalance(
            address = address.value.value,
            balance = balance.value.value,
            snapshotHash = snapshotHash,
            snapshotOrdinal = globalSnapshot.ordinal.value.value,
            timestamp = timestamp
          )
        }.toList

    }

}
