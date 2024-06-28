package org.constellation.snapshotstreaming.opensearch.mapper

import java.util.Date
import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.SortedMap
import scala.collection.immutable.SortedSet
import org.tessellation.schema.address.Address
import org.tessellation.schema.balance.Balance
import org.tessellation.schema.snapshot.SnapshotInfo
import org.tessellation.schema.snapshot.{Snapshot => OriginalSnapshot}
import org.tessellation.schema.transaction.{TransactionReference => OriginalTransactionReference}
import org.tessellation.schema.transaction.{Transaction => OriginalTransaction}
import org.tessellation.schema.transaction.{RewardTransaction => OriginalRewardTransaction}
import org.tessellation.schema.{Block => OriginalBlock}
import org.tessellation.security.signature.Signed
import org.tessellation.security.Hashed
import org.tessellation.security.Hasher
import org.tessellation.syntax.sortedCollection._
import eu.timepit.refined.auto._
import org.constellation.snapshotstreaming.opensearch.schema._

case class SnapshotReferredAddresses(source: Set[Address], destination: Set[Address])

abstract class SnapshotMapper[F[_]: Async, S <: OriginalSnapshot, SI <: SnapshotInfo[_]] {

  def fetchRewards(snapshot: S): SortedSet[OriginalRewardTransaction]

  def hashBlock(block: Signed[OriginalBlock], hasher: Hasher[F]): F[String] = {
    implicit val h = hasher
    block.toHashed.map(_.proofsHash.value)
  }

  def hashTransaction(transaction: Signed[OriginalTransaction], txHasher: Hasher[F]): F[String] = {
    implicit val hasher = txHasher
    transaction.toHashed.map(_.hash.value)
  }

  def mapBlocks(snapshot: Hashed[S], timestamp: Date, txHasher: Hasher[F], hasher: Hasher[F]): F[Seq[Block]] = for {
    blocks <- snapshot.blocks.unsorted
      .map(_.block)
      .map(mapBlock(snapshot.hash.value, snapshot.ordinal.value.value, timestamp, txHasher, hasher))
      .toList
      .sequence
  } yield blocks

  private def mapBlock(
    snapshotHash: String,
    snapshotOrdinal: Long,
    timestamp: Date,
    txHasher: Hasher[F],
    hasher: Hasher[F]
  )(
    block: Signed[OriginalBlock]
  ): F[Block] =
    for {
      blockHash <- hashBlock(block, hasher)
      transactionsHashes <- block.value.transactions.toSortedSet.unsorted
        .map(hashTransaction(_, txHasher))
        .toList
        .sequence
    } yield Block(
      hash = blockHash,
      height = block.height.value,
      parent = block.parent.map(br => BlockReference(br.hash.value, br.height.value)).toList.toSet,
      transactions = transactionsHashes.toSet,
      snapshotHash = snapshotHash,
      snapshotOrdinal = snapshotOrdinal,
      timestamp = timestamp
    )

  def mapTransactions(snapshot: Hashed[S], timestamp: Date, txHasher: Hasher[F], hasher: Hasher[F]) = for {
    transactions <- snapshot.blocks.unsorted
      .map(_.block)
      .map(mapTransactionsFromBlock(snapshot.hash.value, snapshot.ordinal.value.value, timestamp, txHasher, hasher))
      .toList
      .sequence
  } yield transactions.flatten

  private def mapTransactionsFromBlock(
    snapshotHash: String,
    snapshotOrdinal: Long,
    timestamp: Date,
    txHasher: Hasher[F],
    hasher: Hasher[F]
  )(
    block: Signed[OriginalBlock]
  ) = for {
    blockHash <- hashBlock(block, hasher)
    transactions <- block.transactions.toSortedSet.unsorted
      .map(mapTransaction(blockHash, snapshotHash, snapshotOrdinal, timestamp, txHasher))
      .toList
      .sequence
  } yield transactions

  private def mapTransaction(
    blockHash: String,
    snapshotHash: String,
    snapshotOrdinal: Long,
    timestamp: Date,
    txHasher: Hasher[F]
  )(
    transaction: Signed[OriginalTransaction]
  ): F[Transaction] = for {
    transactionHash <- hashTransaction(transaction, txHasher)
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

  private def mapTransactionRef(nodeRef: OriginalTransactionReference): TransactionReference =
    TransactionReference(nodeRef.hash.value, nodeRef.ordinal.value)

  def balanceDiff(
    snapshot: S,
    prevBalances: Option[SortedMap[Address, Balance]],
    info: SnapshotInfo[_],
  ): SortedMap[Address, Balance] =
    prevBalances match {
      case Some(prev) =>
        val changed = info.balances.filterNot { case (address, balance) =>
          prev.get(address).exists(_ === balance)
        }

        /* NOTE: SnapshotInfo calculation optimization gets rid of addresses that have empty balances.
         It is fine for node but in snapshot-streaming we need to keep such addresses set to Balance.empty.
         We do that by finding addresses that are missing in info but are referenced in the transactions.
         */
        val explicitlyZeroed = {
          val srcTransactions = extractSnapshotReferredAddresses(snapshot).source
          (srcTransactions -- info.balances.keys)
            .map(address => address -> Balance.empty)
            .toSortedMap
        }

        changed ++ explicitlyZeroed
      case None =>
        info.balances
    }

  def extractSnapshotReferredAddresses(snapshot: S): SnapshotReferredAddresses

  def mapBalances(
    globalSnapshot: Hashed[S],
    balances: SortedMap[Address, Balance],
    timestamp: Date
  ): Seq[AddressBalance] =
    balances.toSeq.map { case (address, balance) =>
      AddressBalance(
        address = address.value.value,
        balance = balance.value.value,
        snapshotHash = globalSnapshot.hash.value,
        snapshotOrdinal = globalSnapshot.ordinal.value.value,
        timestamp = timestamp
      )
    }.toList

}
