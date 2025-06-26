package org.constellation.snapshotstreaming.mapper

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.constellationnetwork.currency.schema.currency.{CurrencySnapshot => OriginalCurrencySnapshot}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.snapshot.{Snapshot => OriginalSnapshot, SnapshotInfo}
import io.constellationnetwork.schema.transaction.{
  RewardTransaction => OriginalRewardTransaction,
  Transaction => OriginalTransaction,
  TransactionReference => OriginalTransactionReference
}
import io.constellationnetwork.schema.{Block => OriginalBlock}
import io.constellationnetwork.security.{Hashed, Hasher}
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.syntax.sortedCollection._
import org.constellation.snapshotstreaming.schema.{
  AddressBalance,
  Block,
  BlockReference,
  Transaction,
  TransactionReference
}
import io.circe.syntax._

import java.time.LocalDateTime
import scala.collection.immutable.{HashMap, SortedMap, SortedSet}

case class SnapshotReferredAddresses(source: Set[Address], destination: Set[Address])

abstract class SnapshotMapper[F[_]: Async, S <: OriginalSnapshot] {

  def fetchRewards(snapshot: S): SortedSet[OriginalRewardTransaction]

  def hashBlock(block: Signed[OriginalBlock], hasher: Hasher[F]): F[String] = {
    implicit val h = hasher
    block.toHashed.map(_.proofsHash.value)
  }

  def hashTransaction(transaction: Signed[OriginalTransaction], txHasher: Hasher[F]): F[String] = {
    implicit val hasher = txHasher
    transaction.toHashed.map(_.hash.value)
  }

  def mapBlocks(snapshot: Hashed[S], timestamp: LocalDateTime, txHasher: Hasher[F], hasher: Hasher[F]): F[Seq[Block]] =
    for {
      blocks <- snapshot.blocks.unsorted
        .map(_.block)
        .map(mapBlock(snapshot.hash.value, snapshot.ordinal.value.value, timestamp, txHasher, hasher))
        .toList
        .sequence
    } yield blocks

  private def mapBlock(
    snapshotHash: String,
    snapshotOrdinal: Long,
    timestamp: LocalDateTime,
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

  def mapTransactions(snapshot: Hashed[S], timestamp: LocalDateTime, txHasher: Hasher[F], hasher: Hasher[F]) = for {
    transactions <- snapshot.blocks.unsorted
      .map(_.block)
      .map(mapTransactionsFromBlock(snapshot.hash.value, snapshot.ordinal.value.value, timestamp, txHasher, hasher))
      .toList
      .sequence
  } yield transactions.flatten

  private def mapTransactionsFromBlock(
    snapshotHash: String,
    snapshotOrdinal: Long,
    timestamp: LocalDateTime,
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
    timestamp: LocalDateTime,
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
    transactionOriginal = transaction.asJson,
    ordinal = transaction.value.ordinal.value.value,
    timestamp = timestamp
  )

  private def mapTransactionRef(nodeRef: OriginalTransactionReference): TransactionReference =
    TransactionReference(nodeRef.hash.value, nodeRef.ordinal.value)

  def balanceDiff(
    snapshot: S,
    prevBalances: Option[SortedMap[Address, Balance]],
    info: SnapshotInfo[_]
  ): SortedMap[Address, Balance] =
    prevBalances match {
      case Some(prev) =>
        val optimizedPrev = HashMap.from(prev)
        val changed = info.balances.filterNot { case (address, balance) =>
          optimizedPrev.get(address).exists(_.value.value === balance.value.value)
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


}
