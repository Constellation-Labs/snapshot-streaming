package org.constellation.snapshotstreaming.opensearch.mapper

import cats.effect.Async

import java.util.Date
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._

import scala.collection.immutable.{SortedMap, SortedSet}
import org.tessellation.schema.address.Address
import org.tessellation.schema.balance.Balance
import org.tessellation.schema.{Block => OriginalBlock}
import org.tessellation.schema.transaction.{RewardTransaction => OriginalRewardTransaction, Transaction => OriginalTransaction, TransactionReference => OriginalTransactionReference}
import org.tessellation.security.Hashed
import org.tessellation.security.signature.Signed
import eu.timepit.refined.auto._
import org.constellation.snapshotstreaming.opensearch.schema._
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.snapshot.{SnapshotInfo, Snapshot => OriginalSnapshot}
import org.tessellation.syntax.sortedCollection._

abstract class SnapshotMapper[F[_]: Async: KryoSerializer, S <: OriginalSnapshot] {

  def fetchRewards(snapshot: S): SortedSet[OriginalRewardTransaction]

  def hashBlock(block: Signed[OriginalBlock]): F[String] =
    block.toHashed.map(_.proofsHash.value)

  def hashTransaction(transaction: Signed[OriginalTransaction]): F[String] =
    transaction.toHashed.map(_.hash.value)

  def mapSnapshot(snapshot: Hashed[S], timestamp: Date): F[Snapshot] = for {
    blocksHashes <- snapshot.blocks.unsorted.map(_.block).map(hashBlock).toList.sequence
  } yield Snapshot(
    hash = snapshot.hash.value,
    ordinal = snapshot.ordinal.value.value,
    height = snapshot.height.value,
    subHeight = snapshot.subHeight.value,
    lastSnapshotHash = snapshot.lastSnapshotHash.value,
    blocks = blocksHashes.toSet,
    rewards = fetchRewards(snapshot).unsorted.map(reward =>
      RewardTransaction(
        reward.destination.value,
        reward.amount.value
      )
    ),
    timestamp = timestamp
  )

  def mapBlocks(snapshot: Hashed[S], timestamp: Date): F[Seq[Block]] = for {
    blocks <- snapshot.blocks.unsorted
      .map(_.block)
      .map(mapBlock(snapshot.hash.value, snapshot.ordinal.value.value, timestamp))
      .toList
      .sequence
  } yield blocks

  private def mapBlock(snapshotHash: String, snapshotOrdinal: Long, timestamp: Date)(
    block: Signed[OriginalBlock]
  ): F[Block] =
    for {
      blockHash <- hashBlock(block)
      transactionsHashes <- block.value.transactions.toSortedSet.unsorted.map(hashTransaction).toList.sequence
    } yield Block(
      hash = blockHash,
      height = block.height.value,
      parent = block.parent.map(br => BlockReference(br.hash.value, br.height.value)).toList.toSet,
      transactions = transactionsHashes.toSet,
      snapshotHash = snapshotHash,
      snapshotOrdinal = snapshotOrdinal,
      timestamp = timestamp
    )

  def mapTransactions(snapshot: Hashed[S], timestamp: Date) = for {
    transactions <- snapshot.blocks.unsorted
      .map(_.block)
      .map(mapTransactionsFromBlock(snapshot.hash.value, snapshot.ordinal.value.value, timestamp))
      .toList
      .sequence
  } yield transactions.flatten

  private def mapTransactionsFromBlock(snapshotHash: String, snapshotOrdinal: Long, timestamp: Date)(
    block: Signed[OriginalBlock]
  ) = for {
    blockHash <- hashBlock(block)
    transactions <- block.transactions.toSortedSet.unsorted
      .map(mapTransaction(blockHash, snapshotHash, snapshotOrdinal, timestamp))
      .toList
      .sequence
  } yield transactions

  private def mapTransaction(blockHash: String, snapshotHash: String, snapshotOrdinal: Long, timestamp: Date)(
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

  private def mapTransactionRef(nodeRef: OriginalTransactionReference): TransactionReference =
    TransactionReference(nodeRef.hash.value, nodeRef.ordinal.value)

  def snapshotReferredBalancesInfo(
                                    snapshot: S,
                                    info: SnapshotInfo[_]
                                  ): SortedMap[Address, Balance] = {
    val bothAddresses = extractTxnAddresses(txn => (txn.source, txn.destination))(snapshot)
    val rewardsAddresses = fetchRewards(snapshot).toList.map(_.destination)
    val addressesToKeep = bothAddresses.flatMap(addresses => List(addresses._1, addresses._2)) ++ rewardsAddresses
    val filteredBalances = info.balances.filter { case (address, _) => addressesToKeep.contains(address) }

    val srcTransactions = extractTxnAddresses(txn => txn.source)(snapshot)
    val setZeroBalances =
      (srcTransactions.toSet -- info.balances.keys.toSet)
        .map(address => address -> Balance(0L))
        .toSortedMap

    filteredBalances ++ setZeroBalances
  }

  private def extractTxnAddresses[A](getAddress: OriginalTransaction => A)(snapshot: S): List[A] =
    snapshot.blocks.toList.flatMap(
      _.block.transactions.toSortedSet.toList.map(signedTxn => getAddress(signedTxn.value))
    )
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
