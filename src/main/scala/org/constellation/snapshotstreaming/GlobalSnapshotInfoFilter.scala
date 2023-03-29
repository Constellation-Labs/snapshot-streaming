package org.constellation.snapshotstreaming

import scala.collection.immutable.SortedMap

import org.tessellation.schema.address.Address
import org.tessellation.schema.balance.Balance
import org.tessellation.schema.transaction.DAGTransaction
import org.tessellation.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo}
import org.tessellation.syntax.sortedCollection._

import eu.timepit.refined.auto._

object GlobalSnapshotInfoFilter {

  def snapshotReferredBalancesInfo(
    snapshot: GlobalIncrementalSnapshot,
    info: GlobalSnapshotInfo
  ): SortedMap[Address, Balance] = {
    val bothAddresses = extractTxnAddresses(txn => (txn.source, txn.destination))(snapshot)
    val rewardsAddresses = snapshot.rewards.toList.map(_.destination)
    val addressesToKeep = bothAddresses.flatMap(addresses => List(addresses._1, addresses._2)) ++ rewardsAddresses
    val filteredBalances = info.balances.filter { case (address, _) => addressesToKeep.contains(address) }

    val srcTransactions = extractTxnAddresses(txn => txn.source)(snapshot)
    val setZeroBalances =
      (srcTransactions.toSet -- info.balances.keys.toSet)
        .map(address => address -> Balance(0L))
        .toSortedMap

    filteredBalances ++ setZeroBalances
  }

  private def extractTxnAddresses[T](getAddress: DAGTransaction => T)(snapshot: GlobalIncrementalSnapshot): List[T] =
    snapshot.blocks.toList.flatMap(
      _.block.transactions.toSortedSet.toList.map(signedTxn => getAddress(signedTxn.value))
    )

}
