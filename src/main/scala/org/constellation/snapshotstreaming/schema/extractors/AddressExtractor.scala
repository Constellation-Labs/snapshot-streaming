package org.constellation.snapshotstreaming.schema.extractors

//import org.constellation.snapshotstreaming.schema.AllowSpends.{AllowSpend, TokenLock, TokenUnlock}
import org.constellation.snapshotstreaming.schema.schema.{GlobalData, MetagraphData}
import org.constellation.snapshotstreaming.schema._
//import org.constellation.snapshotstreaming.schema.AllowSpends.{AllowSpend, TokenLock, TokenUnlock}
import org.constellation.snapshotstreaming.schema.schema.{GlobalData, MetagraphData}
import org.constellation.snapshotstreaming.schema.{AddressBalance, RewardTransaction, Transaction}

trait AddressExtractor[T] {
  def extractAddresses(value: T): Set[String]
}

object AddressExtractor {

  implicit val addressSnapshotExtractor: AddressExtractor[Snapshot] = snapshot =>
    snapshot.rewards.flatMap(
      rewardTransactionExtractor.extractAddresses
    )

  implicit val transactionExtractor: AddressExtractor[Transaction] = tx => Set(tx.source, tx.destination)

  implicit val feeTransactionExtractor: AddressExtractor[FeeTransaction] = tx => Set(tx.source, tx.destination)

//  implicit val allowSpendExtractor: AddressExtractor[AllowSpend] = spend => Set(spend.source, spend.destination)
//
//  implicit val tokenLockExtractor: AddressExtractor[TokenLock] = lock => Set(lock.source)
//
//  implicit val tokenUnlockExtractor: AddressExtractor[TokenUnlock] = unlock => Set(unlock.address)

  implicit val rewardTransactionExtractor: AddressExtractor[RewardTransaction] = reward => Set(reward.destination)

  implicit val addressBalanceExtractor: AddressExtractor[AddressBalance] = balance => Set(balance.address)

  implicit val addressCurrencySnapshotExtractor: AddressExtractor[CurrencySnapshot] = snapshot =>
    snapshot.rewards.flatMap(
      rewardTransactionExtractor.extractAddresses
    ) ++ snapshot.ownerAddress ++ snapshot.stakingAddress

  implicit def currencyDataExtractor[A: AddressExtractor]: AddressExtractor[CurrencyData[A]] = data =>
    extract(data.data)

  implicit val globalDataExtractor: AddressExtractor[GlobalData] = data =>
    addressSnapshotExtractor.extractAddresses(data.snapshot) ++
      data.txs.toSet.flatMap(implicitly[AddressExtractor[Transaction]].extractAddresses) ++
//      data.allowSpends.toSet.flatMap(implicitly[AddressExtractor[AllowSpend]].extractAddresses) ++
//      data.tokenLocks.toSet.flatMap(implicitly[AddressExtractor[TokenLock]].extractAddresses) ++
//      data.tokenUnlocks.toSet.flatMap(implicitly[AddressExtractor[TokenUnlock]].extractAddresses) ++
      data.balances.toSet.flatMap(implicitly[AddressExtractor[AddressBalance]].extractAddresses)

  implicit val metagraphDataExtractor: AddressExtractor[MetagraphData] = data =>
    data.snapshots.toSet.flatMap(implicitly[AddressExtractor[CurrencyData[CurrencySnapshot]]].extractAddresses) ++
      data.txs.toSet.flatMap(implicitly[AddressExtractor[CurrencyData[Transaction]]].extractAddresses) ++
      data.feeTxs.toSet.flatMap(implicitly[AddressExtractor[CurrencyData[FeeTransaction]]].extractAddresses) ++
//      data.allowSpends.toSet.flatMap(implicitly[AddressExtractor[CurrencyData[AllowSpend]]].extractAddresses) ++
//      data.tokenLocks.toSet.flatMap(implicitly[AddressExtractor[CurrencyData[TokenLock]]].extractAddresses) ++
//      data.tokenUnlocks.toSet.flatMap(implicitly[AddressExtractor[CurrencyData[TokenUnlock]]].extractAddresses) ++
      data.balances.toSet.flatMap(implicitly[AddressExtractor[CurrencyData[AddressBalance]]].extractAddresses)

  def extract[T: AddressExtractor](value: T): Set[String] =
    implicitly[AddressExtractor[T]].extractAddresses(value)

}
