package org.constellation.snapshotstreaming.schema.extractors

import org.constellation.snapshotstreaming.schema.AllowSpends.{AllowSpend, TokenLock, TokenUnlock}
import org.constellation.snapshotstreaming.schema.schema.{GlobalData, MetagraphData}
import org.constellation.snapshotstreaming.schema._
import org.constellation.snapshotstreaming.schema.AllowSpends.{AllowSpend, TokenLock, TokenUnlock}
import org.constellation.snapshotstreaming.schema.schema.{GlobalData, MetagraphData}
import org.constellation.snapshotstreaming.schema.{AddressBalance, RewardTransaction, Transaction}

trait AddressExtractor[T] {
  def extractAddresses(value: T): Seq[String]
}

object AddressExtractor {

  implicit val addressSnapshotExtractor: AddressExtractor[Snapshot] = snapshot =>
    snapshot.rewards.toSeq.flatMap(
      rewardTransactionExtractor.extractAddresses
    )

  implicit val transactionExtractor: AddressExtractor[Transaction] = tx => Seq(tx.source, tx.destination)

  implicit val feeTransactionExtractor: AddressExtractor[FeeTransaction] = tx => Seq(tx.source, tx.destination)

  implicit val allowSpendExtractor: AddressExtractor[AllowSpend] = spend => Seq(spend.source, spend.destination)

  implicit val tokenLockExtractor: AddressExtractor[TokenLock] = lock => Seq(lock.source)

  implicit val tokenUnlockExtractor: AddressExtractor[TokenUnlock] = unlock => Seq(unlock.address)

  implicit val rewardTransactionExtractor: AddressExtractor[RewardTransaction] = reward => Seq(reward.destination)

  implicit val addressBalanceExtractor: AddressExtractor[AddressBalance] = balance => Seq(balance.address)

  implicit val addressCurrencySnapshotExtractor: AddressExtractor[CurrencySnapshot] = snapshot =>
    snapshot.rewards.toSeq.flatMap(
      rewardTransactionExtractor.extractAddresses
    ) ++ snapshot.ownerAddress ++ snapshot.stakingAddress

  implicit def currencyDataExtractor[A: AddressExtractor]: AddressExtractor[CurrencyData[A]] = data =>
    extract(data.data)

  implicit val globalDataExtractor: AddressExtractor[GlobalData] = data =>
    addressSnapshotExtractor.extractAddresses(data.snapshot) ++
      data.txs.flatMap(implicitly[AddressExtractor[Transaction]].extractAddresses) ++
      data.allowSpends.flatMap(implicitly[AddressExtractor[AllowSpend]].extractAddresses) ++
      data.tokenLocks.flatMap(implicitly[AddressExtractor[TokenLock]].extractAddresses) ++
      data.tokenUnlocks.flatMap(implicitly[AddressExtractor[TokenUnlock]].extractAddresses) ++
      data.balances.flatMap(implicitly[AddressExtractor[AddressBalance]].extractAddresses)

  implicit val metagraphDataExtractor: AddressExtractor[MetagraphData] = data =>
    data.snapshots.flatMap(implicitly[AddressExtractor[CurrencyData[CurrencySnapshot]]].extractAddresses) ++
      data.txs.flatMap(implicitly[AddressExtractor[CurrencyData[Transaction]]].extractAddresses) ++
      data.feeTxs.flatMap(implicitly[AddressExtractor[CurrencyData[FeeTransaction]]].extractAddresses) ++
      data.allowSpends.flatMap(implicitly[AddressExtractor[CurrencyData[AllowSpend]]].extractAddresses) ++
      data.tokenLocks.flatMap(implicitly[AddressExtractor[CurrencyData[TokenLock]]].extractAddresses) ++
      data.tokenUnlocks.flatMap(implicitly[AddressExtractor[CurrencyData[TokenUnlock]]].extractAddresses) ++
      data.balances.flatMap(implicitly[AddressExtractor[CurrencyData[AddressBalance]]].extractAddresses)

  def extract[T: AddressExtractor](value: T): Seq[String] =
    implicitly[AddressExtractor[T]].extractAddresses(value)

}
