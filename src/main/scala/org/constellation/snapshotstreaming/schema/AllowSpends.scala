package org.constellation.snapshotstreaming.schema

import java.util.UUID

object AllowSpends {

  case class AllowSpend(
    hash: String,
    source: String,
    destination: String,
    currency: Option[String],
    amount: Long,
    fee: Long,
    parent: TransactionReference,
    lastValidEpochProgress: Long,
    roundId: UUID,
    ordinal: Long,
    approvers: List[String]
  )

  case class SpendTransaction(
    allowSpendRef: Option[String],
    currency: Option[String],
    amount: Long,
    destination: String
  )

  case class TokenLock(
    snapshotHash: String,
    hash: String,
    source: String,
    amount: Long,
    parent: TransactionReference,
    currencyId: Option[String],
    unlockEpoch: Long,
    ordinal: Long
  )

  case class TokenUnlock(
    lockReference: TransactionReference,
    amount: Long,
    currencyId: Option[String],
    address: String
  )

}
