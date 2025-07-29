package org.constellation.snapshotstreaming.schema

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

import java.util.UUID

object AllowSpends {

  case class AllowSpend(
    hash: String,
    currencyId: Option[String],
    source: String,
    destination: String,
    amount: Long,
    fee: Long,
    parent: TransactionReference,
    lastValidEpochProgress: Long,
    roundId: UUID,
    ordinal: Long,
    approvers: List[String],
    snapshotHash: String,
  )

  object AllowSpend {
    implicit def allowSpendEncoder: Encoder[AllowSpend] = deriveEncoder
  }

  case class SpendTransaction(
    hash: String,
    currencyId: Option[String],
    source: String,
    destination: String,
    amount: Long,
    allowSpendRef: Option[String],
    snapshotHash: String,
    metagraphId: Option[String],
  )

  object SpendTransaction {
    implicit def spendTxEncoder: Encoder[SpendTransaction] = deriveEncoder
  }

  case class AllowSpendExpiration(
    snapshotHash: String,
    hash: String,
    allowSpendRef: String,
  )

  object AllowSpendExpiration {
    implicit def allowSpendExpirationEncoder: Encoder[SpendTransaction] = deriveEncoder
  }

}
