package org.constellation.snapshotstreaming.schema

import io.circe.Encoder
import io.circe.generic.semiauto._

import java.time.LocalDateTime

final case class FeeTransaction(
  hash: String,
  amount: Long,
  source: String,
  destination: String,
  dataUpdateRef: String,
  snapshotHash: String,
  snapshotOrdinal: Long,
  timestamp: LocalDateTime,
)

object FeeTransaction {
  implicit def feeTransactionEncoder: Encoder[FeeTransaction] = deriveEncoder
}


case class FeeTransactionReference(hash: String, ordinal: Long)

object FeeTransactionReference {
  implicit val feeTransactionReferenceEncoder: Encoder[FeeTransactionReference] = deriveEncoder
}
