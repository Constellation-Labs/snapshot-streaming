package org.constellation.snapshotstreaming.opensearch.schema

import java.util.Date
import io.circe.Encoder
import io.circe.generic.semiauto._
import schema._

final case class FeeTransaction(
  hash: String,
  amount: Long,
  source: String,
  destination: String,
  parent: FeeTransactionReference,
  salt: Long,
  snapshotHash: String,
  snapshotOrdinal: Long,
  timestamp: Date
)

object FeeTransaction {
  implicit def feeTransactionEncoder: Encoder[FeeTransaction] = deriveEncoder
}


case class FeeTransactionReference(hash: String, ordinal: Long)

object FeeTransactionReference {
  implicit val feeTransactionReferenceEncoder: Encoder[FeeTransactionReference] = deriveEncoder
}
