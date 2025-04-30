package org.constellation.snapshotstreaming.schema

import io.circe.Encoder
import io.circe.generic.semiauto._
import io.constellationnetwork.schema.transaction.{Transaction => OriginalTransaction}
import io.constellationnetwork.security.signature.Signed

import java.time.LocalDateTime

final case class Transaction(
  hash: String,
  amount: Long,
  source: String,
  destination: String,
  fee: Long,
  parent: TransactionReference,
  salt: Long,
  blockHash: String,
  snapshotHash: String,
  snapshotOrdinal: Long,
  transactionOriginal: Signed[OriginalTransaction],
  ordinal: Long,
  timestamp: LocalDateTime
)

object Transaction {

  implicit def transactionEncoder: Encoder[Transaction] = deriveEncoder
}

case class TransactionReference(hash: String, ordinal: Long)

object TransactionReference {
  implicit val transactionReferenceEncoder: Encoder[TransactionReference] = deriveEncoder
}
