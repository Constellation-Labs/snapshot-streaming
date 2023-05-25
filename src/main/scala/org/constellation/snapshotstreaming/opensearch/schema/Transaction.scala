package org.constellation.snapshotstreaming.opensearch.schema

import java.util.Date

import org.tessellation.schema.transaction.{Transaction => OriginalTransaction}
import org.tessellation.security.signature.Signed

import io.circe.Encoder
import io.circe.generic.semiauto._

import schema._

final case class Transaction[T <: OriginalTransaction](
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
  transactionOriginal: Signed[T],
  timestamp: Date
)

object Transaction {

  implicit def transactionEncoder[T <: OriginalTransaction: Encoder]: Encoder[Transaction[T]] = deriveEncoder
}

case class TransactionReference(hash: String, ordinal: Long)

object TransactionReference {
  implicit val transactionReferenceEncoder: Encoder[TransactionReference] = deriveEncoder
}
