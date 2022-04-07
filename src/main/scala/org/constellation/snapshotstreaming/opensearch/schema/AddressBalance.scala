package org.constellation.snapshotstreaming.opensearch.schema

import java.util.Date

import io.circe.Encoder
import io.circe.generic.semiauto._

import schema._

final case class AddressBalance(
  address: String,
  balance: Long,
  snapshotHash: String,
  snapshotOrdinal: Long,
  timestamp: Date
) {
  def docId = s"${address}${snapshotOrdinal}"
}

object AddressBalance {

  implicit val addressBalanceEncoder: Encoder[AddressBalance] = deriveEncoder

}
