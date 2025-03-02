package org.constellation.snapshotstreaming.schema

import io.circe.Encoder
import io.circe.generic.semiauto._

import java.time.LocalDateTime

final case class AddressBalance(
  address: String,
  balance: Long,
  snapshotHash: String,
  snapshotOrdinal: Long,
  timestamp: LocalDateTime
)

object AddressBalance {

  implicit val addressBalanceEncoder: Encoder[AddressBalance] = deriveEncoder

}
