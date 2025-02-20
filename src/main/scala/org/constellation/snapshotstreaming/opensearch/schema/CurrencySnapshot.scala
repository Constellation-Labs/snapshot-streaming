package org.constellation.snapshotstreaming.opensearch.schema

import java.util.Date

import io.circe.Encoder
import io.circe.generic.semiauto._

import schema._

final case class CurrencySnapshot(
  hash: String,
  ordinal: Long,
  height: Long,
  subHeight: Long,
  lastSnapshotHash: String,
  blocks: Set[String],
  rewards: Set[RewardTransaction],
  timestamp: Date,
  fee: Long,
  ownerAddress: Option[String],
  stakingAddress: Option[String],
  sizeInKB: Long
)

object CurrencySnapshot {

  implicit val currencySnapshotEncoder: Encoder[CurrencySnapshot] = deriveEncoder

}
