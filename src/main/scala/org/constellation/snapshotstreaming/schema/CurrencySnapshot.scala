package org.constellation.snapshotstreaming.schema

import io.circe.Encoder
import io.circe.generic.semiauto._

import java.time.LocalDateTime

final case class CurrencySnapshot(
  hash: String,
  ordinal: Long,
  height: Long,
  subHeight: Long,
  lastSnapshotHash: String,
  epochProgress: Long,
  blocks: Set[String],
  rewards: Set[RewardTransaction],
  fee: Option[Long] = None,
  ownerAddress: Option[String] = None,
  stakingAddress: Option[String] = None,
  version: String,
  timestamp: LocalDateTime,
  sizeInKB: Long
)

object CurrencySnapshot {

  implicit val currencySnapshotEncoder: Encoder[CurrencySnapshot] = deriveEncoder

}
