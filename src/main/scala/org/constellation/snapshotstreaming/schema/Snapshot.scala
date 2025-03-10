package org.constellation.snapshotstreaming.schema

import io.circe.Encoder
import io.circe.generic.semiauto._

import java.time.LocalDateTime

final case class Snapshot(
  hash: String,
  ordinal: Long,
  height: Long,
  subHeight: Long,
  lastSnapshotHash: String,
  epochProgress: Long,
  blocks: Set[String],
  rewards: Set[RewardTransaction],
  version: String,
  timestamp: LocalDateTime
)

object Snapshot {

  implicit val snapshotEncoder: Encoder[Snapshot] = deriveEncoder

}
