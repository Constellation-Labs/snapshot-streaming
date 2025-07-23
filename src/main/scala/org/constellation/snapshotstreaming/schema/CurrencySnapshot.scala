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
                                   rewards: Seq[RewardTransaction],
                                   fee: Long,
                                   ownerAddress: Option[String] = None,
                                   stakingAddress: Option[String] = None,
                                   timestamp: LocalDateTime,
                                   version: String,
                                   sizeInKB: Long
)

object CurrencySnapshot {

  implicit val currencySnapshotEncoder: Encoder[CurrencySnapshot] = deriveEncoder

}
