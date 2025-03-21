package org.constellation.snapshotstreaming.schema

import io.circe.Encoder
import io.circe.generic.semiauto._
import org.constellation.snapshotstreaming.schema.AllowSpends.{AllowSpend, AllowSpendExpiration, SpendTransaction}
import org.constellation.snapshotstreaming.schema.TokenLocks.{TokenLock, TokenUnlock}

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
