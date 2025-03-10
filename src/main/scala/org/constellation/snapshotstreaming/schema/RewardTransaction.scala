package org.constellation.snapshotstreaming.schema

import io.circe.Encoder
import io.circe.generic.semiauto._

final case class RewardTransaction(
  destination: String,
  amount: Long
)

object RewardTransaction {
  implicit val rewardTransactionEncoder: Encoder[RewardTransaction] = deriveEncoder
}
