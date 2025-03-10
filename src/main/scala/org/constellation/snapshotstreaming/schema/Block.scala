package org.constellation.snapshotstreaming.schema

import io.circe.Encoder
import io.circe.generic.semiauto._

import java.time.LocalDateTime

final case class Block(
  hash: String,
  height: Long,
  parent: Set[BlockReference],
  transactions: Set[String],
  snapshotHash: String,
  snapshotOrdinal: Long,
  timestamp: LocalDateTime
)

object Block {

  implicit val blockEncoder: Encoder[Block] = deriveEncoder
}

final case class BlockReference(
  hash: String,
  height: Long
)

object BlockReference {
  implicit val blockReferenceEncoder: Encoder[BlockReference] = deriveEncoder
}
