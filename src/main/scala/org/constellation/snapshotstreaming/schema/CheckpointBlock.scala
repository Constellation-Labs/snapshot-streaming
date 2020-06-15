package org.constellation.snapshotstreaming.schema

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

case class CheckpointBlock(hash: String,
                           height: Height,
                           transactions: Seq[String],
                           notifications: Seq[String],
                           observations: Seq[String],
                           children: Long,
                           snapshotHash: String,
                           soeHash: String,
                           parentSOEHashes: Seq[String])

object CheckpointBlock {
  implicit val checkpointEncoder: Encoder[CheckpointBlock] = deriveEncoder
}
