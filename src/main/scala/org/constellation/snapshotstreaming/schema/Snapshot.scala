package org.constellation.snapshotstreaming.schema

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

case class Snapshot(hash: String, height: Long, checkpointBlocks: Seq[String])

object Snapshot {
  implicit val snapshotEncoder: Encoder[Snapshot] = deriveEncoder
}
