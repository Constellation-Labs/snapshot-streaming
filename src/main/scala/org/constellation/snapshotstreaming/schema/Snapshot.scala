package org.constellation.snapshotstreaming.schema

import java.util.Date

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

case class Snapshot(hash: String,
                    height: Long,
                    checkpointBlocks: Seq[String],
                    timestamp: Date)

object Snapshot {
  implicit val snapshotEncoder: Encoder[Snapshot] = deriveEncoder
  implicit val dateEncoder: Encoder[Date] =
    Encoder.encodeString.contramap(date => date.toInstant.toString)
}
