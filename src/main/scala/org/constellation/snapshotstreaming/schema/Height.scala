package org.constellation.snapshotstreaming.schema

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

case class Height(min: Long, max: Long)

object Height {
  implicit val heightEncoder: Encoder[Height] = deriveEncoder
}
