package org.constellation.snapshotstreaming.opensearch.schema

import java.util.Date

import io.circe.Encoder

object schema {

  implicit val dateEncoder: Encoder[Date] =
    Encoder.encodeString.contramap(date => date.toInstant.toString)

}
