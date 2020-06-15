package org.constellation.snapshotstreaming.schema

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

case class LastTransactionRef(prevHash: String, ordinal: Long)

object LastTransactionRef {
  implicit val lastTransactionRefEncoder: Encoder[LastTransactionRef] =
    deriveEncoder
}
