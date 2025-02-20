package org.constellation.snapshotstreaming.opensearch.schema

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

case class CurrencyData[A](identifier: String, data: A)

object CurrencyData {

  implicit def currencyDataEncoder[A: Encoder]: Encoder[CurrencyData[A]] = deriveEncoder
}
