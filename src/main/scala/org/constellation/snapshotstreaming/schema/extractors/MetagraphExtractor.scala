package org.constellation.snapshotstreaming.schema.extractors

import org.constellation.snapshotstreaming.schema.schema.MetagraphData
import org.constellation.snapshotstreaming.schema.{CurrencyData, CurrencySnapshot}

trait MetagraphExtractor[T] {
  def extractMetagraphIds(value: T): Set[String]
}

object MetagraphExtractor {

  implicit def currencyDataExtractor[T]: MetagraphExtractor[CurrencyData[T]] = { case CurrencyData(id, _) => Set(id) }

  implicit val metagraphDataExtractor: MetagraphExtractor[MetagraphData] = data =>
    data.allAsIncremental.toSet.flatMap(implicitly[MetagraphExtractor[CurrencyData[CurrencySnapshot]]].extractMetagraphIds)

  def extract[T: MetagraphExtractor](value: T): Set[String] =
    implicitly[MetagraphExtractor[T]].extractMetagraphIds(value)

}
