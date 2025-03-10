package org.constellation.snapshotstreaming.schema.extractors

import org.constellation.snapshotstreaming.schema.schema.MetagraphData
import org.constellation.snapshotstreaming.schema.{CurrencyData, CurrencySnapshot}
import org.constellation.snapshotstreaming.schema.schema.MetagraphData

trait MetagraphExtractor[T] {
  def extractMetagraphIds(value: T): Seq[String]
}

object MetagraphExtractor {

  implicit def currencyDataExtractor[T]: MetagraphExtractor[CurrencyData[T]] = { case CurrencyData(id, _) => Seq(id) }

  implicit val metagraphDataExtractor: MetagraphExtractor[MetagraphData] = data =>
    data.snapshots.flatMap(implicitly[MetagraphExtractor[CurrencyData[CurrencySnapshot]]].extractMetagraphIds)

  def extract[T: MetagraphExtractor](value: T): Seq[String] =
    implicitly[MetagraphExtractor[T]].extractMetagraphIds(value)

}
