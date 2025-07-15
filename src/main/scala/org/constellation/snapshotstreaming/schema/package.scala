package org.constellation.snapshotstreaming

import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Greater
import org.constellation.snapshotstreaming.storage.SnapshotWithState
import org.tessellation.ext.kryo.KryoRegistrationId


package object schema {

  type StreamingKryoRegistrationIdRange = Greater[1000]

  type StreamingKryoRegistrationId = KryoRegistrationId[StreamingKryoRegistrationIdRange]

//  implicit val optionAddressOrdering: Ordering[Option[Address]] = Ordering.Option(Address.OrderingInstance)


  val kryoRegistrar: Map[Class[_], StreamingKryoRegistrationId] = Map(
    classOf[SnapshotWithState] -> 1001,
    classOf[CurrencySnapshotV1] -> 1002
  )


}

