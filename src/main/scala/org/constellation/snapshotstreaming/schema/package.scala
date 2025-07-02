package org.constellation.snapshotstreaming

import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Greater
import org.constellation.snapshotstreaming.storage.SnapshotWithState
import org.tessellation.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshotInfo, CurrencySnapshotStateProof}
import org.tessellation.ext.kryo.KryoRegistrationId
import org.tessellation.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, GlobalSnapshotStateProof}
import org.tessellation.schema.address.Address
import org.tessellation.schema.currencyMessage.{CurrencyMessage, MessageType}
import org.tessellation.schema.snapshot.SnapshotInfo
import org.tessellation.security.Hashed


package object schema {

  type StreamingKryoRegistrationIdRange = Greater[1000]

  type StreamingKryoRegistrationId = KryoRegistrationId[StreamingKryoRegistrationIdRange]

  implicit val optionAddressOrdering: Ordering[Option[Address]] = Ordering.Option(Address.OrderingInstance)


  val kryoRegistrar: Map[Class[_], StreamingKryoRegistrationId] = Map(
    classOf[SnapshotWithState] -> 1001,
    classOf[Hashed[_]] -> 1002,
    classOf[GlobalSnapshotInfo] -> 1003,
    classOf[CurrencyIncrementalSnapshot] -> 1004,
    classOf[CurrencySnapshotStateProof] -> 1005,
    classOf[CurrencySnapshotInfo] -> 1006,
    classOf[GlobalIncrementalSnapshot] -> 1009,
    classOf[GlobalSnapshotStateProof] -> 1011,
    optionAddressOrdering.getClass -> 1026,
    classOf[CurrencyMessage] -> 1027,
    classOf[CurrencySnapshotStateProof] -> 1030,
    classOf[SnapshotInfo[CurrencySnapshotStateProof]] -> 1031,
    classOf[MessageType] -> 1032,
    MessageType.Owner.getClass -> 1033,
    MessageType.Staking.getClass -> 1034,
  )


}

