package org.constellation.snapshotstreaming

import cats.data.{NonEmptyMap, NonEmptySet}
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Greater
import org.constellation.snapshotstreaming.storage.SnapshotWithState
import org.tessellation.currency.schema.currency.{
  CurrencyIncrementalSnapshot,
  CurrencySnapshotInfo,
  CurrencySnapshotStateProof
}
import org.tessellation.ext.cats.data.OrderBasedOrdering
import org.tessellation.ext.kryo.KryoRegistrationId
import org.tessellation.schema.GlobalSnapshotInfo
import org.tessellation.schema.address.Address
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
    classOf[cats.kernel.Order[_]] -> 1007
  )

//  val migrations = List(Migration[GlobalIncrementalSnapshotV1, GlobalIncrementalSnapshot](_.toGlobalIncrementalSnapshot))

}
