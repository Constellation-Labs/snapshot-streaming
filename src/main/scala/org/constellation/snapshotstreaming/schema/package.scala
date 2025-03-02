package org.constellation.snapshotstreaming

import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Greater
import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshotInfo, CurrencySnapshotStateProof}
import io.constellationnetwork.ext.kryo.KryoRegistrationId
import io.constellationnetwork.schema.GlobalSnapshotInfo
import io.constellationnetwork.security.Hashed
import org.constellation.snapshotstreaming.storage.FileBasedLastGlobalIncrementalSnapshotStorage.SnapshotWithState


package object schema {

  type StreamingKryoRegistrationIdRange = Greater[1000]

  type StreamingKryoRegistrationId = KryoRegistrationId[StreamingKryoRegistrationIdRange]

  val kryoRegistrar: Map[Class[_], StreamingKryoRegistrationId] = Map(
    classOf[SnapshotWithState] -> 1001,
    classOf[Hashed[_]] -> 1002,
    classOf[GlobalSnapshotInfo] -> 1003,
    classOf[CurrencyIncrementalSnapshot] -> 1004,
    classOf[CurrencySnapshotStateProof] -> 1005,
    classOf[CurrencySnapshotInfo] -> 1006,
    classOf[cats.kernel.Order[_]]->1007,
  )

}
