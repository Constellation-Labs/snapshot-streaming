package org.constellation.snapshotstreaming

import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Greater
import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshotInfo, CurrencySnapshotStateProof}
import io.constellationnetwork.ext.kryo.KryoRegistrationId
import io.constellationnetwork.schema.swap.AllowSpendBlock
import io.constellationnetwork.schema.tokenLock.TokenLockBlock
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, GlobalSnapshotStateProof}
import io.constellationnetwork.security.Hashed
import io.constellationnetwork.security.signature.Signed
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
    //classOf[cats.kernel.Order[_]]->1007,
    //classOf[Signed[_]] -> 1008,
    classOf[GlobalIncrementalSnapshot] -> 1009,
    AllowSpendBlock.OrderingInstance.getClass -> 1010,
    classOf[GlobalSnapshotStateProof] -> 1011,
    TokenLockBlock.OrderingInstance.getClass -> 1012,
  )

}
