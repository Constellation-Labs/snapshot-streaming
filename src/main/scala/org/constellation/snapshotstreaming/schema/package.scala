package org.constellation.snapshotstreaming

import cats.data.{NonEmptyMap, NonEmptySet}
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Greater
import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshotInfo, CurrencySnapshotStateProof}
import io.constellationnetwork.currency.schema.globalSnapshotSync.GlobalSnapshotSync
import io.constellationnetwork.ext.cats.data.OrderBasedOrdering
import io.constellationnetwork.ext.kryo.KryoRegistrationId
import io.constellationnetwork.kryo.Migration
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.currencyMessage.{CurrencyMessage, MessageType}
import io.constellationnetwork.schema.node.UpdateNodeParameters
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.snapshot.SnapshotInfo
import io.constellationnetwork.schema.swap.{AllowSpend, AllowSpendBlock, AllowSpendReference}
import io.constellationnetwork.schema.tokenLock.{TokenLock, TokenLockBlock, TokenLockReference}
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalIncrementalSnapshotV1, GlobalSnapshotInfo, GlobalSnapshotStateProof, SnapshotOrdinal}
import io.constellationnetwork.security.Hashed
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.SignatureProof
import org.constellation.snapshotstreaming.storage.SnapshotWithState

import scala.collection.immutable.{SortedMap, SortedSet}


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
    AllowSpendBlock.OrderingInstance.getClass -> 1010,
    classOf[GlobalSnapshotStateProof] -> 1011,
    TokenLockBlock.OrderingInstance.getClass -> 1012,
    classOf[AllowSpend] -> 1013,
    AllowSpend.OrderingInstance.getClass -> 1014,
    classOf[AllowSpendBlock] -> 1015,
    AllowSpendBlock.OrderingInstance.getClass -> 1016,
    classOf[TokenLockBlock] -> 1017,
    TokenLockBlock.OrderingInstance.getClass -> 1018,
    classOf[TokenLock] -> 1019,
    TokenLock.OrderingInstance.getClass -> 1020,
    classOf[AllowSpendReference] -> 1021,
    AllowSpendReference.OrderingInstance.getClass -> 1022,
    classOf[TokenLockReference] -> 1023,
    TokenLockReference.OrderingInstance.getClass -> 1024,
    classOf[UpdateNodeParameters] -> 1025,
    optionAddressOrdering.getClass -> 1026,
    classOf[CurrencyMessage] -> 1027,
    classOf[GlobalSnapshotSync] -> 1029,
    classOf[CurrencySnapshotStateProof] -> 1030,
    classOf[SnapshotInfo[CurrencySnapshotStateProof]] -> 1031,
    classOf[MessageType] -> 1032,
    MessageType.Owner.getClass -> 1033,
    MessageType.Staking.getClass -> 1034,
    MessageType.OrderingInstance.getClass -> 1035,
    CurrencyMessage.OrderingInstance.getClass -> 1036,
    PeerId.OrderingInstance.getClass -> 1037,
    GlobalSnapshotSync.OrderingInstance.getClass -> 1038,
    MessageType.OrderingInstance.getClass -> 1039,
  )

  val migrations = List(Migration[GlobalIncrementalSnapshotV1, GlobalIncrementalSnapshot](_.toGlobalIncrementalSnapshot))

}

