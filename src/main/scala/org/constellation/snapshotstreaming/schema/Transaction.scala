package org.constellation.snapshotstreaming.schema

import enumeratum._
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

case class TransactionEdgeData(amount: Long,
                               lastTxRef: LastTransactionRef,
                               fee: Option[Long],
                               salt: Long)
object TransactionEdgeData {
  implicit val transactionEdgeDataEncoder: Encoder[TransactionEdgeData] =
    deriveEncoder
}

sealed trait EdgeHashType extends EnumEntry

object EdgeHashType extends CirceEnum[EdgeHashType] with Enum[EdgeHashType] {
  case object AddressHash extends EdgeHashType
  case object TransactionDataHash extends EdgeHashType

  val values = findValues
}

case class TypedEdgeHash(hashReference: String,
                         hashType: EdgeHashType,
                         baseHash: Option[String])

object TypedEdgeHash {
  implicit val typedEdgeHashEncoder: Encoder[TypedEdgeHash] = deriveEncoder
}

case class ObservationEdge(parents: Seq[TypedEdgeHash], data: TypedEdgeHash)

object ObservationEdge {
  implicit val observationEdgeEncoder: Encoder[ObservationEdge] = deriveEncoder
}

case class Id(hex: String)

object Id {
  implicit val idEncoder: Encoder[Id] = deriveEncoder
}

case class HashSignature(signature: String, id: Id)

object HashSignature {
  implicit val hashSignatureEncoder: Encoder[HashSignature] = deriveEncoder
}

case class SignatureBatch(hash: String, signatures: Seq[HashSignature])

object SignatureBatch {
  implicit val signatureBatchEncoder: Encoder[SignatureBatch] = deriveEncoder
}

case class SignedObservationEdge(signatureBatch: SignatureBatch)

object SignedObservationEdge {
  implicit val signedObservationEdgeEncoder: Encoder[SignedObservationEdge] =
    deriveEncoder
}

case class TransactionEdge(observationEdge: ObservationEdge,
                           signedObservationEdge: SignedObservationEdge,
                           data: TransactionEdgeData)

object TransactionEdge {
  implicit val transactionEdgeEncoder: Encoder[TransactionEdge] = deriveEncoder
}

case class TransactionOriginal(edge: TransactionEdge,
                               lastTxRef: LastTransactionRef,
                               isDummy: Boolean = false,
                               isTest: Boolean = false)

object TransactionOriginal {
  implicit val transactionOriginalEncoder: Encoder[TransactionOriginal] =
    deriveEncoder
}

case class Transaction(hash: String,
                       amount: Long,
                       receiver: String,
                       sender: String,
                       fee: Long,
                       isDummy: Boolean,
                       lastTransactionRef: LastTransactionRef,
                       snapshotHash: String,
                       checkpointBlock: String,
                       transactionOriginal: TransactionOriginal)

object Transaction {
  implicit val transactionEncoder: Encoder[Transaction] = deriveEncoder
}
