package org.constellation.snapshotstreaming.schema

import io.circe.Encoder
import org.tessellation.security.hash.Hash
import org.tessellation.security.signature.signature

import java.util.Date

object schema {

  case class GlobalData(
    snapshot: Snapshot,
    blocks: Seq[Block],
    txs: Seq[Transaction],
    balances: Seq[AddressBalance],
    proofs: Seq[SignatureProof],
    metagraphSnapshotCount: Int,
  )

  case class MetagraphData(
    globalSnapshotHash: String,
    snapshots: Seq[CurrencyData[CurrencySnapshot]],
    blocks: Seq[CurrencyData[Block]],
    txs: Seq[CurrencyData[Transaction]],
    feeTxs: Seq[CurrencyData[FeeTransaction]],
    balances: Seq[CurrencyData[AddressBalance]],
  )

  def toIncremental(gsHash: Hash, snapshot: CurrencySnapshot): CurrencySnapshot =
    CurrencySnapshot(
      globalSnapshotHash = gsHash.value,
      hash = snapshot.hash,
      ordinal = snapshot.ordinal,
      height = snapshot.height,
      subHeight = snapshot.subHeight,
      lastSnapshotHash = snapshot.lastSnapshotHash,
      epochProgress = snapshot.epochProgress,
      blocks = snapshot.blocks,
      rewards = snapshot.rewards,
      fee = None,
      ownerAddress = None,
      stakingAddress = None,
      timestamp = snapshot.timestamp,
      version = snapshot.version,
      sizeInKB = None
    )

  implicit val dateEncoder: Encoder[Date] =
    Encoder.encodeString.contramap(date => date.toInstant.toString)

  case class SignatureProof(snapshotHash: String, id: String, signature: String)

  object SignatureProof {

    def from(snapshotHash: Hash, sp: signature.SignatureProof): SignatureProof =
      SignatureProof(snapshotHash.value, sp.id.hex.value, sp.signature.value.value)

  }

}
