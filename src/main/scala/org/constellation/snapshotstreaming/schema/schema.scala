package org.constellation.snapshotstreaming.schema

import io.circe.Encoder
import org.tessellation.security.signature.signature.SignatureProof

import java.util.Date

object schema {

  case class GlobalData(
                         snapshot: Snapshot,
                         blocks: Seq[Block],
                         txs: Seq[Transaction],
                         proofs: Seq[SignatureProof],
  )

  case class MetagraphData(
    snapshots: Seq[CurrencyData[CurrencySnapshot]],
    blocks: Seq[CurrencyData[Block]],
    txs: Seq[CurrencyData[Transaction]],
    feeTxs: Seq[CurrencyData[FeeTransaction]],
  )

  def toIncremental(snapshot: Snapshot): CurrencySnapshot =
    CurrencySnapshot(
      hash = snapshot.hash,
      ordinal = snapshot.ordinal,
      height = snapshot.height,
      subHeight = snapshot.subHeight,
      lastSnapshotHash = snapshot.lastSnapshotHash,
      epochProgress = snapshot.epochProgress,
      blocks = snapshot.blocks,
      rewards = snapshot.rewards,
      fee = 0,
      ownerAddress = None,
      stakingAddress = None,
      timestamp = snapshot.timestamp,
      version = snapshot.version,
      sizeInKB = 0
    )

  implicit val dateEncoder: Encoder[Date] =
    Encoder.encodeString.contramap(date => date.toInstant.toString)

}
