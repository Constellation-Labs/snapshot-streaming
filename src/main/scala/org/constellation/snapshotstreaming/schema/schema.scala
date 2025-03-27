package org.constellation.snapshotstreaming.schema

import io.circe.Encoder
import org.tessellation.security.signature.signature.SignatureProof
import AllowSpends.{AllowSpend, TokenLock, TokenUnlock}
import org.tessellation.sdk.security
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
    allowSpends: Seq[AllowSpend] = Seq.empty,
    tokenLocks: Seq[TokenLock] = Seq.empty,
    tokenUnlocks: Seq[TokenUnlock] = Seq.empty
  )

  case class MetagraphData(
    globalSnapshotHash: String,
    snapshots: Seq[CurrencyData[CurrencySnapshot]],
    blocks: Seq[CurrencyData[Block]],
    txs: Seq[CurrencyData[Transaction]],
    feeTxs: Seq[CurrencyData[FeeTransaction]],
    balances: Seq[CurrencyData[AddressBalance]],
    allowSpends: Seq[CurrencyData[AllowSpend]] = Seq.empty,
    tokenLocks: Seq[CurrencyData[TokenLock]] = Seq.empty,
    tokenUnlocks: Seq[CurrencyData[TokenUnlock]] = Seq.empty
  )

  implicit val dateEncoder: Encoder[Date] =
    Encoder.encodeString.contramap(date => date.toInstant.toString)

  case class SignatureProof(snapshotHash: String, id: String, signature: String)

  object SignatureProof {

    def from(snapshotHash: Hash, sp: signature.SignatureProof): SignatureProof =
      SignatureProof(snapshotHash.value, sp.id.hex.value, sp.signature.value.value)

  }

}
