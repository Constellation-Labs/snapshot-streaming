package org.constellation.snapshotstreaming.schema

import io.circe.Encoder
import io.constellationnetwork.security.signature.signature.SignatureProof
import AllowSpends.{AllowSpend, TokenLock, TokenUnlock}

import java.util.Date

object schema {

  case class GlobalData(
    snapshot: Snapshot,
    blocks: Seq[Block],
    txs: Seq[Transaction],
    balances: Seq[AddressBalance],
    proofs: Seq[SignatureProof],
    allowSpends: Seq[AllowSpend] = Seq.empty,
    tokenLocks: Seq[TokenLock] = Seq.empty,
    tokenUnlocks: Seq[TokenUnlock] = Seq.empty
  )

  case class MetagraphData(
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

}
