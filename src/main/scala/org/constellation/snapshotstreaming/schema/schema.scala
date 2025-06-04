package org.constellation.snapshotstreaming.schema

import io.circe.Encoder
import io.constellationnetwork.security.signature.signature.SignatureProof
import AllowSpends.{AllowSpend, AllowSpendExpiration, SpendTransaction}
import org.constellation.snapshotstreaming.schema.TokenLocks.{TokenLock, TokenUnlock}

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
                         tokenUnlocks: Seq[TokenUnlock] = Seq.empty,
                         delegatedStakingCreate: Seq[DelegatedStakingCreate],
                         delegatedStakingWithdraw: Seq[DelegatedStakingWithdraw],
                         completedDelegatedStakingWithdrawHashes: Seq[String],
                         delegatedStakingRewards: Seq[DelegatedStakingReward],
                         spendTransactions: Seq[SpendTransaction],
                         allowSpendExpirations: Seq[AllowSpendExpiration]
  )

  case class MetagraphData(
    snapshots: Seq[CurrencyData[CurrencySnapshot]],
    blocks: Seq[CurrencyData[Block]],
    txs: Seq[CurrencyData[Transaction]],
    feeTxs: Seq[CurrencyData[FeeTransaction]],
    balances: Seq[CurrencyData[AddressBalance]],
    allowSpends: Seq[CurrencyData[AllowSpend]] = Seq.empty,
    spendTransactions: Seq[CurrencyData[SpendTransaction]],
    allowSpendExpirations: Seq[CurrencyData[AllowSpendExpiration]],
    tokenLocks: Seq[CurrencyData[TokenLock]] = Seq.empty,
    tokenUnlocks: Seq[CurrencyData[TokenUnlock]] = Seq.empty
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
