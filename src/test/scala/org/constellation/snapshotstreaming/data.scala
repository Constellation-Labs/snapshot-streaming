package org.constellation.snapshotstreaming

import cats.data.NonEmptyList
import cats.data.NonEmptySet
import cats.syntax.all._
import eu.timepit.refined.auto._
import org.tessellation.syntax.sortedCollection._

import scala.collection.immutable.SortedMap
import scala.collection.immutable.SortedSet
import org.tessellation.schema.ID.Id
import org.tessellation.schema._
import org.tessellation.schema.epoch.EpochProgress
import org.tessellation.schema.height.Height
import org.tessellation.schema.height.SubHeight
import org.tessellation.schema.peer.PeerId
import org.tessellation.schema.transaction.RewardTransaction
import org.tessellation.security.Hashed
import org.tessellation.security.hash.Hash
import org.tessellation.security.hash.ProofsHash
import org.tessellation.security.hex.Hex
import org.tessellation.security.signature.Signed
import org.tessellation.security.signature.signature.Signature
import org.tessellation.security.signature.signature.SignatureProof
import eu.timepit.refined.types.numeric.NonNegLong
import cats.effect.kernel.Sync
import cats.effect.Async
import org.tessellation.currency.schema.currency.CurrencyIncrementalSnapshot
import org.tessellation.currency.schema.currency.CurrencySnapshotInfo
import org.tessellation.currency.schema.feeTransaction.{FeeTransaction, FeeTransactionReference}
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.address.Address
import org.tessellation.schema.balance.Amount
import org.tessellation.schema.balance.Balance
import org.tessellation.schema.transaction.Transaction
import org.tessellation.schema.transaction.TransactionAmount
import org.tessellation.schema.transaction.TransactionFee
import org.tessellation.schema.transaction.TransactionOrdinal
import org.tessellation.schema.transaction.TransactionReference
import org.tessellation.schema.transaction.TransactionSalt
import org.tessellation.security.HasherSelector
import org.tessellation.security.HashSelect
import org.tessellation.security.HashLogic
import org.tessellation.security.JsonHash
import org.tessellation.security.signature.Signed.forAsyncHasher
import org.tessellation.security.Hasher
import org.tessellation.security.SecurityProvider

import java.security.KeyPair

object data {

  val hashSelect = new HashSelect {
    def select(ordinal: SnapshotOrdinal): HashLogic = JsonHash
  }

  def incrementalGlobalSnapshot[F[_]: Sync: HasherSelector](
    ordinal: NonNegLong,
    height: NonNegLong,
    subHeight: NonNegLong,
    lastSnapshot: Hash,
    hash: Hash,
    globalSnapshotInfo: GlobalSnapshotInfo = GlobalSnapshotInfo.empty,
    blocks: SortedSet[BlockAsActiveTip] = SortedSet.empty,
    rewards: SortedSet[RewardTransaction] = SortedSet.empty
  ): F[Hashed[GlobalIncrementalSnapshot]] = {
    implicit val hasher = HasherSelector[F].getCurrent

    globalSnapshotInfo.stateProof(SnapshotOrdinal(ordinal)).map { sp =>
      Hashed(
        Signed(
          GlobalIncrementalSnapshot(
            ordinal = SnapshotOrdinal(ordinal),
            height = Height(height),
            subHeight = SubHeight(subHeight),
            lastSnapshotHash = lastSnapshot,
            blocks = blocks,
            stateChannelSnapshots = SortedMap.empty,
            rewards = rewards,
            epochProgress = EpochProgress.MinValue,
            nextFacilitators = NonEmptyList.of(PeerId(Hex(""))),
            tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
            stateProof = sp,
          ),
          NonEmptySet.one(SignatureProof(Id(Hex("")), Signature(Hex(""))))
        ),
        hash,
        ProofsHash(Hash.empty.value)
      )
    }
  }


  def emptyCurrencySnapshotInfo: CurrencySnapshotInfo =
    CurrencySnapshotInfo(SortedMap.empty, SortedMap.empty, None, None)


  def createBalances(addresses: Address*) =
    addresses.map(address => address -> Balance(1000L)).toMap.toSortedMap

  def applyTransactions(
    balances: SortedMap[Address, Balance],
    txs: List[Signed[Transaction]],
    rewards: List[RewardTransaction],
    feeTxs: List[Signed[FeeTransaction]]
  ): SortedMap[Address, Balance] = {
    val txApplied = txs.foldLeft(balances.view.mapValues(_.value.toLong).toMap) { case (acc, tx) =>
      acc
        .updatedWith(tx.source)(existing => (existing.getOrElse(0L) - tx.amount.value).some)
        .updatedWith(tx.destination)(existing => (existing.getOrElse(0L) + tx.amount.value).some)
    }

    val rewardsApplied = rewards.foldLeft(txApplied) { case (acc, tx) =>
      acc
        .updatedWith(tx.destination)(existing => (existing.getOrElse(0L) + tx.amount.value).some)
    }

    val feeTxsApplied = feeTxs.foldLeft(rewardsApplied) { case (acc, feeTx) =>
      acc
        .updatedWith(feeTx.source)(existing => (existing.getOrElse(0L) - feeTx.amount.value).some)
        .updatedWith(feeTx.destination)(existing => (existing.getOrElse(0L) + feeTx.amount.value).some)
    }

    val nonEmpty = feeTxsApplied.filterNot { case (_, balance) => balance === 0L }

    nonEmpty.view.mapValues(v => Balance(NonNegLong.unsafeFrom(v))).toMap.toSortedMap
  }

  def createRewards(addresses: Address*) =
    addresses.map(address => RewardTransaction(address, TransactionAmount(1000L))).toSortedSet

  def createBlocksWithTransactions[F[_]: Async: KryoSerializer: HasherSelector: SecurityProvider](
    keyToSign: KeyPair,
    transactionsForBlock: NonEmptySet[Signed[Transaction]]*
  ) = {
    implicit val hasher: Hasher[F] = HasherSelector[F].getCurrent
    val parent = BlockReference(Height(4L), ProofsHash("parent"))
    transactionsForBlock
      .traverse(txns =>
        forAsyncHasher[F, Block](Block(NonEmptyList.one(parent), txns), keyToSign)
          .map(BlockAsActiveTip(_, 0L))
      )
      .map(_.toList.toSortedSet)

  }

  def createTxn[F[_]: Async: KryoSerializer: HasherSelector: SecurityProvider](
    src: Address,
    srcKey: KeyPair,
    dst: Address,
    amount: TransactionAmount = TransactionAmount(1L)
  ): F[Signed[Transaction]] = {
    implicit val hasher: Hasher[F] = HasherSelector[F].getCurrent

    forAsyncHasher[F, Transaction](
      Transaction(
        src,
        dst,
        amount,
        TransactionFee.zero,
        TransactionReference.empty,
        TransactionSalt(0L)
      ),
      srcKey
    )
  }

  def createFeeTxn[F[_]: Async: KryoSerializer: HasherSelector: SecurityProvider](
    src: Address,
    srcKey: KeyPair,
    dst: Address
  ): F[Signed[FeeTransaction]] = {
    implicit val hasher: Hasher[F] = HasherSelector[F].getCurrent

    forAsyncHasher[F, FeeTransaction](
      FeeTransaction(
        src,
        dst,
        Amount(1L),
        FeeTransactionReference(TransactionOrdinal(0L), Hash.empty),
        TransactionSalt(0L)
      ),
      srcKey
    )
  }

  def incrementalCurrencySnapshot[F[_]: Sync: HasherSelector](
    ordinal: NonNegLong,
    height: NonNegLong,
    subHeight: NonNegLong,
    lastSnapshot: Hash,
    hash: Hash,
    currencySnapshotInfo: CurrencySnapshotInfo = emptyCurrencySnapshotInfo,
    blocks: SortedSet[BlockAsActiveTip] = SortedSet.empty,
    rewards: SortedSet[RewardTransaction] = SortedSet.empty,
    feeTransactions: Option[SortedSet[Signed[FeeTransaction]]] = None
  ): F[Hashed[CurrencyIncrementalSnapshot]] = {
    implicit val hasher = HasherSelector[F].getCurrent

    currencySnapshotInfo.stateProof(SnapshotOrdinal(ordinal)).map { sp =>
      Hashed(
        Signed(
          CurrencyIncrementalSnapshot(
            ordinal = SnapshotOrdinal(ordinal),
            height = Height(height),
            subHeight = SubHeight(subHeight),
            lastSnapshotHash = lastSnapshot,
            blocks = blocks,
            rewards = rewards,
            tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
            stateProof = sp,
            epochProgress = EpochProgress.MinValue,
            dataApplication = None,
            messages = None,
            feeTransactions = feeTransactions,
          ),
          NonEmptySet.one(SignatureProof(Id(Hex("")), Signature(Hex(""))))
        ),
        hash,
        ProofsHash(Hash.empty.value)
      )
    }
  }

}
