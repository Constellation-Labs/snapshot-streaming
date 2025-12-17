package org.constellation.snapshotstreaming

import cats.Parallel
import cats.data.NonEmptyList
import cats.data.NonEmptySet
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.constellationnetwork.syntax.sortedCollection._

import scala.collection.immutable.SortedMap
import scala.collection.immutable.SortedSet
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.Height
import io.constellationnetwork.schema.height.SubHeight
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.transaction.RewardTransaction
import io.constellationnetwork.security.Hashed
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hash.ProofsHash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.Signature
import io.constellationnetwork.security.signature.signature.SignatureProof
import eu.timepit.refined.types.numeric.NonNegLong
import cats.effect.kernel.Sync
import cats.effect.Async
import io.constellationnetwork.currency.schema.currency.CurrencyIncrementalSnapshot
import io.constellationnetwork.currency.schema.currency.CurrencySnapshotInfo
import io.constellationnetwork.currency.dataApplication.FeeTransaction
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.transaction.Transaction
import io.constellationnetwork.schema.transaction.TransactionAmount
import io.constellationnetwork.schema.transaction.TransactionFee
import io.constellationnetwork.schema.transaction.TransactionReference
import io.constellationnetwork.schema.transaction.TransactionSalt
import io.constellationnetwork.security.HasherSelector
import io.constellationnetwork.security.HashSelect
import io.constellationnetwork.security.HashLogic
import io.constellationnetwork.security.JsonHash
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.SecurityProvider

import java.security.KeyPair

object data {

  val hashSelect = new HashSelect {
    def select(ordinal: SnapshotOrdinal): HashLogic = JsonHash
  }

  def incrementalGlobalSnapshot[F[_]: Parallel: Async: HasherSelector](
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
            allowSpendBlocks = None,
            tokenLockBlocks = None,
            spendActions = None,
            updateNodeParameters = None,
            artifacts = None,
            activeDelegatedStakes = None,
            delegatedStakesWithdrawals = None,
            delegateRewards = SortedMap.empty[PeerId, Map[Address, Amount]].some,
            activeNodeCollaterals = None,
            nodeCollateralWithdrawals = None,
          ),
          NonEmptySet.one(SignatureProof(Id(Hex("")), Signature(Hex("")))),
        ),
        hash,
        ProofsHash(Hash.empty.value)
      )
    }
  }


  def emptyCurrencySnapshotInfo: CurrencySnapshotInfo =
    CurrencySnapshotInfo(SortedMap.empty, SortedMap.empty, None, None, None, None, None, None, None)


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
        Hash.empty
      ),
      srcKey
    )
  }

  def incrementalCurrencySnapshot[F[_]: Parallel: Async: HasherSelector](
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
            globalSnapshotSyncs= None,
            feeTransactions = feeTransactions,
            artifacts= None,
            allowSpendBlocks = None,
            tokenLockBlocks = None,
            globalSyncView = None
          ),
          NonEmptySet.one(SignatureProof(Id(Hex("")), Signature(Hex(""))))
        ),
        hash,
        ProofsHash(Hash.empty.value)
      )
    }
  }

}
