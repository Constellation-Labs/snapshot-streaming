package org.constellation.snapshotstreaming

import java.security.KeyPair

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.Async
import cats.effect.kernel.Sync
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.dataApplication.FeeTransaction
import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshotInfo}
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.SpendAction
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.node.UpdateNodeParameters
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.swap.AllowSpendBlock
import io.constellationnetwork.schema.transaction._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.{Hash, ProofsHash}
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.syntax.sortedCollection._

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong

object data {

  val hashSelect = new HashSelect {
    def select(ordinal: SnapshotOrdinal): HashLogic = JsonHash
  }

  def globalSnapshot(
    ordinal: NonNegLong,
    height: NonNegLong,
    subHeight: NonNegLong,
    lastSnapshot: Hash,
    hash: Hash
  ): Hashed[GlobalSnapshot] =
    Hashed(
      Signed(
        GlobalSnapshot(
          ordinal = SnapshotOrdinal(ordinal),
          height = Height(height),
          subHeight = SubHeight(subHeight),
          lastSnapshotHash = lastSnapshot,
          blocks = SortedSet.empty,
          stateChannelSnapshots = SortedMap.empty,
          rewards = SortedSet.empty,
          epochProgress = EpochProgress.MinValue,
          nextFacilitators = NonEmptyList.of(PeerId(Hex(""))),
          info = GlobalSnapshotInfoV1(SortedMap.empty, SortedMap.empty, SortedMap.empty),
          tips = SnapshotTips(SortedSet.empty, SortedSet.empty)
        ),
        NonEmptySet.one(SignatureProof(Id(Hex("")), Signature(Hex(""))))
      ),
      hash,
      ProofsHash(Hash.empty.value)
    )

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
            SortedSet.empty[Signed[AllowSpendBlock]].some,
            SortedMap.empty[Address, List[SpendAction]].some,
            SortedMap.empty[Id, Signed[UpdateNodeParameters]].some
          ),
          NonEmptySet.one(SignatureProof(Id(Hex("")), Signature(Hex(""))))
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
            feeTransactions = feeTransactions,
            dataApplication = None,
            globalSnapshotSyncs = None,
            messages = None,
            artifacts = None,
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
