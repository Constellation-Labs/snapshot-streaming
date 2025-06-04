package org.constellation.snapshotstreaming.mapper

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.constellationnetwork.node.shared.config.types.SharedConfig
import io.constellationnetwork.schema.delegatedStake.{
  DelegatedStakeRecord,
  PendingDelegatedStakeWithdrawal,
  UpdateDelegatedStake
}
import io.constellationnetwork.schema.round.RoundId
import io.constellationnetwork.schema.{
  GlobalIncrementalSnapshot,
  GlobalSnapshotInfo,
  artifact,
  swap,
  tokenLock,
  transaction
}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, Hasher}
import org.constellation.snapshotstreaming.SnapshotProcessor.GlobalSnapshotWithState
import org.constellation.snapshotstreaming.schema.AllowSpends.{AllowSpend, AllowSpendExpiration, SpendTransaction}
import org.constellation.snapshotstreaming.schema.TokenLocks.{TokenLock, TokenUnlock}
import org.constellation.snapshotstreaming.schema.schema.GlobalData
import org.constellation.snapshotstreaming.schema.{
  DelegatedStakingCreate,
  DelegatedStakingReward,
  DelegatedStakingWithdraw,
  RewardTransaction,
  Snapshot,
  TransactionReference
}

import java.time.LocalDateTime
import scala.collection.immutable.{SortedMap, SortedSet}

abstract class GlobalSnapshotMapper[F[_]: Async] extends SnapshotMapper[F, GlobalIncrementalSnapshot] {

  val sharedCfg: SharedConfig

  def mapSnapshot(snapshot: Hashed[GlobalIncrementalSnapshot], timestamp: LocalDateTime, hasher: Hasher[F]): F[Snapshot]

  def mapGlobalSnapshot(
    globalSnapshotWithState: GlobalSnapshotWithState,
    timestamp: LocalDateTime,
    txHasher: Hasher[F],
    hasher: Hasher[F]
  ): F[GlobalData] = {
    val GlobalSnapshotWithState(globalSnapshot, maybePrevSnapshotInfo, snapshotInfo, _, ts) =
      globalSnapshotWithState
    for {
      snapshot <- mapSnapshot(globalSnapshot, timestamp, hasher)
      blocks <- mapBlocks(globalSnapshot, timestamp, txHasher, hasher)
      transactions <- mapTransactions(globalSnapshot, timestamp, txHasher, hasher)
      filteredBalances = balanceDiff(
        globalSnapshot.signed.value,
        maybePrevSnapshotInfo.map(prev => prev.balances),
        snapshotInfo
      )
      balances = mapBalances(globalSnapshot, filteredBalances, timestamp)

      allowSpends <- mapAllowSpends(globalSnapshot, hasher)
      artifacts <- mapArtifacts(globalSnapshot, hasher)
      (spendTransactions, tokenUnlocks, allowSpendExpirations) = artifacts
      tokenLocks <- mapTokenLocks(globalSnapshot, hasher)

      activeHashedDelegatedStakes <- activeHashedDelegatedStakes(snapshotInfo)(hasher)
      delegatedStakingCreate <- mapDelegatedStakingCreates(
        globalSnapshot.hash,
        activeHashedDelegatedStakes,
        maybePrevSnapshotInfo,
        hasher
      )
      dsWithdrawsWithUpdates <- mapDelegatedStakingWithdrawals(
        globalSnapshot.hash,
        snapshotInfo,
        maybePrevSnapshotInfo,
        hasher
      )
      (delegatedStakingWithdraw, completedDelegatedStakingWithdrawHashes) = dsWithdrawsWithUpdates
      stakingRewards = mapStakingRewards(
        globalSnapshot,
        activeHashedDelegatedStakes
      )
    } yield GlobalData(
      snapshot,
      blocks,
      transactions,
      balances,
      globalSnapshot.signed.proofs.toSortedSet.toSeq,
      allowSpends,
      tokenLocks,
      tokenUnlocks,
      delegatedStakingCreate,
      delegatedStakingWithdraw,
      completedDelegatedStakingWithdrawHashes,
      stakingRewards,
      spendTransactions,
      allowSpendExpirations
    )
  }

  private def flatten[K, T](bag: Option[SortedMap[K, Iterable[T]]]) = bag.toSeq.flatMap(_.toSeq.flatMap {
    case (k, values) => values.toSeq.map((k, _))
  })

  private def mapStakingRewards(
    snapshot: Hashed[GlobalIncrementalSnapshot],
    activeDelegatedStakes: Seq[(DelegatedStakeRecord, Hashed[UpdateDelegatedStake.Create])]
  ) = {
    val activeStakeRefs = activeDelegatedStakes.map { case (_, createStake) =>
      (createStake.nodeId, createStake.source) -> createStake.hash
    }.toMap
    flatten(snapshot.delegateRewards).flatMap { case (peerId, (address, amount)) =>
      activeStakeRefs
        .get((peerId, address))
        .map(stakeHash =>
          DelegatedStakingReward(
            snapshot.hash.value,
            stakeHash.value,
            address.value.value,
            peerId.value.value,
            amount.value.value
          )
        )
        .orElse {
          if (amount.value.value > 0) println(s"Non zero reward for inactive stake ${(address, peerId, amount)}")
          None
        }
    }
  }

  def delegatedStakingWithdrawHash(pendingWithdrawal: PendingDelegatedStakeWithdrawal)
                                  (implicit hasher: Hasher[F]): F[Hash] =
    pendingWithdrawal.event.toHashed.map(_.hash)

  def mapDelegatedStakingWithdraw(snapshotHash: Hash, isCompleted: Boolean)(
    pendingWithdrawal: PendingDelegatedStakeWithdrawal
  )(implicit hasher: Hasher[F]): F[DelegatedStakingWithdraw] =
    delegatedStakingWithdrawHash(pendingWithdrawal).map { dsHash =>
      DelegatedStakingWithdraw(
        snapshotHash.value,
        dsHash.value,
        pendingWithdrawal.event.source.value,
        dsHash.value,
        pendingWithdrawal.rewards.value,
        pendingWithdrawal.createdAt.value.value,
        (pendingWithdrawal.createdAt |+| sharedCfg.delegatedStaking.withdrawalTimeLimit(
          sharedCfg.environment
        )).value.value,
        isCompleted
      )
    }

  def mapDelegatedStakingWithdrawals(
    snapshotHash: Hash,
    snapshotInfo: GlobalSnapshotInfo,
    maybePrevSnapshotInfo: Option[GlobalSnapshotInfo],
    hasher: Hasher[F]
  ): F[(Seq[DelegatedStakingWithdraw], Seq[String])] = {
    implicit val hs: Hasher[F] = hasher

    val prePendingWithdrawals =
      maybePrevSnapshotInfo.toSeq.flatMap(s => flatten(s.delegatedStakesWithdrawals).map(_._2))

    val currentPendingWithdrawals =
      flatten(snapshotInfo.delegatedStakesWithdrawals).map(_._2)

    val prePendingWithdrawalsStakeRefsSet =
      prePendingWithdrawals.map(_.event).toSet

    val currentPendingWithdrawalsStakeRefsSet =
      currentPendingWithdrawals.map(_.event).toSet

    for {
      newPending <- currentPendingWithdrawals
        .filterNot(dsr => prePendingWithdrawalsStakeRefsSet.contains(dsr.event))
        .traverse(mapDelegatedStakingWithdraw(snapshotHash, isCompleted = false))
      completed <- prePendingWithdrawals
        .filterNot(dsr => currentPendingWithdrawalsStakeRefsSet.contains(dsr.event))
        .traverse(delegatedStakingWithdrawHash(_).map(_.value))
    } yield (newPending, completed)
  }

  private def mapDelegatedStakingCreate(snapshotHash: Hash, fromHash: Option[Hash])(
    dsr: DelegatedStakeRecord,
    ev: Hashed[UpdateDelegatedStake.Create]
  ): DelegatedStakingCreate =
    DelegatedStakingCreate(
      snapshotHash.value,
      ev.hash.value,
      dsr.createdAt.value,
      dsr.event.source.value,
      dsr.event.nodeId.value.value,
      dsr.event.amount.value,
      dsr.event.fee.value,
      dsr.rewards.value,
      dsr.event.tokenLockRef.value,
      dsr.event.parent.hash.value,
      fromHash.map(_.value)
    )

  def activeHashedDelegatedStakes(
    snapshotInfo: GlobalSnapshotInfo
  )(implicit hs: Hasher[F]): F[Seq[(DelegatedStakeRecord, Hashed[UpdateDelegatedStake.Create])]] =
    flatten(snapshotInfo.activeDelegatedStakes).traverse { case (_, stake) =>
      stake.event.toHashed.map(ev => (stake, ev))
    }

  def mapDelegatedStakingCreates(
    snapshotHash: Hash,
    activeDelegatedStakes: Seq[(DelegatedStakeRecord, Hashed[UpdateDelegatedStake.Create])],
    maybePrevSnapshotInfo: Option[GlobalSnapshotInfo],
    hasher: Hasher[F]
  ): F[Seq[DelegatedStakingCreate]] = {
    implicit val hs: Hasher[F] = hasher
    maybePrevSnapshotInfo.toSeq
      .flatTraverse(s =>
        flatten(s.activeDelegatedStakes).traverse { case (_, dsr) =>
          dsr.event.toHashed.map(hashed => dsr.event.tokenLockRef -> hashed.hash)
        }
      )
      .map { prevActiveTokenLocks =>
        val prevActiveTokenLocksMap = prevActiveTokenLocks.toMap
        activeDelegatedStakes.mapFilter { case (dsr, ev) => // keep only the new or the updated
          prevActiveTokenLocksMap.get(dsr.event.tokenLockRef) match {
            case None => Some((dsr, ev, None)) // new stake
            case Some(oldStakeHash) =>
              if (oldStakeHash == ev.hash)
                None // active staking already included
              else {
                Some(dsr, ev, Some(oldStakeHash))
              } // update staking
          }
        }.map { case (dsr, ev, oFromStake) =>
          mapDelegatedStakingCreate(snapshotHash, oFromStake)(dsr, ev)
        }
      }
  }

  def mapAllowSpend(snapshotHash: Hash, roundId: RoundId)(
    allowSpend: Signed[swap.AllowSpend]
  )(implicit hasher: Hasher[F]): F[AllowSpend] =
    allowSpend.toHashed.map { allowSpend =>
      AllowSpend(
        allowSpend.hash.value,
        allowSpend.source.value,
        allowSpend.destination.value,
        allowSpend.amount.value,
        allowSpend.fee.value,
        TransactionReference(allowSpend.parent.hash.value, allowSpend.parent.ordinal.value),
        allowSpend.lastValidEpochProgress.value,
        roundId.value,
        allowSpend.ordinal.value,
        allowSpend.approvers.map(_.value),
        snapshotHash.value
      )
    }

  def mapAllowSpends(snapshot: Hashed[GlobalIncrementalSnapshot], hasher: Hasher[F]): F[List[AllowSpend]] = {
    implicit val hs: Hasher[F] = hasher
    snapshot.allowSpendBlocks.toList.flatTraverse(
      _.toList.flatTraverse(alb => alb.transactions.toList.traverse(mapAllowSpend(snapshot.hash, alb.roundId)))
    )
  }

  private def mapTokenLock(snapshotHash: Hash, roundId: RoundId, hasher: Hasher[F])(
    tl: Signed[tokenLock.TokenLock]
  ): F[TokenLock] = {
    implicit val hs: Hasher[F] = hasher
    tl.toHashed.map { tokenLock =>
      TokenLock(
        snapshotHash.value,
        tokenLock.hash.value,
        tokenLock.source.value,
        tokenLock.amount.value,
        tokenLock.unlockEpoch.map(_.value.value),
        tokenLock.ordinal.value,
        roundId.value,
        tokenLock.parent.hash.value
      )
    }
  }

  def mapTokenLocks(snapshot: Hashed[GlobalIncrementalSnapshot], hasher: Hasher[F]): F[List[TokenLock]] =
    snapshot.tokenLockBlocks.toList.flatTraverse(
      _.toList.flatTraverse(tlb => tlb.tokenLocks.toList.traverse(mapTokenLock(snapshot.hash, tlb.roundId, hasher)))
    )

  def mapSpendTx(snapshotHash: Hash)(
    spendTx: artifact.SpendTransaction
  )(implicit hasher: Hasher[F]): F[SpendTransaction] = hasher.hash(spendTx).map { hash =>
    SpendTransaction(
      hash.value,
      spendTx.source.value,
      spendTx.destination.value,
      spendTx.amount.value,
      spendTx.allowSpendRef.map(_.value),
      snapshotHash.value
    )
  }

  def mapTokenUnlock(snapshotHash: Hash, tokenUnlock: artifact.TokenUnlock)(implicit
    hasher: Hasher[F]
  ): F[TokenUnlock] = hasher.hash(tokenUnlock).map { hash =>
    TokenUnlock(
      snapshotHash.value,
      hash.value,
      tokenUnlock.tokenLockRef.value,
      tokenUnlock.amount.value,
      tokenUnlock.source.value
    )
  }

  def mapExpiration(snapshotHash: Hash, expiry: artifact.AllowSpendExpiration)(implicit
    hasher: Hasher[F]
  ): F[AllowSpendExpiration] = hasher.hash(expiry).map { hash =>
    AllowSpendExpiration(
      snapshotHash.value,
      hash.value,
      expiry.allowSpendRef.value
    )
  }

  def mapArtifacts(
    snapshot: Hashed[GlobalIncrementalSnapshot],
    hasher: Hasher[F]
  ): F[(List[SpendTransaction], List[TokenUnlock], List[AllowSpendExpiration])] = {
    implicit val hs: Hasher[F] = hasher
    val events = snapshot.artifacts.toList.flatten
    val spendTxs = events.flatTraverse {
      case artifact.SpendAction(spendTransactions) => spendTransactions.toList.traverse(mapSpendTx(snapshot.hash))
      case _                                       => List.empty[SpendTransaction].pure
    }
    val tokenUnlocks = events.flatTraverse {
      case tu: artifact.TokenUnlock => mapTokenUnlock(snapshot.hash, tu).map(List(_))
      case _                        => List.empty[TokenUnlock].pure
    }
    val expirations = events.flatTraverse {
      case exp: artifact.AllowSpendExpiration => mapExpiration(snapshot.hash, exp).map(List(_))
      case _                                  => List.empty[AllowSpendExpiration].pure
    }

    (spendTxs, tokenUnlocks, expirations).tupled
  }

}

object GlobalSnapshotMapper {

  def make[F[_]: Async](cfg: SharedConfig): GlobalSnapshotMapper[F] =
    new GlobalSnapshotMapper[F] {

      val sharedCfg = cfg

      def fetchRewards(snapshot: GlobalIncrementalSnapshot): SortedSet[transaction.RewardTransaction] =
        snapshot.rewards

      def extractSnapshotReferredAddresses(snapshot: GlobalIncrementalSnapshot): SnapshotReferredAddresses = {
        val transactions = snapshot.blocks.flatMap(_.block.transactions.toSortedSet)
        val source = transactions.map(_.source)
        val destination = transactions.map(_.destination)
        SnapshotReferredAddresses(source, destination)
      }

      def mapSnapshot(
        snapshot: Hashed[GlobalIncrementalSnapshot],
        timestamp: LocalDateTime,
        hasher: Hasher[F]
      ): F[Snapshot] =
        snapshot.blocks.unsorted.map(_.block).map(hashBlock(_, hasher)).toList.sequence.map { blocksHashes =>
          Snapshot(
            hash = snapshot.hash.value,
            ordinal = snapshot.ordinal.value.value,
            height = snapshot.height.value,
            subHeight = snapshot.subHeight.value,
            lastSnapshotHash = snapshot.lastSnapshotHash.value,
            epochProgress = snapshot.epochProgress.value,
            blocks = blocksHashes.toSet,
            rewards = fetchRewards(snapshot).unsorted.map(reward =>
              RewardTransaction(
                reward.destination.value,
                reward.amount.value
              )
            ),
            version = snapshot.version.version,
            timestamp = timestamp
          )
        }

    }

}
