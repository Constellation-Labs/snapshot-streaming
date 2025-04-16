package org.constellation.snapshotstreaming.mapper

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.delegatedStake.{DelegatedStakeRecord, PendingWithdrawal}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.round.RoundId
import io.constellationnetwork.schema.{
  GlobalIncrementalSnapshot,
  GlobalSnapshotInfo,
  SnapshotOrdinal,
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
  DelegatedStakingBalanceChanges,
  DelegatedStakingCreate,
  DelegatedStakingReward,
  DelegatedStakingWithdraw,
  RewardTransaction,
  Snapshot,
  StakingEventCreate,
  StakingEventWithdraw,
  TransactionReference
}

import java.time.LocalDateTime
import scala.collection.immutable.{SortedMap, SortedSet}

abstract class GlobalSnapshotMapper[F[_]: Async] extends SnapshotMapper[F, GlobalIncrementalSnapshot] {

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

      delegatedStakingCreate <- mapDelegatedStakingCreates(
        globalSnapshot.hash,
        snapshotInfo,
        maybePrevSnapshotInfo,
        hasher
      )
      delegatedStakingWithdraw <- mapDelegatedStakingWithdraws(
        globalSnapshot.hash,
        snapshotInfo,
        maybePrevSnapshotInfo,
        hasher
      )

      stakingBalances <- calculateStakingBalances(
        globalSnapshot.hash,
        globalSnapshot.ordinal,
        snapshotInfo,
        maybePrevSnapshotInfo
      )(hasher)

      stakingRewards = mapStakingRewards(
        globalSnapshot
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
      stakingRewards,
      stakingBalances,
      spendTransactions,
      allowSpendExpirations
    )
  }

  private def mapStakingRewards(snapshot: Hashed[GlobalIncrementalSnapshot]) =
    snapshot.delegateRewards.toSeq.flatMap(_.flatMap { case (address, values) =>
      values.toSeq.map { case (peerId, amount) =>
        DelegatedStakingReward(snapshot.hash.value, address.value.value, peerId.value.value, amount.value.value)
      }
    })

  private def flatten[T](bag: Option[SortedMap[Address, List[T]]]) =
    bag.toSeq.flatMap(_.flatMap { case (address, values) => values.map((address, _)) })

  private def calculateStakingBalances(
    gsHash: Hash,
    gsOrdinal: SnapshotOrdinal,
    snapshotInfo: GlobalSnapshotInfo,
    maybePrevSnapshotInfo: Option[GlobalSnapshotInfo]
  )(implicit hasher: Hasher[F]) = {

    val prev = maybePrevSnapshotInfo.toSeq.flatMap(s => flatten(s.activeDelegatedStakes))
    val current = flatten(snapshotInfo.activeDelegatedStakes)

    val newOrUpdatedBalances = current.diff(prev)

    val tokenLockToNewNodeId = newOrUpdatedBalances.map { case (_, dsr) =>
      (dsr.event.tokenLockRef, dsr.event.nodeId)
    }.toMap

    for {
      increasedBalances <- newOrUpdatedBalances.traverse { case (address, dsr) =>
        dsr.event.toHashed.map { hashedDsr =>
          val event = dsr.event.value
          DelegatedStakingBalanceChanges(
            snapshotHash = gsHash.value,
            snapshotOrdinal = gsOrdinal.value.value,
            address = address.value,
            nodeId = event.nodeId.value.value,
            balance = dsr.event.value.amount.value,
            rewards = dsr.rewards.value,
            stakingCreateEvent = StakingEventCreate(hashedDsr.hash.value)
          )
        }
      }

      // find nodes that lost the staking to an update
      dsrOfOldNodes = prev.filter { case (_, dsr) =>
        tokenLockToNewNodeId.get(dsr.event.tokenLockRef).exists(newNodeId => newNodeId =!= dsr.event.nodeId)
      }

      oldNodesZeroedBalances <- dsrOfOldNodes.traverse { case (address, dsr) =>
        dsr.event.toHashed.map { hashedDsr =>
          DelegatedStakingBalanceChanges(
            snapshotHash = gsHash.value,
            snapshotOrdinal = gsOrdinal.value.value,
            address = address.value,
            nodeId = dsr.event.value.nodeId.value.value,
            balance = 0L,
            rewards = 0L,
            stakingCreateEvent = StakingEventCreate(hashedDsr.hash.value)
          )
        }
      }

      previousNodeIdByStakingRefHash <- prev.traverse { case (_, dsr) =>
        dsr.event.toHashed.map(_.hash.value -> dsr.event.nodeId)
      }.map(_.toMap)

      unstakedToZeroBalances <- flatten(snapshotInfo.delegatedStakesWithdrawals).traverse { case (address, dsr) =>
        dsr.event.toHashed.map { hashedDsr =>
          DelegatedStakingBalanceChanges(
            snapshotHash = gsHash.value,
            snapshotOrdinal = gsOrdinal.value.value,
            address = address.value,
            nodeId = previousNodeIdByStakingRefHash(dsr.event.stakeRef.value).value.value,
            balance = 0L,
            rewards = 0L,
            stakingCreateEvent = StakingEventWithdraw(hashedDsr.hash.value)
          )
        }
      }

    } yield increasedBalances ++ oldNodesZeroedBalances ++ unstakedToZeroBalances

  }

  private def mapDelegatedStakingWithdraw(snapshotHash: Hash)(
    pendingWithdrawal: PendingWithdrawal
  )(implicit hasher: Hasher[F]): F[DelegatedStakingWithdraw] =
    pendingWithdrawal.event.toHashed.map { staking =>
      DelegatedStakingWithdraw(
        snapshotHash.value,
        staking.hash.value,
        staking.source.value,
        staking.stakeRef.value,
        pendingWithdrawal.rewards.value,
        pendingWithdrawal.createdAt.value.value
      )
    }

  private def mapDelegatedStakingWithdraws(
    snapshotHash: Hash,
    snapshotInfo: GlobalSnapshotInfo,
    maybePrevSnapshotInfo: Option[GlobalSnapshotInfo],
    hasher: Hasher[F]
  ) = {
    implicit val hs: Hasher[F] = hasher

    val prePendingWithdrawalsStakeRefs =
      maybePrevSnapshotInfo.toSeq.flatMap(s => flatten(s.delegatedStakesWithdrawals).map(_._2.event.stakeRef))

    snapshotInfo.delegatedStakesWithdrawals.toList.flatTraverse(_.toList.flatTraverse { case (_, stakes) =>
      // keep only the new pending withdrawals
      stakes
        .filterNot(dsr => prePendingWithdrawalsStakeRefs.contains(dsr.event.stakeRef))
        .traverse(mapDelegatedStakingWithdraw(snapshotHash))
    })
  }

  private def mapDelegatedStakingCreate(snapshotHash: Hash, prevActiveTokenLocks: Map[Hash, PeerId])(
    dsr: DelegatedStakeRecord
  )(implicit hasher: Hasher[F]): F[DelegatedStakingCreate] =
    dsr.event.toHashed.map { staking =>
      DelegatedStakingCreate(
        snapshotHash.value,
        staking.hash.value,
        staking.ordinal.value,
        staking.source.value,
        staking.nodeId.value.value,
        staking.amount.value,
        staking.fee.value,
        dsr.rewards.value,
        staking.tokenLockRef.value,
        staking.parent.hash.value,
        prevActiveTokenLocks.contains(staking.tokenLockRef)
      )
    }

  private def mapDelegatedStakingCreates(
    snapshotHash: Hash,
    snapshotInfo: GlobalSnapshotInfo,
    maybePrevSnapshotInfo: Option[GlobalSnapshotInfo],
    hasher: Hasher[F]
  ) = {
    implicit val hs: Hasher[F] = hasher
    val prevActiveTokenLocks = maybePrevSnapshotInfo.toSeq
      .flatMap(s =>
        flatten(s.activeDelegatedStakes).map { case (_, dsr) => dsr.event.tokenLockRef -> dsr.event.nodeId }
      )
      .toMap

    snapshotInfo.activeDelegatedStakes.toList.flatTraverse(_.toList.flatTraverse { case (_, stakes) =>
      stakes.filter { dsr => // keep only the new or the updated
        prevActiveTokenLocks.get(dsr.event.tokenLockRef).exists(_ =!= dsr.event.nodeId)
      }
        .traverse(mapDelegatedStakingCreate(snapshotHash, prevActiveTokenLocks))
    })
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

  def make[F[_]: Async](): GlobalSnapshotMapper[F] =
    new GlobalSnapshotMapper[F] {

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
