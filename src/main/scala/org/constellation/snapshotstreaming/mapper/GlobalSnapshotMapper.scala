package org.constellation.snapshotstreaming.mapper

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.constellationnetwork.node.shared.config.types.SharedConfig
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.delegatedStake.{
  DelegatedStakeRecord,
  PendingDelegatedStakeWithdrawal,
  UpdateDelegatedStake
}
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
      delegatedStakingWithdraw <- mapDelegatedStakingWithdraws(
        globalSnapshot.hash,
        snapshotInfo,
        maybePrevSnapshotInfo,
        hasher
      )
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
      stakingRewards,
      spendTransactions,
      allowSpendExpirations
    )
  }

  private def mapStakingRewards(
    snapshot: Hashed[GlobalIncrementalSnapshot],
    activeDelegatedStakes: List[(DelegatedStakeRecord, Hashed[UpdateDelegatedStake.Create])]
  ) = {
    val activeStakeRefs = activeDelegatedStakes.map { case (_, createStake) =>
      (createStake.nodeId, createStake.source) -> createStake.hash
    }.toMap
    println(snapshot.delegateRewards)
    snapshot.delegateRewards.toSeq.flatMap(_.flatMap { case (peerId, values) =>
      values.toSeq.flatMap { case (address, amount) =>
        activeStakeRefs.get((peerId, address)).map( stakeHash =>
        DelegatedStakingReward(
          snapshot.hash.value,
          stakeHash.value,
          address.value.value,
          peerId.value.value,
          amount.value.value
        )).orElse { if (amount.value.value >0) println(s"Non zero reward for inactive stake ${(address, peerId, amount)}"); None }
      }
    })
  }

  private def flatten[T](bag: Option[SortedMap[Address, List[T]]]) =
    bag.toSeq.flatMap(_.flatMap { case (address, values) => values.map((address, _)) })

  def mapDelegatedStakingWithdraw(snapshotHash: Hash, isCompleted: Boolean)(
    pendingWithdrawal: PendingDelegatedStakeWithdrawal
  )(implicit hasher: Hasher[F]): F[DelegatedStakingWithdraw] =
    pendingWithdrawal.event.toHashed.map { staking =>
      DelegatedStakingWithdraw(
        snapshotHash.value,
        staking.hash.value,
        staking.source.value,
        staking.hash.value,
        pendingWithdrawal.rewards.value,
        pendingWithdrawal.createdAt.value.value,
        (pendingWithdrawal.createdAt |+| sharedCfg.delegatedStaking.withdrawalTimeLimit(
          sharedCfg.environment
        )).value.value,
        isCompleted
      )
    }

  def mapDelegatedStakingWithdraws(
    snapshotHash: Hash,
    snapshotInfo: GlobalSnapshotInfo,
    maybePrevSnapshotInfo: Option[GlobalSnapshotInfo],
    hasher: Hasher[F]
  ): F[List[DelegatedStakingWithdraw]] = {
    implicit val hs: Hasher[F] = hasher

    val prePendingWithdrawalsStakeRefs =
      maybePrevSnapshotInfo.toSeq.flatMap(s => flatten(s.delegatedStakesWithdrawals).map(_._2.event))

    val currentPendingWithdrawalsStakeRefs =
      flatten(snapshotInfo.delegatedStakesWithdrawals).map(_._2.event)

    for {
      newPending <- snapshotInfo.delegatedStakesWithdrawals.toList.flatTraverse(_.toList.flatTraverse {
        case (_, stakes) =>
          stakes
            .filterNot(dsr => prePendingWithdrawalsStakeRefs.contains(dsr.event))
            .traverse(mapDelegatedStakingWithdraw(snapshotHash, isCompleted = false))

      })
      completed <- maybePrevSnapshotInfo.toList.flatTraverse(
        _.delegatedStakesWithdrawals.toList.flatTraverse(_.toList.flatTraverse { case (_, stakes) =>
          stakes
            .filterNot(dsr => currentPendingWithdrawalsStakeRefs.contains(dsr.event))
            .traverse(mapDelegatedStakingWithdraw(snapshotHash, isCompleted = true))
        })
      )
    } yield (newPending ++ completed)
  }

  private def mapDelegatedStakingCreate(snapshotHash: Hash, prevActiveTokenLocks: Map[Hash, Hash])(
    dsr: DelegatedStakeRecord,
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
        prevActiveTokenLocks.get(staking.tokenLockRef).map(_.value)
      )
    }

  def activeHashedDelegatedStakes(
    snapshotInfo: GlobalSnapshotInfo
  )(implicit hs: Hasher[F]): F[List[(DelegatedStakeRecord, Hashed[UpdateDelegatedStake.Create])]] =
    snapshotInfo.activeDelegatedStakes.toList.flatTraverse(_.toList.flatTraverse { case (_, stakes) =>
      stakes.traverse(dsr => dsr.event.toHashed.map(ev => (dsr, ev)))
    })

  def mapDelegatedStakingCreates(
    snapshotHash: Hash,
    activeDelegatedStakes: List[(DelegatedStakeRecord, Hashed[UpdateDelegatedStake.Create])],
    maybePrevSnapshotInfo: Option[GlobalSnapshotInfo],
    hasher: Hasher[F]
  ): F[List[DelegatedStakingCreate]] = {
    implicit val hs: Hasher[F] = hasher
    for {
      prevActiveTokenLocks <- maybePrevSnapshotInfo.toSeq
        .flatTraverse(s =>
          flatten(s.activeDelegatedStakes).traverse { case (_, dsr) =>
            dsr.event.toHashed.map(hashed => dsr.event.tokenLockRef -> hashed.hash)
          }
        )
        .map(_.toMap)

      result <- activeDelegatedStakes.mapFilter { case (dsr, ev) => // keep only the new or the updated
        prevActiveTokenLocks.get(dsr.event.tokenLockRef) match {
          case None => Some((dsr, None)) // new stake
          case Some(oldStakeHash) =>
            if (oldStakeHash == ev.hash)
              None // active staking already included
            else
              Some(dsr, Some(ev.hash)) // update staking
        }
      }.traverse { case (dsr, prevRef) =>
        mapDelegatedStakingCreate(snapshotHash, prevActiveTokenLocks)(dsr)
      }
    } yield result
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
