package org.constellation.snapshotstreaming.mapper

import cats.Functor
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.constellationnetwork.currency.schema.currency.CurrencyIncrementalSnapshot
import io.constellationnetwork.schema.round.RoundId
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, artifact, swap, tokenLock, transaction}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, Hasher}
import org.constellation.snapshotstreaming.SnapshotProcessor.GlobalSnapshotWithState
import org.constellation.snapshotstreaming.schema.AllowSpends.{AllowSpend, AllowSpendExpiration, SpendTransaction}
import org.constellation.snapshotstreaming.schema.TokenLocks.{TokenLock, TokenUnlock}
import org.constellation.snapshotstreaming.schema.schema.GlobalData
import org.constellation.snapshotstreaming.schema.{RewardTransaction, Snapshot, TransactionReference}

import java.time.LocalDateTime
import scala.collection.immutable.SortedSet

abstract class GlobalSnapshotMapper[F[_]: Async]
  extends SnapshotMapper[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo] {

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

      allowSpends <- mapAllowSpends(globalSnapshot, timestamp, hasher)
      artifacts <- mapArtifacts(globalSnapshot, hasher)
      (spendTransactions, tokenUnlocks, allowSpendExpirations) = artifacts
      tokenLocks <- mapTokenLocks(globalSnapshot, timestamp, hasher)

    } yield GlobalData(snapshot, blocks, transactions, balances, globalSnapshot.signed.proofs.toSortedSet.toSeq, allowSpends,
      tokenLocks,
      tokenUnlocks,
      spendTransactions,
      allowSpendExpirations)
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

  def mapAllowSpends(snapshot: Hashed[GlobalIncrementalSnapshot], timestamp: LocalDateTime, hasher: Hasher[F]): F[List[AllowSpend]] = {
    implicit val hs: Hasher[F] = hasher
    snapshot.allowSpendBlocks.toList.flatTraverse(
      _.toList.flatTraverse( alb => alb.transactions.toList.traverse(mapAllowSpend(snapshot.hash, alb.roundId)))
    )
  }

  private def mapTokenLock(snapshotHash: Hash, roundId: RoundId)(
    tl: Signed[tokenLock.TokenLock]
  )(implicit hasher: Hasher[F]): F[TokenLock] =
    tl.toHashed.map { tokenLock =>
      TokenLock(
        snapshotHash.value,
        tokenLock.hash.value,
        tokenLock.source.value,
        tokenLock.amount.value,
        tokenLock.unlockEpoch.map(_.value.value),
        tokenLock.ordinal.value,
        roundId.value
      )
    }

  def mapTokenLocks(snapshot: Hashed[GlobalIncrementalSnapshot], timestamp: LocalDateTime, hasher: Hasher[F]): F[List[TokenLock]] = {
    implicit val hs: Hasher[F] = hasher
    snapshot.tokenLockBlocks.toList.flatTraverse(
      _.toList.flatTraverse( tlb => tlb.tokenLocks.toList.traverse(mapTokenLock(snapshot.hash, tlb.roundId)))
    )
  }

  def mapSpendTx(snapshotHash: Hash)(
    spendTx: artifact.SpendTransaction
  )(implicit hasher: Hasher[F]): F[SpendTransaction] = hasher.hash(spendTx).map {
    hash => SpendTransaction(
      hash.value,
      spendTx.source.value,
      spendTx.destination.value,
      spendTx.amount.value,
      spendTx.allowSpendRef.map(_.value),
      snapshotHash.value
    )
  }

  def mapTokenUnlock( tokenUnlock:  artifact.TokenUnlock )
                    (implicit hasher: Hasher[F]): F[TokenUnlock] = hasher.hash(tokenUnlock).map {
    hash => TokenUnlock(
      hash.value,
      tokenUnlock.tokenLockRef.value,
      tokenUnlock.amount.value,
      tokenUnlock.address.value,
    )
  }

  def mapExpiration(expiry:  artifact.AllowSpendExpiration)
                   (implicit hasher: Hasher[F]): F[AllowSpendExpiration] = hasher.hash(expiry).map {
    hash => AllowSpendExpiration(
      hash.value,
      expiry.allowSpendRef.value,
    )
  }


  def mapArtifacts(snapshot: Hashed[GlobalIncrementalSnapshot], hasher: Hasher[F]): F[(List[SpendTransaction], List[TokenUnlock], List[AllowSpendExpiration])] = {
    implicit val hs: Hasher[F] = hasher
    val events = snapshot.artifacts.toList.flatten
    val spendTxs = events.flatTraverse {
      case artifact.SpendAction(spendTransactions) => spendTransactions.toList.traverse(mapSpendTx(snapshot.hash))
      case _ =>  List.empty[SpendTransaction].pure
    }
    val tokenUnlocks = events.flatTraverse {
      case tu: artifact.TokenUnlock => mapTokenUnlock(tu).map(List(_))
      case _ =>  List.empty[TokenUnlock].pure
    }
    val expirations = events.flatTraverse {
      case exp: artifact.AllowSpendExpiration => mapExpiration(exp).map(List(_))
      case _ =>  List.empty[AllowSpendExpiration].pure
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

      def mapSnapshot(snapshot: Hashed[GlobalIncrementalSnapshot], timestamp: LocalDateTime, hasher: Hasher[F]): F[Snapshot] =
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

