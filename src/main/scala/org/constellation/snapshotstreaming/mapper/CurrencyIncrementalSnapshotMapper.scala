package org.constellation.snapshotstreaming.mapper

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.constellationnetwork.currency.dataApplication.{FeeTransaction => OriginalFeeTransaction}
import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshotInfo}
import io.constellationnetwork.json.{JsonSerializer, SizeCalculator}
import org.constellation.snapshotstreaming.schema.AllowSpends.AllowSpendExpiration
import org.constellation.snapshotstreaming.schema.TokenLocks.{TokenLock, TokenUnlock}
import io.constellationnetwork.schema.currencyMessage.MessageType
import io.constellationnetwork.schema.round.RoundId
import io.constellationnetwork.schema.{artifact, swap, tokenLock}
import io.constellationnetwork.schema.transaction.{RewardTransaction => OriginalRewardTransaction}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, Hasher}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary
import org.constellation.snapshotstreaming.schema.AllowSpends.{AllowSpend, SpendTransaction}
import org.constellation.snapshotstreaming.schema.{CurrencySnapshot, FeeTransaction, RewardTransaction, TransactionReference}

import java.time.LocalDateTime
import scala.collection.immutable.SortedSet

abstract class CurrencyIncrementalSnapshotMapper[F[_]: Async]
  extends SnapshotMapper[F, CurrencyIncrementalSnapshot, CurrencySnapshotInfo] {

  def mapSnapshot(
                   snapshot: Hashed[CurrencyIncrementalSnapshot],
                   binary: Signed[StateChannelSnapshotBinary],
                   info: CurrencySnapshotInfo,
                   timestamp: LocalDateTime,
                   hasher: Hasher[F]
                 ): F[CurrencySnapshot]

  def mapFeeTransactions(
                          snapshot: Hashed[CurrencyIncrementalSnapshot],
                          timestamp: LocalDateTime,
                          hasher: Hasher[F]
                        ): F[List[FeeTransaction]]

  def mapAllowSpends(snapshot: Hashed[CurrencyIncrementalSnapshot], timestamp: LocalDateTime, hasher: Hasher[F]): F[List[AllowSpend]]

  def mapTokenLocks(snapshot: Hashed[CurrencyIncrementalSnapshot], timestamp: LocalDateTime, hasher: Hasher[F]): F[List[TokenLock]]

  def mapArtifacts(snapshot: Hashed[CurrencyIncrementalSnapshot], hasher: Hasher[F]): F[(List[SpendTransaction], List[TokenUnlock], List[AllowSpendExpiration])]

}

object CurrencyIncrementalSnapshotMapper {

  def make[F[_]: Async: JsonSerializer](): CurrencyIncrementalSnapshotMapper[F] =
    new CurrencyIncrementalSnapshotMapper[F] {

      def fetchRewards(snapshot: CurrencyIncrementalSnapshot): SortedSet[OriginalRewardTransaction] =
        snapshot.rewards

      def extractSnapshotReferredAddresses(snapshot: CurrencyIncrementalSnapshot): SnapshotReferredAddresses = {
        val transactions = snapshot.blocks.flatMap(_.block.transactions.toSortedSet)
        val feeTransactions = snapshot.feeTransactions.getOrElse(SortedSet.empty[Signed[OriginalFeeTransaction]])
        val source = transactions.map(_.source) ++ feeTransactions.map(_.source)
        val destination = transactions.map(_.destination) ++ feeTransactions.map(_.destination)
        SnapshotReferredAddresses(source, destination)
      }

      def mapFeeTransactions(
                              snapshot: Hashed[CurrencyIncrementalSnapshot],
                              timestamp: LocalDateTime,
                              hasher: Hasher[F]
                            ): F[List[FeeTransaction]] = {
        implicit val hs: Hasher[F] = hasher
        snapshot.feeTransactions.toList.flatTraverse(
          _.toList.traverse(mapFeeTransaction(snapshot.hash.value, snapshot.ordinal.value, timestamp))
        )
      }

      def mapSnapshot(
                       snapshot: Hashed[CurrencyIncrementalSnapshot],
                       binary: Signed[StateChannelSnapshotBinary],
                       info: CurrencySnapshotInfo,
                       timestamp: LocalDateTime,
                       hasher: Hasher[F]
                     ): F[CurrencySnapshot] = for {
        blocksHashes <- snapshot.blocks.unsorted.map(_.block).map(hashBlock(_, hasher)).toList.sequence
        sizeInKb <- SizeCalculator.kilobytes(binary)
      } yield CurrencySnapshot(
        hash = snapshot.hash.value,
        ordinal = snapshot.ordinal.value.value,
        height = snapshot.height.value,
        subHeight = snapshot.subHeight.value,
        lastSnapshotHash = snapshot.lastSnapshotHash.value,
        blocks = blocksHashes.toSet,
        rewards = fetchRewards(snapshot).unsorted.map(reward =>
          RewardTransaction(
            reward.destination.value,
            reward.amount.value
          )
        ),
        epochProgress = snapshot.epochProgress.value,
        timestamp = timestamp,
        fee = binary.fee.value,
        stakingAddress = getMessageAddress(MessageType.Staking, info),
        ownerAddress = getMessageAddress(MessageType.Owner, info),
        version = snapshot.version.version,
        sizeInKB = sizeInKb.toLong
      )

      private def mapFeeTransaction(snapshotHash: String, snapshotOrdinal: Long, timestamp: LocalDateTime)(
        feeTransaction: Signed[OriginalFeeTransaction]
      )(implicit hasher: Hasher[F]): F[FeeTransaction] =
        feeTransaction.toHashed.map { feeTx =>
          FeeTransaction(
            feeTx.hash.value,
            feeTx.amount.value,
            feeTx.source.value,
            feeTx.destination.value,
            feeTx.dataUpdateRef.value,
            snapshotHash,
            snapshotOrdinal,
            timestamp
          )
        }

      private def getMessageAddress(messageType: MessageType, info: CurrencySnapshotInfo): Option[String] =
        info.lastMessages.flatMap(_.get(messageType)).map(_.address.value.value)

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

      def mapAllowSpends(snapshot: Hashed[CurrencyIncrementalSnapshot], timestamp: LocalDateTime, hasher: Hasher[F]): F[List[AllowSpend]] = {
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

      def mapTokenLocks(snapshot: Hashed[CurrencyIncrementalSnapshot], timestamp: LocalDateTime, hasher: Hasher[F]): F[List[TokenLock]] = {
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

      def mapExpiration(snapshotHash: Hash, expiry:  artifact.AllowSpendExpiration)
                       (implicit hasher: Hasher[F]): F[AllowSpendExpiration] = hasher.hash(expiry).map {
        hash => AllowSpendExpiration(
          snapshotHash.value,
          hash.value,
          expiry.allowSpendRef.value,
        )
      }


      def mapArtifacts(snapshot: Hashed[CurrencyIncrementalSnapshot], hasher: Hasher[F]): F[(List[SpendTransaction], List[TokenUnlock], List[AllowSpendExpiration])] = {
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
          case exp: artifact.AllowSpendExpiration => mapExpiration(snapshot.hash, exp).map(List(_))
          case _ =>  List.empty[AllowSpendExpiration].pure
        }

        (spendTxs, tokenUnlocks, expirations).tupled
      }


    }

}
