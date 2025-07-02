package org.constellation.snapshotstreaming.mapper

import cats.effect.Async
import cats.syntax.all._
import org.constellation.snapshotstreaming.schema.{CurrencySnapshot, FeeTransaction, RewardTransaction}
import org.tessellation.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshotInfo}
import org.tessellation.currency.schema.feeTransaction.{FeeTransaction => OriginalFeeTransaction}
import org.tessellation.json.{JsonSerializer, SizeCalculator}
import org.tessellation.schema.currencyMessage.MessageType
import org.tessellation.schema.transaction.{RewardTransaction => OriginalRewardTransaction}
import org.tessellation.security.signature.Signed
import org.tessellation.security.{Hashed, Hasher}
import org.tessellation.statechannel.StateChannelSnapshotBinary

import java.time.LocalDateTime
import scala.collection.immutable.SortedSet

abstract class CurrencyIncrementalSnapshotMapper[F[_]: Async]
  extends SnapshotMapper[F, CurrencyIncrementalSnapshot] {

  def mapSnapshot(
                   snapshot: Hashed[CurrencyIncrementalSnapshot],
                   binary: Signed[StateChannelSnapshotBinary],
                   timestamp: LocalDateTime,
                   hasher: Hasher[F]
                 ): F[CurrencySnapshot]

  def mapFeeTransactions(
                          snapshot: Hashed[CurrencyIncrementalSnapshot],
                          timestamp: LocalDateTime,
                          hasher: Hasher[F]
                        ): F[List[FeeTransaction]]

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
          _.toList.traverse(mapFeeTransaction(snapshot.hash.value, snapshot.ordinal.value.value, timestamp))
        )
      }

      def mapSnapshot(
                       snapshot: Hashed[CurrencyIncrementalSnapshot],
                       binary: Signed[StateChannelSnapshotBinary],
                       timestamp: LocalDateTime,
                       hasher: Hasher[F]
                     ): F[CurrencySnapshot] = for {
        blocksHashes <- snapshot.blocks.unsorted.map(_.block).map(hashBlock(_, hasher)).toList.sequence
        sizeInKb <- SizeCalculator.kilobytes(binary)
      } yield CurrencySnapshot(
        hash = snapshot.hash.value,
        ordinal = snapshot.ordinal.value.value,
        height = snapshot.height.value.value,
        subHeight = snapshot.subHeight.value.value,
        lastSnapshotHash = snapshot.lastSnapshotHash.value,
        blocks = blocksHashes.toSet,
        rewards = fetchRewards(snapshot).unsorted.map(reward =>
          RewardTransaction(
            reward.destination.value.value,
            reward.amount.value.value
          )
        ),
        epochProgress = snapshot.epochProgress.value.value,
        timestamp = timestamp,
        fee = binary.fee.value.value,
        stakingAddress = getMessageAddress(MessageType.Staking, snapshot),
        ownerAddress = getMessageAddress(MessageType.Owner, snapshot),
        version = snapshot.version.version.value,
        sizeInKB = sizeInKb.value
      )

      private def mapFeeTransaction(snapshotHash: String, snapshotOrdinal: Long, timestamp: LocalDateTime)(
        feeTransaction: Signed[OriginalFeeTransaction]
      )(implicit hasher: Hasher[F]): F[FeeTransaction] =
        feeTransaction.toHashed.map { feeTx =>
          FeeTransaction(
            feeTx.hash.value,
            feeTx.amount.value.value,
            feeTx.source.value.value,
            feeTx.destination.value.value,
            null,
            snapshotHash,
            snapshotOrdinal,
            timestamp
          )
        }

      private def getMessageAddress(messageType: MessageType, info: Hashed[CurrencyIncrementalSnapshot]): Option[String] =
        info.messages.toSeq.flatMap(_.filter(_.messageType === messageType)).map(_.address.value.value).lastOption


    }

}
