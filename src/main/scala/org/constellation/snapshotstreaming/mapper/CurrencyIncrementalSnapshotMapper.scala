package org.constellation.snapshotstreaming.mapper

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import org.tessellation.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshotInfo}
import org.tessellation.json.{JsonSerializer, SizeCalculator}
import org.tessellation.schema.currencyMessage.MessageType
import org.tessellation.schema.transaction.{RewardTransaction => OriginalRewardTransaction}
import org.tessellation.security.signature.Signed
import org.tessellation.security.{Hashed, Hasher}
import org.tessellation.statechannel.StateChannelSnapshotBinary
import org.constellation.snapshotstreaming.schema.{
  CurrencySnapshot,
  FeeTransaction,
  FeeTransactionReference,
  RewardTransaction
}
import org.tessellation.currency.schema.feeTransaction.{FeeTransaction => OriginalFeeTransaction}
import org.tessellation.currency.schema.feeTransaction.{FeeTransactionReference => OriginalFeeTransactionReference}
import org.tessellation.security.hash.Hash

import java.time.LocalDateTime
import scala.collection.immutable.SortedSet

abstract class CurrencyIncrementalSnapshotMapper[F[_]: Async]
    extends SnapshotMapper[F, CurrencyIncrementalSnapshot] {

  def mapSnapshot(
    globalSnapshotHash: Hash,
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
        globalSnapshotHash: Hash,
        snapshot: Hashed[CurrencyIncrementalSnapshot],
        binary: Signed[StateChannelSnapshotBinary],
        info: CurrencySnapshotInfo,
        timestamp: LocalDateTime,
        hasher: Hasher[F]
      ): F[CurrencySnapshot] = for {
        blocksHashes <- snapshot.blocks.unsorted.map(_.block).map(hashBlock(_, hasher)).toList.sequence
        rewards = fetchRewards(snapshot).unsorted.map(reward =>
          RewardTransaction(
            snapshot.hash.value,
            reward.destination.value,
            reward.amount.value
          )
        )
        sizeInKb <- SizeCalculator.kilobytes(binary)
      } yield CurrencySnapshot(
        globalSnapshotHash.value,
        hash = snapshot.hash.value,
        ordinal = snapshot.ordinal.value.value,
        height = snapshot.height.value,
        subHeight = snapshot.subHeight.value,
        lastSnapshotHash = snapshot.lastSnapshotHash.value,
        blocks = blocksHashes.toSet,
        rewards = rewards,
        epochProgress = snapshot.epochProgress.value,
        timestamp = timestamp,
        version = snapshot.version.version,
        fee = binary.fee.value.value.some,
        stakingAddress = getMessageAddress(MessageType.Staking, info),
        ownerAddress = getMessageAddress(MessageType.Owner, info),
        sizeInKB = sizeInKb.toLong.some
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
            feeTx.parent.hash.value,
            snapshotHash,
            snapshotOrdinal,
            timestamp
          )
        }

      private def getMessageAddress(messageType: MessageType, info: CurrencySnapshotInfo): Option[String] =
        info.lastMessages.flatMap(_.get(messageType)).map(_.address.value.value)

    }

}
