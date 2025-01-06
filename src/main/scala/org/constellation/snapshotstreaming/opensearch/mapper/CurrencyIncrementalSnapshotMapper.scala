package org.constellation.snapshotstreaming.opensearch.mapper

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import org.constellation.snapshotstreaming.opensearch.schema.{CurrencySnapshot, FeeTransaction, FeeTransactionReference, RewardTransaction}

import scala.collection.immutable.SortedSet
import org.tessellation.currency.schema.currency.CurrencyIncrementalSnapshot
import org.tessellation.currency.schema.currency.CurrencySnapshotInfo
import org.tessellation.currency.schema.feeTransaction.{FeeTransaction => OriginalFeeTransaction}
import org.tessellation.currency.schema.feeTransaction.{FeeTransactionReference => OriginalFeeTransactionReference}
import org.tessellation.json.JsonSerializer
import org.tessellation.json.SizeCalculator
import org.tessellation.schema.currencyMessage.MessageType
import org.tessellation.schema.transaction.{RewardTransaction => OriginalRewardTransaction}
import org.tessellation.security.signature.Signed
import org.tessellation.security.Hashed
import org.tessellation.security.Hasher
import org.tessellation.statechannel.StateChannelSnapshotBinary

import java.util.Date

abstract class CurrencyIncrementalSnapshotMapper[F[_]: Async]
    extends SnapshotMapper[F, CurrencyIncrementalSnapshot, CurrencySnapshotInfo] {

  def mapSnapshot(
    snapshot: Hashed[CurrencyIncrementalSnapshot],
    binary: Signed[StateChannelSnapshotBinary],
    info: CurrencySnapshotInfo,
    timestamp: Date,
    hasher: Hasher[F]
  ): F[CurrencySnapshot]

  def mapFeeTransactions(
    snapshot: Hashed[CurrencyIncrementalSnapshot],
    timestamp: Date,
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
        timestamp: Date,
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
        timestamp: Date,
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
        timestamp = timestamp,
        fee = binary.fee.value,
        stakingAddress = getMessageAddress(MessageType.Staking, info),
        ownerAddress = getMessageAddress(MessageType.Owner, info),
        sizeInKB = sizeInKb.toLong
      )

      private def mapFeeTransaction(snapshotHash: String, snapshotOrdinal: Long, timestamp: Date)(
        feeTransaction: Signed[OriginalFeeTransaction]
      )(implicit hasher: Hasher[F]): F[FeeTransaction] =
        feeTransaction.toHashed.map { feeTx =>
          FeeTransaction(
            feeTx.hash.value,
            feeTx.amount.value,
            feeTx.source.value,
            feeTx.destination.value,
            mapFeeTransactionRef(feeTx.parent),
            feeTx.salt.value,
            snapshotHash,
            snapshotOrdinal,
            timestamp
          )
        }

      private def mapFeeTransactionRef(ref: OriginalFeeTransactionReference): FeeTransactionReference =
        FeeTransactionReference(ref.hash.value, ref.ordinal.value)

      private def getMessageAddress(messageType: MessageType, info: CurrencySnapshotInfo): Option[String] =
        info.lastMessages.flatMap(_.get(messageType)).map(_.address.value.value)

    }

}
