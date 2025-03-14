package org.constellation.snapshotstreaming.mapper

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.constellationnetwork.currency.dataApplication.{FeeTransaction => OriginalFeeTransaction}
import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshotInfo}
import io.constellationnetwork.json.{JsonSerializer, SizeCalculator}
import io.constellationnetwork.schema.currencyMessage.MessageType
import io.constellationnetwork.schema.transaction.{RewardTransaction => OriginalRewardTransaction}
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, Hasher}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary
import org.constellation.snapshotstreaming.schema.{CurrencySnapshot, FeeTransaction, RewardTransaction}

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

    }

}
