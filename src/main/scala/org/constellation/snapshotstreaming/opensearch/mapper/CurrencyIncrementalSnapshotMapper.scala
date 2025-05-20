package org.constellation.snapshotstreaming.opensearch.mapper

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import org.constellation.snapshotstreaming.opensearch.schema.{CurrencySnapshot, RewardTransaction}

import scala.collection.immutable.SortedSet
import io.constellationnetwork.currency.schema.currency.CurrencyIncrementalSnapshot
import io.constellationnetwork.currency.schema.currency.CurrencySnapshotInfo
import io.constellationnetwork.currency.dataApplication.{FeeTransaction => OriginalFeeTransaction}
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.json.SizeCalculator
import io.constellationnetwork.schema.currencyMessage.MessageType
import io.constellationnetwork.schema.transaction.{RewardTransaction => OriginalRewardTransaction}
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.Hashed
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

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

}

object CurrencyIncrementalSnapshotMapper {

  def make[F[_]: Async: JsonSerializer](): CurrencyIncrementalSnapshotMapper[F] =
    new CurrencyIncrementalSnapshotMapper[F] {

      def fetchRewards(snapshot: CurrencyIncrementalSnapshot): SortedSet[OriginalRewardTransaction] =
        snapshot.rewards

      def extractSnapshotReferredAddresses(snapshot: CurrencyIncrementalSnapshot): SnapshotReferredAddresses = {
        val transactions = snapshot.blocks.flatMap(_.block.transactions.toSortedSet)
        val source = transactions.map(_.source)
        val destination = transactions.map(_.destination)
        SnapshotReferredAddresses(source, destination)
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

      private def getMessageAddress(messageType: MessageType, info: CurrencySnapshotInfo): Option[String] =
        info.lastMessages.flatMap(_.get(messageType)).map(_.address.value.value)

    }

}
