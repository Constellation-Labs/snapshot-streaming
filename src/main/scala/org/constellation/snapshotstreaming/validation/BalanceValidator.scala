package org.constellation.snapshotstreaming.validation

import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.schema.address.AddressCacheData
import org.constellation.schema.checkpoint.CheckpointBlock
import org.constellation.schema.snapshot.{SnapshotInfo, StoredSnapshot}
import org.constellation.snapshotstreaming.s3.S3DeserializedResult

case class ScanMalformedAddressesResult(
  malformedBalances: Map[String, AddressCacheData] = Map.empty,
  malformedHeightsCount: Map[String, Long] = Map.empty,
  height: Long = 0L
)

case class PotentiallyDoubled(
  addressCacheData: AddressCacheData,
  counter: Long
) {}

class BalanceValidator[F[_]: Concurrent](initialSwap: Map[String, Long]) {
  private val logger = Slf4jLogger.getLogger[F]
  private val normalizationFactor = 1e8
  private val potentialDoubledHistory = Ref.unsafe(Map.empty[String, PotentiallyDoubled])

  def validate(result: S3DeserializedResult): F[Unit] =
    for {
      _ <- checkNegativeBalances(result)
      _ <- checkPotentialDoubles(result)
      _ <- checkAwaitingBlocks(result)
      _ <- checkRealTransactions(result)
    } yield ()

  private def getBlocks(snapshot: StoredSnapshot): Seq[CheckpointBlock] =
    snapshot.checkpointCache.map(_.checkpointBlock)

  private def findBlocksWithRealTransactionsOnly(snapshot: StoredSnapshot) =
    getBlocks(snapshot)
      .map(cb => cb.copy(transactions = cb.transactions.filterNot(_.isDummy)))
      .filter(_.transactions.nonEmpty)

  private def findMalformedAddresses(
    snapshotInfo: SnapshotInfo,
    previouslyMalformed: Map[String, PotentiallyDoubled]
  ) =
    snapshotInfo.addressCacheData.filter {
      case (address, data) => {
        val diff = data.balance - data.balanceByLatestSnapshot
        val diffAccordingToInitialSwap = initialSwap.get(address).fold(diff)(diff - _)
        val balanceByLatestChanged = previouslyMalformed
          .get(address)
          .exists(prev => prev.addressCacheData.balanceByLatestSnapshot != data.balanceByLatestSnapshot)
        val negative = data.balanceByLatestSnapshot < 0
        diffAccordingToInitialSwap != 0 && !balanceByLatestChanged && !negative
      }
    }

  private def findFixedMalformedAddresses(
    snapshotInfo: SnapshotInfo,
    previouslyMalformed: Map[String, PotentiallyDoubled]
  ) =
    snapshotInfo.addressCacheData.filter {
      case (address, a) => previouslyMalformed.contains(address) && a.balance == a.balanceByLatestSnapshot
    }

  private def findNegativeBalanceAddresses(snapshotInfo: SnapshotInfo) =
    snapshotInfo.addressCacheData.filter { case (_, a) => a.balanceByLatestSnapshot < 0 }

  private def hasAwaitingBlocks(snapshotInfo: SnapshotInfo) = snapshotInfo.awaitingCbs.nonEmpty

  private def checkRealTransactions(result: S3DeserializedResult): F[Unit] = {
    val blocksWithRealTransactions = findBlocksWithRealTransactionsOnly(result.snapshot)
    if (blocksWithRealTransactions.nonEmpty) {
      logger.debug(
        s"Found real transactions at height ${result.height} in blocks: ${blocksWithRealTransactions.map(_.baseHash)}"
      )
    } else Concurrent[F].unit
  }

  private def checkAwaitingBlocks(result: S3DeserializedResult): F[Unit] =
    if (hasAwaitingBlocks(result.snapshotInfo)) {
      logger.debug(
        s"Found awaiting blocks in SnapshotInfo: ${result.snapshotInfo.awaitingCbs.map(_.checkpointBlock.baseHash)}"
      )
    } else Concurrent[F].unit

  private def checkNegativeBalances(result: S3DeserializedResult): F[Unit] = {
    val negativeBalances = findNegativeBalanceAddresses(result.snapshotInfo)

    if (negativeBalances.nonEmpty) {
      logger.debug(s"Found ${negativeBalances.size} negative balances at height ${result.height}") >> negativeBalances.toList.traverse {
        case (address, a) =>
          logger.debug(
            s"Negative balance $address, balance=${a.balance / normalizationFactor},  balanceBylatest=${a.balanceByLatestSnapshot / normalizationFactor}, height=${result.height}"
          )
      }.void
    } else Concurrent[F].unit
  }

  private def checkPotentialDoubles(result: S3DeserializedResult): F[Unit] =
    for {
      prev <- potentialDoubledHistory.get
      newMalformedHeights = findMalformedAddresses(result.snapshotInfo, prev)
      fixedMalformedHeights = findFixedMalformedAddresses(result.snapshotInfo, prev)
      updated = prev -- fixedMalformedHeights.keySet ++ newMalformedHeights.map {
        case (address, a) =>
          address -> PotentiallyDoubled(a, prev.get(address).map(_.counter).getOrElse(0L) + 1)
      }
      _ <- if (newMalformedHeights.nonEmpty) {
        logger.debug(
          s"Found ${newMalformedHeights.size} new malformed addresses at height ${result.height} (${updated.size} in total, ${fixedMalformedHeights.size} fixed) "
        ) >> updated
          .filterKeys(newMalformedHeights.contains)
          .toList
          .traverse {
            case (address, a) =>
              val balance = a.addressCacheData.balance
              val balanceByLatestSnapshot = a.addressCacheData.balanceByLatestSnapshot
              val diff = balance - balanceByLatestSnapshot

              logger.debug(
                s"Potentially malformed $address, balance=${balance / normalizationFactor},  balanceBylatest=${balanceByLatestSnapshot / normalizationFactor}, diff=${diff / normalizationFactor}, malformedInSnapshotsCount=${a.counter}, height=${result.height}"
              )
          }
      } else Concurrent[F].unit
      _ <- potentialDoubledHistory.set(updated)
    } yield ()
}
