package org.constellation.snapshotstreaming.node

import cats.effect.Async

import scala.collection.immutable.{NumericRange, Stream => POStream}

import org.tessellation.dag.snapshot.GlobalSnapshot
import org.tessellation.security.signature.Signed

import fs2.Stream

trait NodeService[F[_]] {
  def getSnapshots(startingOrdinal: Option[Long], gapsOrdinals: Seq[Long]): Stream[F, Signed[GlobalSnapshot]]
}

object NodeService {

  def make[F[_]: Async](nodeDownloadsPool: Seq[NodeDownload[F]]) = new NodeService[F] {

    private val nodePoolSize = nodeDownloadsPool.size
    private val snapshotOrdinalStreamsPool = nodeDownloadsPool.map(nodeDownload => nodeDownload.downloadLatestOrdinal())

    override def getSnapshots(
      startingOrdinal: Option[Long],
      gapsOrdinals: Seq[Long]
    ): Stream[F, Signed[GlobalSnapshot]] =
      for {
        (snapshotOrdinal, nodeNo) <- downloadSnapshotOrdinal(startingOrdinal, gapsOrdinals)
        snapshot <- downloadSnapshot(snapshotOrdinal, nodeNo)
      } yield snapshot

    private def downloadSnapshotOrdinal(
      startingOrdinal: Option[Long],
      gapsOrdinals: Seq[Long]
    ) = (Stream.emits(gapsOrdinals) ++ startingOrdinal.map(downloadAndEmitNewOrdinals(_)).getOrElse(Stream.empty))
      .map(SnapshotOrdinal(_))
      .zip(nodePoolNumbers)

    private def nodePoolNumbers = Stream.emits(0 until nodePoolSize).repeat

    private def downloadAndEmitNewOrdinals(startingOrdinal: Long) = {
      def buildOrdinalsRange(
        nextOrdinalToEmit: Long,
        latestOrdinal: SnapshotOrdinal
      ): Either[Long, NumericRange[Long]] =
        if (latestOrdinal.value >= nextOrdinalToEmit)
          Right(Range.Long(nextOrdinalToEmit, latestOrdinal.value + 1, 1L))
        else
          Left(nextOrdinalToEmit)

      Stream(snapshotOrdinalStreamsPool: _*)
        .parJoin(nodePoolSize + 1)
        .scan[Either[Long, NumericRange[Long]]](Left(startingOrdinal)) {
          case (Left(nextOrdinalToEmit), latestOrdinal) => buildOrdinalsRange(nextOrdinalToEmit, latestOrdinal)
          case (Right(previousRange), latestOrdinal)    => buildOrdinalsRange(previousRange.end, latestOrdinal)
        }
        .flatMap {
          case Left(_)      => Stream.empty
          case Right(range) => Stream.emits(range.iterator.toSeq)
        }
    }

    private def downloadSnapshot(snapshotOrdinal: SnapshotOrdinal, nodeNumberFromWhichStartTrying: Int) = {
      val nodesToTry = POStream.from(nodeNumberFromWhichStartTrying).map(_ % nodePoolSize).take(nodePoolSize).toList

      def downloadSnapshotWithFallback(
        snapshotOrdinal: SnapshotOrdinal,
        nodesToTry: List[Int]
      ): Stream[F, Signed[GlobalSnapshot]] = nodesToTry match {
        case Nil =>
          Stream.raiseError(new Throwable(s"Couldn't download snapshot no. ${snapshotOrdinal.value} from any node."))
        case node :: otherNodes =>
          nodeDownloadsPool(node).downloadSnapshot(snapshotOrdinal).flatMap {
            case Left(_)      => downloadSnapshotWithFallback(snapshotOrdinal, otherNodes)
            case Right(value) => Stream.emit(value)
          }
      }

      downloadSnapshotWithFallback(snapshotOrdinal, nodesToTry)
    }

  }

}
