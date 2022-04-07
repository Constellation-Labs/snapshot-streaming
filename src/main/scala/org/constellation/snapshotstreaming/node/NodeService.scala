package org.constellation.snapshotstreaming.node

import cats.effect.Async

import scala.collection.immutable.{Stream => POStream}

import org.tessellation.dag.snapshot.GlobalSnapshot
import org.tessellation.security.signature.Signed

import fs2.Stream

trait NodeService[F[_]] {
  def getSnapshots(startingOrdinal: Long, additionalOrdinals: Seq[Long]): Stream[F, Signed[GlobalSnapshot]]
}

object NodeService {

  def make[F[_]: Async](nodeDownloadsPool: Seq[NodeDownload[F]]) = new NodeService[F] {

    val nodePoolSize = nodeDownloadsPool.size
    val snapshotOrdinalStreamsPool = nodeDownloadsPool.map(nodeDownload => nodeDownload.downloadLatestOrdinal())

    override def getSnapshots(
      startingOrdinal: Long,
      additionalOrdinals: Seq[Long]
    ): Stream[F, Signed[GlobalSnapshot]] =
      for {
        (snapshotOrdinal, nodeNo) <- downloadSnapshotOrdinal(startingOrdinal, additionalOrdinals)
        snapshot <- downloadSnapshot(snapshotOrdinal, nodeNo)
      } yield snapshot

    def downloadSnapshotOrdinal(
      startingOrdinal: Long,
      additionalOrdinals: Seq[Long]
    ) = {

      def decideIfNewLatestOrdinal(
        nextOrdinalToEmit: Long,
        latestOrdinal: SnapshotOrdinal
      ): Either[Long, (Long, Long)] =
        if (latestOrdinal.value >= nextOrdinalToEmit)
          Right((nextOrdinalToEmit, latestOrdinal.value + 1))
        else
          Left(nextOrdinalToEmit)

      def downloadAndEmitNewOrdinal() =
        Stream(snapshotOrdinalStreamsPool: _*)
          .parJoin(nodePoolSize + 1)
          .scan[Either[Long, (Long, Long)]](Left(startingOrdinal)) {
            case (Left(nextOrdinalToEmit), latestOrdinal) =>
              decideIfNewLatestOrdinal(nextOrdinalToEmit, latestOrdinal)
            case (Right(range), latestOrdinal) => decideIfNewLatestOrdinal(range._2, latestOrdinal)
          }
          .flatMap {
            case Left(_)             => Stream.empty
            case Right((start, end)) => Stream.emits(start until end)
          }

      (Stream.emits(additionalOrdinals) ++ downloadAndEmitNewOrdinal())
        .map(SnapshotOrdinal(_))
        .zip(Stream.emits(0 until nodePoolSize).repeat)
    }

    def downloadSnapshot(snapshotOrdinal: SnapshotOrdinal, nodeNo: Int) = {
      val initialNodesToTry = POStream.from(nodeNo).map(_ % nodePoolSize).take(nodePoolSize).toList
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

      downloadSnapshotWithFallback(snapshotOrdinal, initialNodesToTry)
    }

  }

}
