package org.constellation.snapshotstreaming

import cats.effect.{Async, Resource}

import org.tessellation.kryo.KryoSerializer

import fs2.Stream
import org.constellation.snapshotstreaming.node.{NodeClient, NodeDownload, NodeService}
import org.constellation.snapshotstreaming.opensearch.SnapshotDAO
import org.http4s.client.Client
import org.typelevel.log4cats.slf4j.Slf4jLogger

trait SnapshotService[F[_]] {

  def processSnapshot(): Stream[F, Unit]
}

object SnapshotService {

  def make[F[_]: Async: KryoSerializer](
    client: Client[F],
    config: Configuration
  ): Resource[F, SnapshotService[F]] = for {
    dao <- SnapshotDAO.make[F](config)
    nodeClients = config.nodeUrls.map(url => NodeClient.make[F](client, url))
    nodeService = NodeService.make[F](nodeClients.map(NodeDownload.make[F](_, config)))
    processedService = ProcessedSnapshotsService.make[F](config)
  } yield make(nodeService, processedService)(dao)

  def make[F[_]: Async, T](
    nodeService: NodeService[F],
    processedSnapshotsService: ProcessedSnapshotsService[F]
  )(snapshotDAO: SnapshotDAO[F]): SnapshotService[F] =
    new SnapshotService[F] {

      private val logger = Slf4jLogger.getLoggerFromClass[F](SnapshotService.getClass)

      override def processSnapshot(): Stream[F, Unit] = {
        for {
          init <- processedSnapshotsService.initialState()
          processedSnapshots <- downloadAndSendSnapshots(init)
            .scan(init)(updateProcessedSnapshots)
            .handleErrorWith(ex => Stream.eval(logger.error(ex)("Error during processing snapshot.")) >> Stream.empty)
          _ <- processedSnapshotsService.saveState(processedSnapshots)
        } yield (())
      } ++ processSnapshot() 

      def downloadAndSendSnapshots(init: ProcessedSnapshots) = for {
        snapshot <- nodeService.getSnapshots(init.startingOrdinal, init.gaps)
        snapshotOrdinal <- snapshotDAO.sendSnapshotToOpensearch(snapshot)
      } yield snapshotOrdinal

      val updateProcessedSnapshots: (ProcessedSnapshots, Long) => ProcessedSnapshots = (previousState, ordinal) =>
        previousState match {
          case ProcessedSnapshots(Some(startingOrdinal), gaps) if ordinal >= startingOrdinal =>
            ProcessedSnapshots(Some(ordinal + 1), (startingOrdinal until ordinal).toList ::: gaps)
          case ProcessedSnapshots(lastProcessedOrdinal, gaps) =>
            ProcessedSnapshots(lastProcessedOrdinal, gaps.filter(_ != ordinal))
        }

    }

}
