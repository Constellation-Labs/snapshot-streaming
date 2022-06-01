package org.constellation.snapshotstreaming

import cats.effect.Async

import org.tessellation.kryo.KryoSerializer
import org.tessellation.security.SecurityProvider

import fs2.Stream
import org.constellation.snapshotstreaming.node.{NodeClient, NodeDownload, NodeService}
import org.constellation.snapshotstreaming.opensearch.{SnapshotDAO, UpdateRequestBuilder}
import org.http4s.client.Client
import org.typelevel.log4cats.slf4j.Slf4jLogger

trait SnapshotService[F[_]] {

  def processSnapshot(): Stream[F, Unit]
}

object SnapshotService {

  def make[F[_]: Async: KryoSerializer: SecurityProvider](
    client: Client[F],
    config: Configuration
  ): SnapshotService[F] = {
    val nodeClients = config.nodeUrls.map(url => NodeClient.make[F](client, url))
    val nodeService = NodeService.make[F](nodeClients.map(NodeDownload.make[F](_, config)))
    val requestBuilder = UpdateRequestBuilder.make[F](config)
    val dao = SnapshotDAO.make[F](requestBuilder, config)
    val processedService = ProcessedSnapshotsService.make[F](config)
    make(nodeService, dao, processedService)
  }

  def make[F[_]: Async, T](
    nodeService: NodeService[F],
    snapshotDAO: SnapshotDAO[F],
    processedSnapshotsService: ProcessedSnapshotsService[F]
  ): SnapshotService[F] =
    new SnapshotService[F] {

      private val logger = Slf4jLogger.getLoggerFromClass[F](SnapshotService.getClass)

      override def processSnapshot(): Stream[F, Unit] = {
        for {
          init <- processedSnapshotsService.initialState()
          processedSnapshots <- downloadAndSendSnapshots(init)
            .scan(init)(updateProcessedSnapshots)
          _ <- processedSnapshotsService.saveState(processedSnapshots)
        } yield (())
      }.handleErrorWith(ex => Stream.eval(logger.error(ex)("Error during processing snapshot.")) >> processSnapshot())

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
