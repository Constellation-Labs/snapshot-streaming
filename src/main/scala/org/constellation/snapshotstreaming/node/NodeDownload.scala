package org.constellation.snapshotstreaming.node

import cats.effect.Async
import cats.syntax.either._

import scala.concurrent.duration._

import org.tessellation.dag.snapshot.GlobalSnapshot
import org.tessellation.security.signature.Signed

import fs2.Stream
import org.constellation.snapshotstreaming.Configuration
import org.constellation.snapshotstreaming.node.NodeClient
import org.typelevel.log4cats.slf4j.Slf4jLogger

trait NodeDownload[F[_]] {
  def downloadLatestOrdinal(): Stream[F, SnapshotOrdinal]
  def downloadSnapshot(ordinal: SnapshotOrdinal): Stream[F, Either[SnapshotOrdinal, Signed[GlobalSnapshot]]]
}

object NodeDownload {

  def make[F[_]: Async](nodeClient: NodeClient[F], config: Configuration) = new NodeDownload[F] {

    private val logger = Slf4jLogger.getLoggerFromClass[F](NodeDownload.getClass)

    override def downloadLatestOrdinal(): Stream[F, SnapshotOrdinal] = for {
      _ <- Stream.awakeEvery(config.nodeIntervalInSeconds.seconds)
      ordinal <- Stream
        .eval(nodeClient.getLatestOrdinal)
        .handleErrorWith(ex =>
          Stream.eval(
            logger.error(ex)(s"Couldn't download latest ordinal error from ${nodeClient.uri}.")
          ) >> Stream.empty
        )
        .evalTap(ordinal => logger.debug(s"Latest ordinal from ${nodeClient.uri} is $ordinal."))
    } yield ordinal

    override def downloadSnapshot(
      ordinal: SnapshotOrdinal
    ): Stream[F, Either[SnapshotOrdinal, Signed[GlobalSnapshot]]] =
      Stream
        .eval(nodeClient.getSnapshot(ordinal))
        .evalTap(s => logger.info(s"Snapshot ${s.value.ordinal.value} downloaded from ${nodeClient.uri}."))
        .handleErrorWith(ex =>
          Stream.eval(logger.error(ex)(s"Couldn't download snapshot $ordinal from ${nodeClient.uri}.")) >> Stream
            .raiseError(ex)
        )
        .attempt
        .map(_.leftMap(_ => ordinal))

  }

}
