package org.constellation.snapshotstreaming

import cats.effect._

import org.tessellation._
import org.tessellation.kryo.KryoSerializer
import org.tessellation.security.SecurityProvider

import fs2._
import org.http4s.ember.client.EmberClientBuilder
import org.typelevel.log4cats.slf4j.Slf4jLogger

object App extends IOApp {
  private val logger = Slf4jLogger.getLogger[IO]
  private val configuration = new Configuration

  def run(args: List[String]): IO[ExitCode] =
    SecurityProvider.forAsync[IO].use { implicit sp =>
      KryoSerializer.forAsync[IO](dag.dagSharedKryoRegistrar ++ shared.sharedKryoRegistrar).use {
        implicit kryoSerializer =>
          nodeStream[IO].compile.drain
            .flatTap(_ => logger.debug("Done!"))
            .map(_ => ExitCode.Success)
            .handleErrorWith(e => logger.warn(e)(e.getMessage).map(_ => ExitCode.Error))
      }
    }

  def nodeStream[F[_]: Async: KryoSerializer: SecurityProvider] =
    for {
      client <- Stream.resource(
        EmberClientBuilder
          .default[F]
          .withTimeout(configuration.httpClientTimeout)
          .withIdleTimeInPool(configuration.httpClientIdleTime)
          .build
      )
      snapshotService = SnapshotService.make(client, configuration)

      _ <- snapshotService.processSnapshot()
    } yield ()

}
