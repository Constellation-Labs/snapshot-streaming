package org.constellation.snapshotstreaming

import cats.effect._
import cats.effect.std.Random

import org.tessellation._
import org.tessellation.ext.cats.effect._
import org.tessellation.kryo.KryoSerializer
import org.tessellation.security.SecurityProvider

import org.typelevel.log4cats.slf4j.Slf4jLogger

object App extends IOApp {
  private val logger = Slf4jLogger.getLogger[IO]

  def run(args: List[String]): IO[ExitCode] =
    Random
      .scalaUtilRandom[IO]
      .asResource
      .use { implicit random =>
        KryoSerializer.forAsync[IO](dag.dagSharedKryoRegistrar ++ shared.sharedKryoRegistrar).use { implicit ks =>
          IO(new Configuration).flatMap { configuration =>
            SecurityProvider.forAsync[IO].use { implicit sp =>
              SnapshotProcessor
                .make[IO](configuration)
                .use { snapshotProcessor =>
                  snapshotProcessor.runtime.compile.drain
                    .flatTap(_ => logger.info("Done!"))
                    .as(ExitCode.Success)
                }
            }
          }
        }
      }
      .handleErrorWith(e => logger.error(e)(e.getMessage).as(ExitCode.Error))

}
