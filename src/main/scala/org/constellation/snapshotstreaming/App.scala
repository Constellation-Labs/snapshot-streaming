package org.constellation.snapshotstreaming

import cats.effect._
import cats.effect.std.Random
import cats.syntax.all._

import org.tessellation._
import org.tessellation.ext.cats.effect._
import org.tessellation.json.JsonSerializer
import org.tessellation.kryo.KryoSerializer
import org.tessellation.node.shared.config.types.SharedConfigReader
import org.tessellation.node.shared.ext.pureconfig._
import org.tessellation.schema.SnapshotOrdinal
import org.tessellation.security._

import eu.timepit.refined.pureconfig._
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import pureconfig.module.enumeratum._

object App extends IOApp {
  private val logger = Slf4jLogger.getLogger[IO]

  def run(args: List[String]): IO[ExitCode] =
    ConfigSource.default.loadF[IO, SharedConfigReader]().flatMap { sharedCfg =>
      IO(new Configuration).flatMap { configuration =>
        val hashSelect = new HashSelect {
          def select(ordinal: SnapshotOrdinal): HashLogic =
            if (ordinal <= sharedCfg.lastKryoHashOrdinal.getOrElse(configuration.environment, SnapshotOrdinal.MinValue)) KryoHash
            else JsonHash
        }

        Random
          .scalaUtilRandom[IO]
          .asResource
          .use { implicit random =>
            KryoSerializer.forAsync[IO](shared.sharedKryoRegistrar).use { implicit ks =>
              JsonSerializer.forSync[IO].asResource.use { implicit jsonSerializer =>
                implicit val hasher = Hasher.forSync[IO](hashSelect)

                SecurityProvider.forAsync[IO].use { implicit sp =>
                  SnapshotProcessor
                    .make[IO](configuration, hashSelect, sharedCfg.snapshot.size)
                    .use { snapshotProcessor =>
                      snapshotProcessor.runtime.compile.drain
                        .flatTap(_ => logger.info("Done!"))
                        .as(ExitCode.Success)
                    }
                }
              }
            }
          }
      }
        .handleErrorWith(e => logger.error(e)(e.getMessage).as(ExitCode.Error))
    }

}
