package org.constellation.snapshotstreaming

import cats.effect._
import cats.effect.std.Random
import cats.syntax.all._
import io.constellationnetwork._
import io.constellationnetwork.ext.cats.effect._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.config.types.SharedConfigReader
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security._
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.otel4s.trace.Tracer.Implicits.noop
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import io.constellationnetwork.node.shared.ext.pureconfig._
import eu.timepit.refined.pureconfig._
import pureconfig.module.enumeratum._

object App extends IOApp {
  private val logger = Slf4jLogger.getLogger[IO]

  def run(args: List[String]): IO[ExitCode] =
    Configuration
      .load[IO]
      .flatMap { appConfig =>
        ConfigSource.default.loadF[IO, SharedConfigReader]().flatMap { sharedCfg =>
          Random.scalaUtilRandom[IO].flatMap { implicit random =>
            KryoSerializer.forAsync[IO](shared.sharedKryoRegistrar).use { implicit ks =>
              JsonSerializer.forSync[IO].asResource.use { implicit jsonSerializer =>
                val hashSelect = makeHashSelect(appConfig, sharedCfg)
                implicit val hasherSelector =
                  HasherSelector.forSync[IO](Hasher.forJson[IO], Hasher.forKryo[IO], hashSelect)

                val txHasher = Hasher.forKryo[IO]

                SecurityProvider.forAsync[IO].use { implicit sp =>
                  SnapshotProcessor
                    .make[IO](
                      appConfig.snapshotStreaming,
                      sharedCfg,
                      txHasher
                    )
                    .use { snapshotProcessor =>
                      snapshotProcessor.runtime.compile.drain.recoverWith { case e => logger.error(s"$e") }
                        .flatTap(_ => logger.info("Done!"))
                        .as(ExitCode.Success)
                    }
                }
              }
            }
          }
        }
      }
      .handleErrorWith(e => logger.error(e)(e.getMessage).as(ExitCode.Error))

  private def makeHashSelect(appConfig: AppConfig, cfg: SharedConfigReader) =
    new HashSelect {

      val lastKryoHashOrdinal: SnapshotOrdinal =
        cfg.lastKryoHashOrdinal.getOrElse(appConfig.snapshotStreaming.environment, SnapshotOrdinal.MinValue)

      def select(ordinal: SnapshotOrdinal): HashLogic =
        if (ordinal <= lastKryoHashOrdinal) KryoHash else JsonHash

    }

}
