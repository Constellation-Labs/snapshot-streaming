package org.constellation.snapshotstreaming

import cats.effect._
import cats.effect.std.Random
import cats.syntax.all._
import com.esotericsoftware.minlog.Log
import org.tessellation._
import org.tessellation.ext.cats.effect._
import org.tessellation.json.JsonSerializer
import org.tessellation.kryo.KryoSerializer
import org.tessellation.node.shared.config.types.SharedConfigReader
import org.tessellation.node.shared.ext.pureconfig._
import org.tessellation.schema.SnapshotOrdinal
import org.tessellation.security._
import eu.timepit.refined.pureconfig._
import org.constellation.snapshotstreaming.schema.kryoRegistrar
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.otel4s.trace.Tracer.Implicits.noop
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import pureconfig.module.enumeratum._
import org.tessellation.ext.kryo._
import org.tessellation.node.shared.nodeSharedKryoRegistrar
import org.tessellation.shared.sharedKryoRegistrar

object AppReindexer extends IOApp {
  private val logger = Slf4jLogger.getLogger[IO]

  def run(args: List[String]): IO[ExitCode] =
    Configuration
      .load[IO]
      .flatMap { appConfig =>
        ConfigSource.default.loadF[IO, SharedConfigReader]().flatMap { sharedCfg =>
          Random.scalaUtilRandom[IO].flatMap { implicit random =>
            val cryOs = sharedKryoRegistrar.union(kryoRegistrar)
            KryoSerializer.forAsync[IO](cryOs).use { implicit ks =>
              JsonSerializer.forSync[IO].asResource.use { implicit jsonSerializer =>
                val hashSelect = makeHashSelect(appConfig, sharedCfg)
                implicit val hasherSelector =
                  HasherSelector.forSync[IO](Hasher.forJson[IO], Hasher.forKryo[IO], hashSelect)

                val txHasher = Hasher.forKryo[IO]

                SecurityProvider.forAsync[IO].use { implicit sp =>
                  ReindexerSnapshotProcessor
                    .make[IO](
                      appConfig.snapshotStreaming,
                      sharedCfg,
                      txHasher
                    )
                    .use { snapshotProcessorS3 =>
                      snapshotProcessorS3.runtime.compile.drain.recoverWith { case e => logger.error(s"$e") }
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
