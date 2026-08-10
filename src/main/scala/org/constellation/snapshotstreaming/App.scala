package org.constellation.snapshotstreaming

import cats.effect._
import cats.effect.std.Random
import cats.syntax.all._
import io.constellationnetwork._
import io.constellationnetwork.ext.cats.effect._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.config.types.SharedConfigReader
import io.constellationnetwork.schema.{GlobalStateProofSelector, CurrencyStateProofSelector, SnapshotOrdinal}
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
              JsonSerializer.forAsync[IO].asResource.use { implicit jsonSerializer =>
                val hashSelect = makeHashSelect(appConfig, sharedCfg)
                // `subTrieRootsActivationOrdinal` MUST be passed. It defaults to MaxValue (sub-trie
                // roots OFF), and `sub-trie-roots` is the only fields-added gate that changes the
                // SIGNED GlobalSnapshotStateProof, which this indexer independently re-derives and
                // validates. With gl0 signing roots ON and this selector OFF, every post-activation
                // ordinal fails `StateProof Broken` on the sub-trie fields and nothing is indexed.
                // Mirrors gl0's own construction in TessellationIOApp.
                // Resolved into named vals so the values LOGGED are byte-identical to the values
                // USED -- a diagnostic that recomputes them can disagree with the selector.
                val ssEnv = appConfig.snapshotStreaming.environment
                val lastLegacyOrd =
                  sharedCfg.lastLegacyStateProofOrdinal.getOrElse(ssEnv, SnapshotOrdinal.MinValue)
                val subTrieOrd =
                  sharedCfg.fieldsAddedOrdinals.subTrieRoots.getOrElse(ssEnv, SnapshotOrdinal.MaxValue)
                // println, not the IO logger: these are plain vals outside the IO chain, so an
                // unsequenced logger.info would never run. stdout is captured by journald.
                println(
                  s"[STATE-PROOF-SELECTOR] env=$ssEnv " +
                    s"lastLegacyStateProofOrdinal=${lastLegacyOrd.value.value} " +
                    s"subTrieRootsActivationOrdinal=${subTrieOrd.value.value} " +
                    s"subTrieRootsKeys=${sharedCfg.fieldsAddedOrdinals.subTrieRoots.keys.mkString(",")} " +
                    s"lastLegacyKeys=${sharedCfg.lastLegacyStateProofOrdinal.keys.mkString(",")}"
                )
                implicit val gsps: GlobalStateProofSelector =
                  GlobalStateProofSelector(lastLegacyOrd, subTrieOrd)
                implicit val csps: CurrencyStateProofSelector = CurrencyStateProofSelector.instance
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
