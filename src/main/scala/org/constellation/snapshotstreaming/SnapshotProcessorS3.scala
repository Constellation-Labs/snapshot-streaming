package org.constellation.snapshotstreaming

import cats.effect._
import cats.effect.std.{Console, Random}
import cats.syntax.all._
import cats.Parallel
import cats.data.Validated
import cats.effect.implicits.clockOps
import com.sksamuel.elastic4s.ElasticApi.{fieldSort, matchAllQuery, search, termQuery}
import com.sksamuel.elastic4s.requests.searches.SearchHit
import fs2.Stream
import fs2.io.file.Files
import fs2.io.net.Network
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.merkletree.StateProofValidator
import io.constellationnetwork.node.shared.config.types.SharedConfigReader
import io.constellationnetwork.node.shared.domain.snapshot.storage.LastSnapshotStorage
import io.constellationnetwork.schema.SnapshotReference.{fromHashedSnapshot => getSnapshotReference}
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshot, GlobalSnapshotInfo, GlobalSnapshotInfoV2, SnapshotOrdinal}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import org.constellation.snapshotstreaming.SnapshotProcessor.{GlobalSnapshotWithState, ProcessedSnapshots}
import org.constellation.snapshotstreaming.db.SnapshotDAO
import org.constellation.snapshotstreaming.mapper.{CurrencySnapshotMapper, GlobalSnapshotMapper}
import org.constellation.snapshotstreaming.opensearch.OpensearchDAO
import org.constellation.snapshotstreaming.schema.schema.{GlobalData, MetagraphData}
import org.constellation.snapshotstreaming.s3.S3DAO
import org.constellation.snapshotstreaming.storage.{FileBasedLastGlobalFullSnapshotStorage, FileBasedLastGlobalIncrementalSnapshotStorage}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.otel4s.trace.Tracer

import java.time.{Instant, LocalDateTime, ZoneId}

trait SnapshotProcessorS3[F[_]] {
  val runtime: Stream[F, Unit]
}

object SnapshotProcessorS3 {

  def make[F[_] : Async : Parallel : KryoSerializer : JsonSerializer : SecurityProvider : Random
  : HasherSelector : Network : Files : Tracer : Console](
     configuration: SnapshotStreamingConfig,
     sharedConfig: SharedConfigReader,
     txHasher: Hasher[F]
   ): Resource[F, SnapshotProcessor[F]] =
    for {
      s3DAO <- S3DAO.make[F](configuration.s3)
      opensearchDAO <- OpensearchDAO.make[F](configuration.opensearch)
      sessionPool <- db.session[F](configuration.db)
      snapshotDAO = SnapshotDAO.make[F](sessionPool)
      lastIncrementalGlobalSnapshotStorage <- Resource.eval(fsGlobalIncrementalStorage(configuration))
      tesselationServices <- Resource.eval(
        TessellationServices.make[F](configuration.environment, sharedConfig)
      )
      lastFullGlobalSnapshotStorage = FileBasedLastGlobalFullSnapshotStorage.make[F, GlobalSnapshot](
        configuration.lastSnapshotPath
      )
    } yield make(
      configuration,
      lastIncrementalGlobalSnapshotStorage,
      s3DAO,
      snapshotDAO,
      opensearchDAO,
      GlobalSnapshotMapper.make(),
      CurrencySnapshotMapper.make(),
      txHasher,
      tesselationServices,
      lastFullGlobalSnapshotStorage
    )

  private def fsGlobalIncrementalStorage[F[_] : Async : Parallel: HasherSelector : Files : KryoSerializer](
                                                                                                  configuration: SnapshotStreamingConfig
                                                                                                ) =
    FileBasedLastGlobalIncrementalSnapshotStorage.make[F](configuration.lastIncrementalSnapshotPath)

  def make[F[_] : Async : Parallel : HasherSelector](
                                                      configuration: SnapshotStreamingConfig,
                                                      lastIncrementalGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
                                                      s3DAO: S3DAO[F],
                                                      snapshotDAO: SnapshotDAO[F],
                                                      opensearchDAO: OpensearchDAO[F],
                                                      globalMapper: GlobalSnapshotMapper[F],
                                                      currencyMapper: CurrencySnapshotMapper[F],
                                                      txHasher: Hasher[F],
                                                      tessellationServices: TessellationServices[F],
                                                      lastFullGlobalSnapshotStorage: FileBasedLastGlobalFullSnapshotStorage[F]
                                                    ): SnapshotProcessor[F] = new SnapshotProcessor[F] {
    private val logger = Slf4jLogger.getLogger[F]


    private def storeInPostgres(global: GlobalData, metagraph: MetagraphData) =
          (snapshotDAO.insertGlobalData(global, metagraph.snapshots.size) >> snapshotDAO
          .insertMetagraphData(global.snapshot.hash, metagraph)
          .whenA(metagraph.snapshots.nonEmpty)).timed.flatMap{ t =>
        logger
          .info(s"Snapshot ${global.snapshot.ordinal} (hash: ${global.snapshot.hash.show}) sent to postgres in ${t._1.toSeconds}.") }
          .handleErrorWith(s => logger.error(s)("Error in database layer") >> s.raiseError[F, Unit])

    private def splitData(globalSnapshotWithState: GlobalSnapshotWithState, d: LocalDateTime, hasher: Hasher[F]) = (
      globalMapper.mapGlobalSnapshot(globalSnapshotWithState, d, hasher, txHasher),
      currencyMapper.mapCurrencySnapshots(globalSnapshotWithState, d, hasher, txHasher)
    ).tupled

    private def store(globalSnapshotWithState: GlobalSnapshotWithState, ts: LocalDateTime, hasher: Hasher[F]): F[Unit] =
      splitData(globalSnapshotWithState, ts, hasher).flatMap { case (globalData, metagraphData) =>
        storeInPostgres(globalData, metagraphData)
      }.void

    private def process(globalSnapshotWithState: GlobalSnapshotWithState, hasher: Hasher[F]): F[Unit] = {
      val GlobalSnapshotWithState(snapshot, _, snapshotInfo, _, dt) = globalSnapshotWithState
      HasherSelector[F]
        .forOrdinal(snapshot.ordinal) { implicit hasher =>
          logger.info(
            s"Global Snapshot ${snapshot.ordinal.value.value} with logic=${hasher.getLogic(snapshot.ordinal)}"
          ) >>
            (hasher.getLogic(snapshot.ordinal) match {
              case JsonHash => StateProofValidator.validate(snapshot, snapshotInfo)
              case KryoHash =>
                StateProofValidator.validate(snapshot, GlobalSnapshotInfoV2.fromGlobalSnapshotInfo(snapshotInfo))
            })
        }
        .flatMap {
          case Validated.Valid(()) =>
            store(globalSnapshotWithState, dt, hasher) //>>

          case Validated.Invalid(e) =>
            logger.warn(
              s"Calculated stateProof does not match state from snapshot: ${e}."
            )
        }
    }

    val searchGlobalSnapshots =
      search(configuration.opensearch.indexes.snapshots)
        .query(matchAllQuery())
        .sortBy(fieldSort("ordinal").asc())
        .sourceInclude("ordinal", "hash", "timestamp")

    def hitMapper(hit: SearchHit) = {
      val row = hit.sourceAsMap
      val hashO = row.get("hash").map(_.toString)
      val tsO = row.get("timestamp").map(dateTimeString => {
        val instant = Instant.parse(dateTimeString.toString)
        LocalDateTime.ofInstant(instant, ZoneId.of("UTC"))
      })
      (hashO, tsO).tupled
    }

    def cursorMapper(hit: SearchHit) = hit.sourceAsMap.get("ordinal").map(_.toString.toLong)


    def getGlobalSnapshotByOrdinal(ordinal: SnapshotOrdinal)(implicit hs: HasherSelector[F]) : F[Option[Hashed[GlobalIncrementalSnapshot]]] = {
      implicit val hasher = hs.getForOrdinal(ordinal)
      val q= search(configuration.opensearch.indexes.snapshots)
        .query(termQuery("ordinal", ordinal.value.value))
      opensearchDAO.singleQuery(q, hitMapper).flatMap ( _.traverse { case (hash, _) =>
        s3DAO.downloadSnapshot(Hash(hash)).flatMap(_.toHashed)
      })
    }

    val reindexerConf = configuration.reindexer.get

    val runtime: Stream[F, Unit] = {

      Stream
        .eval(lastFullGlobalSnapshotStorage.get.map(_.get))
        .evalMap(full => lastIncrementalGlobalSnapshotStorage.getCombined.map(inc => (full, inc) ))
        .flatMap { case (signedFullGlobalSnapshot, hashedIncrementalCombinedO)  =>

          val startAfterOrdinal = hashedIncrementalCombinedO.map(_._1.ordinal).orElse(signedFullGlobalSnapshot.value.ordinal.some).map(_.value.value)

          opensearchDAO
            .bulkStream(searchGlobalSnapshots, hitMapper, cursorMapper, startAfterOrdinal)
            .parEvalMap(reindexerConf.s3Parallelism) { case (h, ts) =>
              logger.info(s"Downloading hash ${h} from S3") >>
                s3DAO.downloadSnapshot(Hash(h)).flatMap { snapshot =>
                  implicit val hasher = HasherSelector[F].getForOrdinal(snapshot.ordinal)
                  logger.info(s" ordinal ${snapshot.ordinal} for hash ${h}")
                  snapshot.toHashed[F]
                }.map((h, _, ts))
            }.prefetchN(reindexerConf.s3Parallelism*2)
            .evalMapAccumulate(hashedIncrementalCombinedO.map{ case (lastSnapshot, lastState) => ProcessedSnapshots(lastSnapshot.signed, lastState, List.empty)}) {
              case (None, (gsHash, snapshot, dt)) =>
                val gss= GlobalSnapshotWithState(snapshot.copy(hash = Hash(gsHash)), None, signedFullGlobalSnapshot.value.info, Map.empty, dt)
                (Option(ProcessedSnapshots( snapshot.signed, signedFullGlobalSnapshot.value.info , List(gss))), gss).pure
              case (Some(processoStatus), (gsHash, snapshot, dt)) =>
                  tessellationServices.globalSnapshotContextService
                    .createContext(
                      processoStatus.lastState,
                      processoStatus.lastSnapshot,
                      snapshot,
                      getGlobalSnapshotByOrdinal,
                      dt
                    ).map { newContext =>
                      val updatedSnapshot = newContext.snapshot
                      val updatedPprocessoStatus = processoStatus.copy(
                        lastSnapshot = updatedSnapshot.signed,
                        lastState = newContext.snapshotInfo,
                        List(newContext)
                      )
                      (Option(updatedPprocessoStatus), newContext)
                    }
            }
        }.map(_._2)
        .prefetchN(reindexerConf.snapshotContextPrefetch)
        .evalTap { case GlobalSnapshotWithState(snapshot, _, _, _, _) =>
            logger.info(s"Pulled following global snapshot: ${getSnapshotReference(snapshot).show}")
        }
        .parEvalMap(reindexerConf.dbParallelism) { case state@GlobalSnapshotWithState(snapshot, _, _, _, _) =>
            val hasher = HasherSelector[F].getForOrdinal(snapshot.ordinal)
            process(state, hasher).map(_ => state)
        }.chunkMin(configuration.checkpointEvery)
        .evalMap { snapshots =>
          snapshots.last.traverse { last =>
            logger.info(s"Checkpoint at snapshot ordinal ${last.snapshot.ordinal} hash ${last.snapshot.hash} ") >>
              lastIncrementalGlobalSnapshotStorage.set(last.snapshot, last.snapshotInfo)
          }
        }.void
    }
  }
}
