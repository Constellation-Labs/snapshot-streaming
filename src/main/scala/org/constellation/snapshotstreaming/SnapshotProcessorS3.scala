package org.constellation.snapshotstreaming

import cats.data.Validated
import cats.effect._
import cats.effect.std.{Console, Random}
import cats.syntax.all._
import cats.Parallel
import com.sksamuel.elastic4s.ElasticApi.{fieldSort, matchAllQuery, search}
import com.sksamuel.elastic4s.requests.searches.SearchHit
import com.sksamuel.elastic4s.requests.update.UpdateRequest
import fs2.Stream
import fs2.io.file.Files
import fs2.io.net.Network
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.merkletree.StateProofValidator
import io.constellationnetwork.node.shared.config.types.SharedConfigReader
import io.constellationnetwork.node.shared.domain.snapshot.storage.LastSnapshotStorage
import io.constellationnetwork.node.shared.infrastructure.cluster.storage.L0ClusterStorage
import io.constellationnetwork.schema.SnapshotReference.{fromHashedSnapshot => getSnapshotReference}
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshot, GlobalSnapshotInfo, GlobalSnapshotInfoV2}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import org.constellation.snapshotstreaming.SnapshotProcessor.{GlobalSnapshotWithState, ProcessedSnapshots}
import org.constellation.snapshotstreaming.db.SnapshotDAO
import org.constellation.snapshotstreaming.mapper.{CurrencySnapshotMapper, GlobalSnapshotMapper}
import org.constellation.snapshotstreaming.opensearch.OpensearchDAO
import org.constellation.snapshotstreaming.schema.schema.{GlobalData, MetagraphData}
import org.constellation.snapshotstreaming.s3.S3DAO
import org.constellation.snapshotstreaming.storage.{FileBasedLastGlobalFullSnapshotStorage, FileBasedLastGlobalIncrementalSnapshotStorage}
import org.http4s.ember.client.EmberClientBuilder
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
      s3DAO <- configuration.s3.traverse(S3DAO.make[F])
      opensearchDAO <- configuration.opensearch.traverse(OpensearchDAO.make[F])
      sessionPool <- configuration.db.traverse(db.session[F])
      snapshotDAO = sessionPool.map(SnapshotDAO.make[F])
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

  private def fsGlobalIncrementalStorage[F[_] : Async : HasherSelector : Files : KryoSerializer](
                                                                                                  configuration: SnapshotStreamingConfig
                                                                                                ) =
    FileBasedLastGlobalIncrementalSnapshotStorage.make[F](configuration.lastIncrementalSnapshotPath)

  def make[F[_] : Async : Parallel : HasherSelector](
                                                      configuration: SnapshotStreamingConfig,
                                                      lastIncrementalGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
                                                      s3DAO: Option[S3DAO[F]],
                                                      snapshotDAO: Option[SnapshotDAO[F]],
                                                      opensearchDAO: Option[OpensearchDAO[F]],
                                                      globalMapper: GlobalSnapshotMapper[F],
                                                      currencyMapper: CurrencySnapshotMapper[F],
                                                      txHasher: Hasher[F],
                                                      tessellationServices: TessellationServices[F],
                                                      lastFullGlobalSnapshotStorage: FileBasedLastGlobalFullSnapshotStorage[F]
                                                    ): SnapshotProcessor[F] = new SnapshotProcessor[F] {
    private val logger = Slf4jLogger.getLogger[F]


    private def storeInPostgres(global: GlobalData, metagraph: MetagraphData) =
      snapshotDAO.traverse(dao =>
        dao.insertGlobalData(global, metagraph.snapshots.size) >> dao
          .insertMetagraphData(global.snapshot.hash, metagraph)
          .whenA(metagraph.snapshots.nonEmpty)
      ) >>
        logger
          .info(s"Snapshot ${global.snapshot.ordinal} (hash: ${global.snapshot.hash.show}) sent to postgres.")
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
      search(configuration.opensearch.get.indexes.snapshots)
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

    val runtime: Stream[F, Unit] = {
      val fOrdinal = for {
        incrementalOrdO <- lastIncrementalGlobalSnapshotStorage.getOrdinal.map(_.map(_.value.value))
        fullOrdinalO <- lastFullGlobalSnapshotStorage.get.map(_.map(_.value.ordinal.value.value))
      } yield incrementalOrdO.orElse(fullOrdinalO.orElse(Some(0L)))

      Stream
        .eval(fOrdinal)
        .flatMap { startAfterOrdinal =>
          opensearchDAO.get
            .bulkStream(searchGlobalSnapshots, hitMapper, cursorMapper, startAfterOrdinal)
            .parEvalMap(100) { case (h, ts) =>
              logger.info(s"Downloading ${h} from S3") >>
                s3DAO.get.downloadSnapshot(Hash(h)).flatMap { snapshot =>
                  implicit val hasher = HasherSelector[F].getForOrdinal(snapshot.ordinal)
                  snapshot.toHashed[F]
                }.map((h, _, ts))
            }
        }.chunkLimit(100)
        .evalMap { chunk =>
          val incrementalSnapshots = chunk.toList
          logger.info(s"Found ${incrementalSnapshots.size} global snapshots to process") >>
          lastIncrementalGlobalSnapshotStorage.getCombined.flatMap {
            case Some((lastSnapshot, lastState)) =>
              ProcessedSnapshots(lastSnapshot.signed, lastState, List.empty).pure
            case None =>
              lastFullGlobalSnapshotStorage.get.map {
                case Some(signedFullGlobalSnapshot) =>
                  val incSnapshot = incrementalSnapshots.head._2.signed
                  ProcessedSnapshots(incSnapshot, signedFullGlobalSnapshot.value.info, List.empty)
                case None =>
                  throw new Throwable(
                    s"Neither last processed snapshot nor initial snapshot were found on disk!"
                  )
              }
          }.flatMap { state =>
            incrementalSnapshots
              .foldM(state) { case (processedSnapshots, (gsHash, snapshot, dt)) =>
                logger.info(s"Processing global snapshot: ${getSnapshotReference(snapshot).show}")

                tessellationServices.globalSnapshotContextService
                  .createContext(
                    processedSnapshots.lastState,
                    processedSnapshots.lastSnapshot,
                    snapshot,
                    dt
                  )
                  .map { globalSnapshotsWithStateNew =>
                    val globalSnapshotsWithState = globalSnapshotsWithStateNew.copy(
                      snapshot = globalSnapshotsWithStateNew.snapshot.copy(hash = Hash(gsHash))
                    )
                    ProcessedSnapshots(
                      snapshot.signed,
                      globalSnapshotsWithState.snapshotInfo,
                      processedSnapshots.snapshotsWithState.appended(globalSnapshotsWithState)
                    )
                  }
              }
              .map(_.snapshotsWithState)
          }
        }
        .evalTap { snapshots =>
          snapshots.traverse { case GlobalSnapshotWithState(snapshot, _, _, _, _) =>
            logger.info(s"Pulled following global snapshot: ${getSnapshotReference(snapshot).show}")
          }
        }
        .evalMap { snapshots =>
          snapshots.parTraverse { case state@GlobalSnapshotWithState(snapshot, _, _, _, _) =>
            val hasher = HasherSelector[F].getForOrdinal(snapshot.ordinal)
            process(state, hasher).map(_ => state)
          }
        }
        .evalMap { snapshots =>
          val last = snapshots.maxBy(_.snapshot.ordinal.value.value)
          lastIncrementalGlobalSnapshotStorage.getCombined.flatMap {
            case None => lastIncrementalGlobalSnapshotStorage.setInitial(last.snapshot, last.snapshotInfo)
            case _ => lastIncrementalGlobalSnapshotStorage.set(last.snapshot, last.snapshotInfo)
          }

        }
    }
  }
}
