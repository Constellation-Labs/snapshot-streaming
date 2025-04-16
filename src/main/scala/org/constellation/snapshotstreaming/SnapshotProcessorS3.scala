package org.constellation.snapshotstreaming

import cats.data.{NonEmptyList, Validated}
import cats.effect._
import cats.effect.std.{Console, Random}
import cats.syntax.all._
import cats.Parallel
import cats.effect.implicits.clockOps
import com.sksamuel.elastic4s.ElasticApi.{fieldSort, matchAllQuery, search}
import com.sksamuel.elastic4s.requests.searches.SearchHit
import com.sksamuel.elastic4s.requests.update.UpdateRequest
import fs2.Stream
import fs2.io.file.{Files, Flags, Path}
import fs2.io.net.Network
import org.tessellation.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshot, CurrencySnapshotInfo}
import org.tessellation.ext.cats.syntax.next.catsSyntaxNext
import org.tessellation.json.JsonSerializer
import org.tessellation.kryo.KryoSerializer
import org.tessellation.merkletree.StateProofValidator
import org.tessellation.node.shared.config.types.SharedConfigReader
import org.tessellation.node.shared.domain.snapshot.storage.LastSnapshotStorage
import org.tessellation.node.shared.infrastructure.cluster.storage.L0ClusterStorage
import org.tessellation.schema.SnapshotReference.{fromHashedSnapshot => getSnapshotReference}
import org.tessellation.schema.address.Address
import org.tessellation.schema.{GlobalIncrementalSnapshot, GlobalSnapshot, GlobalSnapshotInfo, GlobalSnapshotInfoV2}
import org.tessellation.security._
import org.tessellation.security.hash.Hash
import org.tessellation.security.signature.Signed
import org.tessellation.statechannel.StateChannelSnapshotBinary
import org.constellation.snapshotstreaming.SnapshotProcessor.{GlobalSnapshotWithState, ProcessedSnapshots}
import org.constellation.snapshotstreaming.db.SnapshotDAO
import org.constellation.snapshotstreaming.mapper.{CurrencySnapshotMapper, GlobalSnapshotMapper}
import org.constellation.snapshotstreaming.opensearch.OpensearchDAO
import org.constellation.snapshotstreaming.schema.schema.{GlobalData, MetagraphData}
import org.constellation.snapshotstreaming.s3.S3DAO
import org.constellation.snapshotstreaming.storage.{FileBasedLastGlobalFullSnapshotStorage, FileBasedLastGlobalIncrementalSnapshotStorage, SnapshotWithState}
import org.http4s.ember.client.EmberClientBuilder
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.otel4s.trace.Tracer

import java.time.{Instant, LocalDateTime, ZoneId}

trait SnapshotProcessorS3[F[_]] {
  val runtime: Stream[F, Unit]
}

object SnapshotProcessorS3 {

  def make[F[
    _
  ]: Async: Parallel: KryoSerializer: JsonSerializer: SecurityProvider: Random: HasherSelector: Network: Files: Tracer: Console](
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

  private def fsGlobalIncrementalStorage[F[_]: Async: HasherSelector: Files: KryoSerializer](
    configuration: SnapshotStreamingConfig
  ) =
    FileBasedLastGlobalIncrementalSnapshotStorage.make[F](configuration.lastIncrementalSnapshotPath)

  def make[F[_]: Async: Parallel: HasherSelector](
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

    private def storeInPostgres(globalSnapshots: Seq[GlobalData], metagraphs: Seq[MetagraphData]) =
      snapshotDAO.traverse(dao =>
        dao.insertGlobalData(globalSnapshots.toList) >> dao
          .insertMetagraphData(metagraphs.toList)
          .whenA(metagraphs.nonEmpty)
      ) >>
        logger
          .debug(s"${globalSnapshots.size} sent to postgres.")
          .handleErrorWith(s => logger.error(s)("Error in database layer") >> s.raiseError[F, Unit])

    private def splitData(globalSnapshotWithState: GlobalSnapshotWithState) = {
      val hasher = HasherSelector[F].getForOrdinal(globalSnapshotWithState.snapshot.ordinal)
      (
        globalMapper.mapGlobalSnapshot(globalSnapshotWithState, hasher, txHasher).timed.flatMap { case (t, gs) =>
          logger.debug(s"Global snapshot mapped for ${globalSnapshotWithState.snapshot.hash} in ${t.toMillis} ms").map {
            _ => gs
          }
        },
        currencyMapper.mapCurrencySnapshots(globalSnapshotWithState, hasher, txHasher).timed.flatMap { case (t, gs) =>
          logger
            .debug(s"Currency snapshot mapped for ${globalSnapshotWithState.snapshot.hash} in ${t.toMillis} ms")
            .map(_ => gs)
        }
      ).tupled
    }

    private def store(globalSnapshotsWithState: Seq[GlobalSnapshotWithState]): F[Unit] =
      globalSnapshotsWithState
        .traverse(splitData)
        .map(_.unzip)
        .flatMap { case (globalDataSeq, metagraphDataSeq) =>
          storeInPostgres(globalDataSeq, metagraphDataSeq)
        }
        .void

    val searchGlobalSnapshots =
      search(configuration.opensearch.get.indexes.snapshots)
        .query(matchAllQuery())
        .sortBy(fieldSort("ordinal").asc())
        .sourceInclude("ordinal", "hash", "timestamp")

    def hitMapper(hit: SearchHit) = {
      val row = hit.sourceAsMap
      val hashO = row.get("hash").map(_.toString)
      val tsO = row
        .get("timestamp")
        .map { dateTimeString =>
          val instant = Instant.parse(dateTimeString.toString)
          LocalDateTime.ofInstant(instant, ZoneId.of("UTC"))
        }
      (hashO, tsO).tupled
    }

    def cursorMapper(hit: SearchHit) = hit.sourceAsMap.get("ordinal").map(_.toString.toLong)

    val reindexerConf = configuration.reindexer.get

    val runtime: Stream[F, Unit] =
      Stream
        .eval(lastFullGlobalSnapshotStorage.get.map(_.get))
        .evalMap(full => lastIncrementalGlobalSnapshotStorage.getCombined.map(inc => (full, inc)))
        .flatMap { case (signedFullGlobalSnapshot, hashedIncrementalCombinedO) =>
          val startAfterOrdinal = hashedIncrementalCombinedO
            .map(_._1.ordinal)
            .orElse(signedFullGlobalSnapshot.value.ordinal.some)
            .map(_.value.value)

          opensearchDAO.get
            .bulkStream(searchGlobalSnapshots, hitMapper, cursorMapper, startAfterOrdinal)
            .parEvalMap(reindexerConf.s3Parallelism) { case (h, ts) =>
              logger.info(s"Downloading hash ${h} from S3") >>
                s3DAO.get
                  .downloadSnapshot(Hash(h))
                  .timed
                  .flatMap { case (t, snapshot) =>
                    implicit val hasher = HasherSelector[F].getForOrdinal(snapshot.ordinal)
                    logger.info(s"Snapshot hash ${h} ordinal ${snapshot.ordinal} downloaded in ${t.toMillis} ms") >>
                      snapshot.toHashed[F]
                  }
                  .map((h, _, ts))
            }
            .evalMapAccumulate(hashedIncrementalCombinedO.map { case (lastSnapshot, lastState) =>
              ProcessedSnapshots(lastSnapshot.signed, lastState, List.empty)
            }) {
              case (None, (gsHash, snapshot, dt)) =>
                val gss = GlobalSnapshotWithState(
                  snapshot.copy(hash = Hash(gsHash)),
                  None,
                  signedFullGlobalSnapshot.value.info,
                  Map.empty,
                  dt
                )
                (Option(ProcessedSnapshots(snapshot.signed, signedFullGlobalSnapshot.value.info, List(gss))), gss).pure
              case (Some(processoStatus), (gsHash, snapshot, dt)) =>
                tessellationServices.globalSnapshotContextService
                  .createContext(
                    processoStatus.lastState,
                    processoStatus.lastSnapshot,
                    snapshot,
                    dt
                  )
                  .timed
                  .flatMap { case (t, newContext) =>
                    logger.info(s"$gsHash Context created in ${t.toMillis} ms").map(_ => newContext)
                  }
                  .map { newContext =>
                    val updatedSnapshot = newContext.snapshot
                    val updatedPprocessoStatus = processoStatus.copy(
                      lastSnapshot = updatedSnapshot.signed,
                      lastState = newContext.snapshotInfo,
                      List(newContext)
                    )
                    (Option(updatedPprocessoStatus), newContext)
                  }
            }
            .map(_._2)
            .evalTap { case GlobalSnapshotWithState(snapshot, _, _, _, _) =>
              logger.info(s"Pulled following global snapshot: ${getSnapshotReference(snapshot).show}")
            }
            .chunkN(reindexerConf.dbChunks)
            .parEvalMapUnordered(reindexerConf.dbParallelism) { states =>
              store(states.asSeq).timed.flatMap { case (t, _) =>
                logger.debug(s"Stored ${states.size} snapshots in ${t.toMillis} ms").map(_ => states)
              }
            }.unchunks.chunkN(reindexerConf.checkpointEvery)
            .evalMap { snapshots =>
              snapshots.last.traverse { last =>
                val snapshotWithState = SnapshotWithState(last.snapshot, last.snapshotInfo)
                val snapshotOrdinal = last.snapshot.ordinal.value.value
                FileBasedLastGlobalIncrementalSnapshotStorage.
                  saveSnapshotWithStateJson(Path(s"snapshotWithState.$snapshotOrdinal.json.gz"), snapshotWithState, Flags.Write) >>
                logger.info(s"Checkpoint at snapshot ordinal ${last.snapshot.ordinal} hash ${last.snapshot.hash} ")
              }
            }.void
        }

  }

}

