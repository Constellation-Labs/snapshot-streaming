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
import io.constellationnetwork.node.shared.domain.snapshot.services.GlobalL0Service
import io.constellationnetwork.node.shared.domain.snapshot.storage.LastSnapshotStorage
import io.constellationnetwork.node.shared.http.p2p.clients.L0GlobalSnapshotClient
import io.constellationnetwork.node.shared.infrastructure.cluster.storage.L0ClusterStorage
import io.constellationnetwork.schema.SnapshotReference.{fromHashedSnapshot => getSnapshotReference}
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.{CurrencyStateProofSelector, GlobalIncrementalSnapshot, GlobalSnapshot, GlobalSnapshotInfo, GlobalSnapshotInfoV2, GlobalStateProofSelector, SnapshotOrdinal}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.producer.FileSystemMerklePatriciaProducer
import org.constellation.snapshotstreaming.SnapshotProcessor.{GlobalSnapshotWithState, L0ClusterStorageRef, ProcessedSnapshots, makeClient}
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
  )(implicit
    globalStateProofSelector: GlobalStateProofSelector,
    currencyStateProofSelector: CurrencyStateProofSelector
  ): Resource[F, SnapshotProcessor[F]] =
    for {
      client <- makeClient(configuration.httpClient)
      s3DAO <- S3DAO.make[F](configuration.s3)
      opensearchDAO <- OpensearchDAO.make[F](configuration.opensearch)
      sessionPool <- db.session[F](configuration.db)
      snapshotDAO = SnapshotDAO.make[F](sessionPool)
      globalSnapshotClient = L0GlobalSnapshotClient.make[F](client, None, sharedConfig.snapshot.timeouts)
      l0ClusterStorage <- Resource.eval(L0ClusterStorageRef(configuration.node))
      mptProducer <- Resource.eval(HasherSelector[F].withCurrent { implicit hasher =>
        FileSystemMerklePatriciaProducer.make[F](sharedConfig.snapshot.mptSnapshotInfoPath)
      })
      mptStore <- Resource.eval(HasherSelector[F].withCurrent { implicit hasher =>
        MptStore.make[F, GlobalStateKey](mptProducer, GlobalStateKey.toHex[F])
      })
      lastIncrementalGlobalSnapshotStorage <- Resource.eval(fsGlobalIncrementalStorage(configuration, mptStore))
      l0Service = GlobalL0Service.make[F](
        globalSnapshotClient,
        l0ClusterStorage,
        lastIncrementalGlobalSnapshotStorage,
        configuration.node.pullLimit.some,
        configuration.node.l0PeersMap.keys.some,
        mptStore
      )
      tesselationServices <- Resource.eval(
        TessellationServices.make[F](configuration.environment, sharedConfig, mptStore)
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
      GlobalSnapshotMapper.make(Configuration.nodeSharedConfig(configuration.environment, sharedConfig)),
      CurrencySnapshotMapper.make(),
      txHasher,
      tesselationServices,
      lastFullGlobalSnapshotStorage,
      mptStore
    )

  private def makeClient[F[_]: Async: Network](httpClientConfig: HttpClientConfig) =
    EmberClientBuilder
      .default[F]
      .withTimeout(httpClientConfig.timeout)
      .withIdleTimeInPool(httpClientConfig.idleTimeInPool)
      .build

  private def L0ClusterStorageRef[F[_]: Async: Random](nodeCfg: NodeConfig) =
    Ref.of(nodeCfg.l0PeersMap).map(L0ClusterStorage.make(_))

  private def fsGlobalIncrementalStorage[F[_]: Async: Parallel: HasherSelector: Files: KryoSerializer: JsonSerializer](
    configuration: SnapshotStreamingConfig,
    mptStore: MptStore[F, GlobalStateKey]
  )(implicit stateProofSelector: GlobalStateProofSelector) =
    FileBasedLastGlobalIncrementalSnapshotStorage.make[F](configuration.lastIncrementalSnapshotPath, mptStore)

  def make[F[_] : Async : Parallel : HasherSelector: JsonSerializer](
    configuration: SnapshotStreamingConfig,
    lastIncrementalGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    s3DAO: S3DAO[F],
    snapshotDAO: SnapshotDAO[F],
    opensearchDAO: OpensearchDAO[F],
    globalMapper: GlobalSnapshotMapper[F],
    currencyMapper: CurrencySnapshotMapper[F],
    txHasher: Hasher[F],
    tessellationServices: TessellationServices[F],
    lastFullGlobalSnapshotStorage: FileBasedLastGlobalFullSnapshotStorage[F],
    mptStore: MptStore[F, GlobalStateKey]
  )(implicit stateProofSelector: GlobalStateProofSelector): SnapshotProcessor[F] = new SnapshotProcessor[F] {
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
                val gss= GlobalSnapshotWithState(snapshot.copy(hash = Hash(gsHash)), None, signedFullGlobalSnapshot.value.info.toGlobalSnapshotInfo, Map.empty, dt)
                (Option(ProcessedSnapshots( snapshot.signed, signedFullGlobalSnapshot.value.info.toGlobalSnapshotInfo , List(gss))), gss).pure
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
