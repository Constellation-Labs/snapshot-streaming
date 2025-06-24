package org.constellation.snapshotstreaming

import cats.effect._
import cats.effect.std.{Console, Queue, Random}
import cats.syntax.all._
import cats.Parallel
import cats.data.Validated
import cats.effect.implicits.clockOps
import com.sksamuel.elastic4s.ElasticApi.{fieldSort, matchAllQuery, search, termQuery}
import com.sksamuel.elastic4s.requests.searches.SearchHit
import fs2.Stream
import fs2.io.file.Files
import fs2.io.net.Network
import io.circe.Decoder
import io.constellationnetwork.currency.schema.currency.CurrencyIncrementalSnapshot
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.merkletree.StateProofValidator
import io.constellationnetwork.node.shared.config.types.SharedConfigReader
import io.constellationnetwork.node.shared.domain.snapshot.services.GlobalL0Service
import io.constellationnetwork.node.shared.domain.snapshot.storage.LastSnapshotStorage
import io.constellationnetwork.node.shared.http.p2p.clients.L0GlobalSnapshotClient
import io.constellationnetwork.node.shared.infrastructure.cluster.storage.L0ClusterStorage
import io.constellationnetwork.schema.SnapshotReference.{fromHashedSnapshot => getSnapshotReference}
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshot, GlobalSnapshotInfo, GlobalSnapshotInfoV2, SnapshotOrdinal}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary
import org.constellation.snapshotstreaming.SnapshotProcessor.GlobalSnapshotWithState
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
      client <- makeClient(configuration.httpClient)
      s3DAO <- S3DAO.make[F](configuration.s3)
      opensearchDAO <- OpensearchDAO.make[F](configuration.opensearch)
      sessionPool <- db.session[F](configuration.db)
      snapshotDAO = SnapshotDAO.make[F](sessionPool)
      lastIncrementalGlobalSnapshotStorage <- Resource.eval(fsGlobalIncrementalStorage(configuration))
      globalSnapshotClient = L0GlobalSnapshotClient.make[F](client)
      l0ClusterStorage <- Resource.eval(L0ClusterStorageRef(configuration.node))
      l0Service = GlobalL0Service
        .make[F](
          globalSnapshotClient,
          l0ClusterStorage,
          lastIncrementalGlobalSnapshotStorage,
          configuration.node.pullLimit.some,
          configuration.node.l0PeersMap.keys.some
        )
      tesselationServices <- Resource.eval(
        TessellationServices.make[F](configuration.environment, sharedConfig, l0Service)
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
      lastFullGlobalSnapshotStorage
    )

  private def makeClient[F[_]: Async: Network](httpClientConfig: HttpClientConfig) =
    EmberClientBuilder
      .default[F]
      .withTimeout(httpClientConfig.timeout)
      .withIdleTimeInPool(httpClientConfig.idleTimeInPool)
      .build

  private def L0ClusterStorageRef[F[_]: Async: Random](nodeCfg: NodeConfig) =
    Ref.of(nodeCfg.l0PeersMap).map(L0ClusterStorage.make(_))

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

    def deserialize[A: Decoder](binary: Signed[StateChannelSnapshotBinary]): F[Option[A]] =
      jsonBrotliBinarySerializer.deserialize[A](binary.value.content).map(_.toOption)

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
            .evalMap { case (hash, hashedSnapshot, ts) =>
                  val reversedStateChannelSnapshots = hashedSnapshot.signed.value.stateChannelSnapshots.map {
                    case (address, snapshots) =>
                      address -> snapshots.reverse
                  }
                  val currencySnapshots = reversedStateChannelSnapshots.values.toList.flatTraverse(
                    _.traverse(deserialize[Signed[CurrencyIncrementalSnapshot]]).map(x => x.toList.flatten).flatMap(_.traverse { s =>
                      HasherSelector[F]
                        .forOrdinal(hashedSnapshot.ordinal) { implicit hasher =>
                          s.toHashed
                        }
                    }))
                  currencySnapshots.map(cs => (hashedSnapshot, cs))
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
//
//    val runtime: Stream[F, Unit] =
//      for {
//        queue <- Stream.eval(
//          Queue.bounded[F, (Hashed[GlobalIncrementalSnapshot], List[Hashed[CurrencyIncrementalSnapshot]])](
//            configuration.node.pullLimit.value.toInt * 2
//          )
//        )
//
//        incrementalCombined <- Stream.eval(lastIncrementalGlobalSnapshotStorage.getCombined)
//        initialState = incrementalCombined.map { case (hashedSnapshot, _) => hashedSnapshot.signed }
//        // Producer stream - pulls and processes snapshots
//        producer = Stream
//          .awakeEvery(configuration.node.pullInterval)
//          .evalTap { _ =>
//            queue.size.flatMap { size =>
//              logger.info(
//                s"Producer: Starting pull cycle. Pulling: ${configuration.node.pullLimit.value}. Current queue size: $size"
//              )
//            }
//          }
//          .evalMap(_ => lastIncrementalGlobalSnapshotStorage.getOrdinal)
//          .evalMap { lastSnapshot =>
//            val lastOrdinal = lastSnapshot.getOrElse(SnapshotOrdinal.MinValue)
//            l0Service
//              .pullGlobalSnapshots(lastOrdinal)
//              .map(
//                _.leftMap(_ => new Throwable(s"Existence of last snapshot has been checked. It shouldn't happen!"))
//              )
//              .flatMap(_.liftTo[F])
//              .flatMap { incrementalSnapshots =>
//                logger.info(s"Producer: Pulled ${incrementalSnapshots.size} snapshots") >>
//                  incrementalSnapshots.traverse { snapshot =>
//                    val reversedStateChannelSnapshots = snapshot.signed.value.stateChannelSnapshots.map {
//                      case (address, snapshots) =>
//                        address -> snapshots.reverse
//                    }
//                    val currencySnapshots = reversedStateChannelSnapshots.values.toList.flatTraverse(
//                      _.traverse(deserialize[Signed[CurrencyIncrementalSnapshot]]).map(x => x.toList.flatten).flatMap(_.traverse { s =>
//                        HasherSelector[F]
//                          .forOrdinal(snapshot.ordinal) { implicit hasher =>
//                            s.toHashed
//                          }
//                      })
//                    )
//
//                    val x = currencySnapshots.map(cs => (snapshot, cs))
//                    x
//                  }
//              }
//          }
//          .flatMap(Stream.emits)
//          .evalMap { snapshot =>
//            queue.offer(snapshot).flatMap { _ =>
//              logger.info(
//                s"Producer: Added snapshot to queue (offered ${getSnapshotReference(snapshot._1)})"
//              )
//            }
//
//          }
//          .drain
//
//        // Consumer stream - stores snapshots
//        consumer = Stream
//          .fromQueueUnterminated(queue)
//          .evalTap { case (snapshot, currencySnapshots) =>
//            queue.size.flatMap { size =>
//              logger.info(
//                s"Consumer: Starting to process snapshot ${getSnapshotReference(snapshot)}. Queue size: $size"
//              )
//            }
//          }
//          .evalMap { case (snapshot, currencySnapshots) =>
//            val hasher = HasherSelector[F].getForOrdinal(snapshot.ordinal)
//            logger.info(s"Consumer: Processing snapshot ${getSnapshotReference(snapshot)}") >>
//              retryF(
//                store(snapshot, currencySnapshots , hasher).timedLog(s"Consumer: processed snapshot ${snapshot.ordinal.value}")
//              ).handleErrorWith { e =>
//                logger.error(e)(
//                  s"Consumer: unrecoverable error processing snapshot ${getSnapshotReference(snapshot)}"
//                ) *> e.raiseError[F, Unit]
//              }.as(snapshot)
//          }
//          .drain
//
//        // Run all streams concurrently
//        _ <- Stream(producer, consumer).parJoin(2)
//      } yield ()
  }
}
