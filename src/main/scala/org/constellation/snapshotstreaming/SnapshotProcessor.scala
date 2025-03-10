package org.constellation.snapshotstreaming

import java.util.Date
import cats.{Applicative, Parallel}
import cats.data.NonEmptyList
import cats.data.NonEmptyMap
import cats.data.Validated
import cats.effect._
import cats.effect.std.{Console, Random}
import cats.effect.syntax.all._
import cats.syntax.all._
import org.tessellation.currency.schema.currency.CurrencyIncrementalSnapshot
import org.tessellation.currency.schema.currency.CurrencySnapshot
import org.tessellation.currency.schema.currency.CurrencySnapshotInfo
import org.tessellation.ext.cats.syntax.next._
import org.tessellation.kryo.KryoSerializer
import org.tessellation.json.JsonSerializer
import org.tessellation.merkletree.StateProofValidator
import org.tessellation.node.shared.domain.snapshot.Validator
import org.tessellation.node.shared.domain.snapshot.services.GlobalL0Service
import org.tessellation.node.shared.domain.snapshot.storage.LastSnapshotStorage
import org.tessellation.node.shared.http.p2p.clients.L0GlobalSnapshotClient
import org.tessellation.node.shared.infrastructure.cluster.storage.L0ClusterStorage
import org.tessellation.schema.SnapshotReference.{fromHashedSnapshot => getSnapshotReference}
import org.tessellation.schema.address.Address
import org.tessellation.schema.peer.L0Peer
import org.tessellation.schema.peer.PeerId
import org.tessellation.schema.GlobalIncrementalSnapshot
import org.tessellation.schema.GlobalSnapshotInfo
import org.tessellation.schema.GlobalSnapshotInfoV2
import org.tessellation.security._
import org.tessellation.security.signature.Signed
import com.sksamuel.elastic4s.ElasticDsl.bulk
import com.sksamuel.elastic4s.requests.update.UpdateRequest
import fs2.Stream
import fs2.io.file.Files
import fs2.io.net.Network
import org.constellation.snapshotstreaming.db.SnapshotDAO
import org.constellation.snapshotstreaming.mapper.{CurrencySnapshotMapper, GlobalSnapshotMapper}
import org.constellation.snapshotstreaming.opensearch.OpensearchDAO
import org.constellation.snapshotstreaming.opensearch.UpdateRequestBuilder
import org.constellation.snapshotstreaming.mapper.CurrencySnapshotMapper
import org.constellation.snapshotstreaming.mapper.GlobalSnapshotMapper
import org.constellation.snapshotstreaming.s3.S3DAO
import org.constellation.snapshotstreaming.schema.schema.{GlobalData, MetagraphData}
import org.constellation.snapshotstreaming.storage.FileBasedLastGlobalFullSnapshotStorage
import org.constellation.snapshotstreaming.storage.FileBasedLastGlobalIncrementalSnapshotStorage
import org.http4s.ember.client.EmberClientBuilder
import org.tessellation.node.shared.config.types.SharedConfigReader
import org.tessellation.schema.GlobalSnapshot
import org.tessellation.statechannel.StateChannelSnapshotBinary
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.otel4s.trace.Tracer

import java.time.{Instant, LocalDateTime, ZoneId}

trait SnapshotProcessor[F[_]] {
  val runtime: Stream[F, Unit]
}

object SnapshotProcessor {

  def make[F[
    _
  ]: Async: Parallel: KryoSerializer: JsonSerializer: SecurityProvider: Random: HasherSelector: Network: Files: Tracer: Console](
    configuration: SnapshotStreamingConfig,
    sharedConfig: SharedConfigReader,
    txHasher: Hasher[F]
  ): Resource[F, SnapshotProcessor[F]] =
    for {
      client <- makeClient(configuration.httpClient)
      s3DAO <- configuration.s3.traverse(S3DAO.make[F])
      opensearchDAO <- configuration.opensearch.traverse(OpensearchDAO.make[F])
      sessionPool <- configuration.db.traverse(db.session[F])
      snapshotDAO = sessionPool.map(SnapshotDAO.make[F])
      globalSnapshotClient = L0GlobalSnapshotClient.make[F](client)
      l0ClusterStorage <- Resource.eval(L0ClusterStorageRef(configuration.node))
      lastIncrementalGlobalSnapshotStorage <- Resource.eval(fsGlobalIncrementalStorage(configuration))
      l0Service = GlobalL0Service
        .make[F](
          globalSnapshotClient,
          l0ClusterStorage,
          lastIncrementalGlobalSnapshotStorage,
          configuration.node.pullLimit.some,
          configuration.node.l0PeersMap.keys.some
        )
      tesselationServices <- Resource.eval(
        TessellationServices.make[F](configuration.environment, sharedConfig)
      )
      lastFullGlobalSnapshotStorage = FileBasedLastGlobalFullSnapshotStorage.make[F, GlobalSnapshot](
        configuration.lastSnapshotPath
      )
    } yield make(
      configuration,
      lastIncrementalGlobalSnapshotStorage,
      l0Service,
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

  private def L0ClusterStorageRef[F[_]: Async: Random](nodeCfg: NodeConfig) =
    Ref.of(nodeCfg.l0PeersMap).map(L0ClusterStorage.make(_))

  private def makeClient[F[_]: Async: Network](httpClientConfig: HttpClientConfig) =
    EmberClientBuilder
      .default[F]
      .withTimeout(httpClientConfig.timeout)
      .withIdleTimeInPool(httpClientConfig.idleTimeInPool)
      .build

  def make[F[_]: Async: Parallel: HasherSelector](
    configuration: SnapshotStreamingConfig,
    lastIncrementalGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    l0Service: GlobalL0Service[F],
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

    private def logGroupedRequests(br: Seq[UpdateRequest], mode: String): F[Unit] = {
      val groupedBr = br.groupBy(_.index.index)
      groupedBr.toList.traverse_ { case (index, group) =>
        logger.info(s"Processing $mode group for index: $index with ${group.size} requests")
      }
    }

    private val oRquestBuilder = configuration.opensearch.map(UpdateRequestBuilder.make)

    private def uploadToOpenSearch(global: GlobalData, metagraph: MetagraphData): F[Unit] =
      oRquestBuilder
        .map(_.bulkUpdateRequests(global, metagraph))
        .traverse { requests =>
          logger.info("Starting to send parallel bulk updates to Opensearch") >>
            requests.parallelRequests.parTraverse { br =>
              logGroupedRequests(br, "parallel") >>
                opensearchDAO.traverse(_.sendToOpensearch(bulk(br)))
            }.timed.flatTap { case (elapsedTime, _) =>
              logger.info(s"Parallel bulk update operation took ${elapsedTime.toMillis} ms")
            } >>
            logger.info("Starting to send sequential bulk updates to Opensearch") >>
            requests.sequentialRequests.traverse { br =>
              logGroupedRequests(br, "sequential") >>
                opensearchDAO.traverse(_.sendToOpensearch(bulk(br)))
            }.timed.flatTap { case (elapsedTime, _) =>
              logger.info(s"Sequential bulk update operation took ${elapsedTime.toMillis} ms")
            } >> logger.info(
              s"Snapshot ${global.snapshot.ordinal} (hash: ${global.snapshot.hash.show.take(8)}) sent to opensearch."
            )
        }
        .void

    private def storeInPostgres(global: GlobalData, metagraph: MetagraphData) =
      snapshotDAO.traverse(dao =>
        dao.insertGlobalData(global, metagraph.snapshots.size) >> dao
          .insertMetagraphData(global.snapshot.hash, metagraph)
          .whenA(metagraph.snapshots.nonEmpty)
      ) >>
        logger
          .info(s"Snapshot ${global.snapshot.ordinal} (hash: ${global.snapshot.hash.show}) sent to postgres.")
          .handleErrorWith(s => logger.error(s)("Error in database layer") >> s.raiseError[F, Unit])

    private def storeInS3(globalSnapshotWithState: GlobalSnapshotWithState) =
      s3DAO.traverse(_.uploadSnapshot(globalSnapshotWithState.snapshot)).void

    private def splitData(globalSnapshotWithState: GlobalSnapshotWithState, d: LocalDateTime, hasher: Hasher[F]) = (
      globalMapper.mapGlobalSnapshot(globalSnapshotWithState, d, hasher, txHasher),
      currencyMapper.mapCurrencySnapshots(globalSnapshotWithState, d, hasher, txHasher)
    ).tupled

    private def store(globalSnapshotWithState: GlobalSnapshotWithState, hasher: Hasher[F]): F[Unit] =
      storeInS3(globalSnapshotWithState) >> Clock[F].realTime.map { d =>
        val instant = Instant.ofEpochMilli(d.toMillis)
        LocalDateTime.ofInstant(instant, ZoneId.systemDefault())
      }.flatMap(splitData(globalSnapshotWithState, _, hasher)).flatMap { case (globalData, metagraphData) =>
        storeInPostgres(globalData, metagraphData) >> uploadToOpenSearch(globalData, metagraphData)
      }.void

    private def process(globalSnapshotWithState: GlobalSnapshotWithState, hasher: Hasher[F]): F[Unit] = {
      val GlobalSnapshotWithState(snapshot, _, snapshotInfo, _, _) = globalSnapshotWithState
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
            lastIncrementalGlobalSnapshotStorage.get.flatMap {
              case Some(last) if Validator.isNextSnapshot(last, snapshot.signed.value) =>
                store(globalSnapshotWithState, hasher) >>
                  lastIncrementalGlobalSnapshotStorage.set(snapshot, snapshotInfo)
              case Some(last) =>
                logger.warn(
                  s"Pulled snapshot doesn't form a correct chain, ignoring! Last: ${getSnapshotReference(last)} pulled: ${getSnapshotReference(snapshot)}"
                )
              case None =>
                lastFullGlobalSnapshotStorage.get
                  .flatMap(_.traverse { snapshot =>
                    HasherSelector[F].forOrdinal(snapshot.ordinal)(implicit hasher => snapshot.toHashed)
                  })
                  .flatMap {
                    case Some(last) if Validator.isNextSnapshot(last, snapshot.signed.value) =>
                      store(globalSnapshotWithState, hasher) >>
                        lastIncrementalGlobalSnapshotStorage
                          .setInitial(snapshot, snapshotInfo)
                          .onError(e => logger.error(e)(s"Failure setting initial global snapshot!"))
                    case Some(last) =>
                      logger.warn(
                        s"Pulled snapshot doesn't form a correct chain, ignoring! Last: ${getSnapshotReference(last)} pulled: ${getSnapshotReference(snapshot)}"
                      )
                    case None =>
                      new Throwable(
                        s"Neither last processed snapshot nor initial snapshot were found during snapshot processing!"
                      ).raiseError[F, Unit]
                  }
            }
          case Validated.Invalid(e) =>
            logger.warn(
              s"Calculated stateProof does not match state from snapshot: ${e}."
            )
        }
    }

    val runtime: Stream[F, Unit] =
      Stream
        .awakeEvery(configuration.node.pullInterval)
        .evalMap { _ =>
          lastIncrementalGlobalSnapshotStorage.getCombined.flatMap {
            case Some((lastSnapshot, lastState)) =>
              l0Service.pullGlobalSnapshots
                .map(
                  _.leftMap(_ => new Throwable(s"Existence of last snapshot has been checked. It shouldn't happen!"))
                )
                .flatMap(_.liftTo[F])
                .flatMap { incrementalSnapshots =>
                  incrementalSnapshots.foldM(ProcessedSnapshots(lastSnapshot.signed, lastState, List.empty)) {
                    (processedSnapshots, snapshot) =>
                      tessellationServices.globalSnapshotContextService
                        .createContext(
                          processedSnapshots.lastState,
                          processedSnapshots.lastSnapshot,
                          snapshot,
                          LocalDateTime.now()
                        )
                        .map { globalSnapshotsWithState =>
                          ProcessedSnapshots(
                            snapshot.signed,
                            globalSnapshotsWithState.snapshotInfo,
                            processedSnapshots.snapshotsWithState.appended(globalSnapshotsWithState)
                          )
                        }
                  }
                }
                .map(_.snapshotsWithState)

            case None =>
              lastFullGlobalSnapshotStorage.get.flatMap {
                case Some(signedFullGlobalSnapshot) =>
                  l0Service
                    .pullGlobalSnapshot(signedFullGlobalSnapshot.value.ordinal.next)
                    .map(
                      _.map(nextSnapshot =>
                        GlobalSnapshotWithState(nextSnapshot, None, signedFullGlobalSnapshot.value.info, Map.empty, LocalDateTime.now())
                      )
                    )
                    .map(_.toList)
                case None =>
                  new Throwable(
                    s"Neither last processed snapshot nor initial snapshot were found on disk!"
                  )
                    .raiseError[F, List[GlobalSnapshotWithState]]
              }
          }
        }
        .evalTap { snapshots =>
          snapshots.traverse { case GlobalSnapshotWithState(snapshot, _, _, _, _) =>
            logger.info(s"Pulled following global snapshot: ${getSnapshotReference(snapshot).show}")
          }
        }
        .evalMap {
          _.tailRecM {
            case (state @ GlobalSnapshotWithState(snapshot, _, _, _,_)) :: nextSnapshots
                if configuration.node.terminalSnapshotOrdinal.forall(snapshot.ordinal <= _) =>
              val hasher = HasherSelector[F].getForOrdinal(snapshot.ordinal)
              process(state, hasher).as {
                if (configuration.node.terminalSnapshotOrdinal.forall(snapshot.ordinal < _))
                  nextSnapshots.asLeft[Boolean]
                else
                  false.asRight[List[GlobalSnapshotWithState]]
              }.handleErrorWith { e =>
                logger
                  .warn(e)(s"Snapshot processing failed for ${getSnapshotReference(snapshot)}")
                  .as(
                    true.asRight[List[GlobalSnapshotWithState]]
                  )
              }
            case leftToProcess =>
              Applicative[F].pure(
                leftToProcess.isEmpty.asRight[List[GlobalSnapshotWithState]]
              )
          }
        }
        .takeWhile(identity)
        .as(())

  }

  case class GlobalSnapshotWithState(
    snapshot: Hashed[GlobalIncrementalSnapshot],
    maybePrevSnapshotInfo: Option[GlobalSnapshotInfo],
    snapshotInfo: GlobalSnapshotInfo,
    currencySnapshots: Map[Address, NonEmptyList[
      Either[Hashed[
        CurrencySnapshot
      ], (Hashed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo, Signed[StateChannelSnapshotBinary])]
    ]],
    ts: LocalDateTime
  )

  case class ProcessedSnapshots(
    lastSnapshot: Signed[GlobalIncrementalSnapshot],
    lastState: GlobalSnapshotInfo,
    snapshotsWithState: List[GlobalSnapshotWithState]
  )

}
