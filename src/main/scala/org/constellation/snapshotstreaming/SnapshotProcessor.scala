package org.constellation.snapshotstreaming

import java.time.{Instant, LocalDateTime, ZoneId}
import cats.data.{NonEmptyList, Validated}
import cats.effect._
import cats.effect.implicits.clockOps
import cats.effect.std.{Console, Queue, Random}
import cats.syntax.all._
import cats.{Parallel}
import fs2.Stream
import fs2.io.file.{Files, Flags, Path}
import fs2.io.net.Network
import io.constellationnetwork.currency.schema.currency.{
  CurrencyIncrementalSnapshot,
  CurrencySnapshot,
  CurrencySnapshotInfo
}
import io.constellationnetwork.ext.cats.syntax.next._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.config.types.SharedConfigReader
import io.constellationnetwork.node.shared.domain.snapshot.Validator
import io.constellationnetwork.node.shared.domain.snapshot.services.GlobalL0Service
import io.constellationnetwork.node.shared.domain.snapshot.storage.LastSnapshotStorage
import io.constellationnetwork.node.shared.http.p2p.clients.L0GlobalSnapshotClient
import io.constellationnetwork.node.shared.infrastructure.cluster.storage.L0ClusterStorage
import io.constellationnetwork.schema.SnapshotReference.{fromHashedSnapshot => getSnapshotReference}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.{
  GlobalIncrementalSnapshot,
  GlobalSnapshot,
  GlobalSnapshotInfo,
  GlobalSnapshotInfoV2
}
import io.constellationnetwork.security._
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary
import io.constellationnetwork.merkletree.StateProofValidator
import org.constellation.snapshotstreaming.db.SnapshotDAO
import org.constellation.snapshotstreaming.mapper.{CurrencySnapshotMapper, GlobalSnapshotMapper}
import org.constellation.snapshotstreaming.opensearch.OpensearchDAO
import org.constellation.snapshotstreaming.s3.S3DAO
import org.constellation.snapshotstreaming.schema.schema.{GlobalData, MetagraphData}
import org.constellation.snapshotstreaming.storage.{
  FileBasedLastGlobalFullSnapshotStorage,
  FileBasedLastGlobalIncrementalSnapshotStorage,
  SnapshotWithState
}
import org.http4s.ember.client.EmberClientBuilder
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.otel4s.trace.Tracer

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
      s3DAO <- S3DAO.make[F](configuration.s3)
      opensearchDAO <- OpensearchDAO.make[F](configuration.opensearch)
      sessionPool <- db.session[F](configuration.db)
      snapshotDAO = SnapshotDAO.make[F](sessionPool)
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
        TessellationServices.make[F](configuration.environment, sharedConfig, l0Service)
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
      GlobalSnapshotMapper.make(Configuration.nodeSharedConfig(configuration.environment, sharedConfig)),
      CurrencySnapshotMapper.make(),
      txHasher,
      tesselationServices,
      lastFullGlobalSnapshotStorage
    )

  private def fsGlobalIncrementalStorage[F[_]: Async: Parallel: HasherSelector: Files: KryoSerializer](
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
    s3DAO: S3DAO[F],
    snapshotDAO: SnapshotDAO[F],
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
        .whenA(metagraph.snapshots.nonEmpty)).timed.flatMap { t =>
        logger
          .info(
            s"Snapshot ${global.snapshot.ordinal} (hash: ${global.snapshot.hash.show}) sent to postgres in ${t._1.toSeconds} s."
          ) >>
          logger
            .info(s"Metagraph Snapshots for currencies ${metagraph.snapshots.map(_.identifier)}  sent to postgres.")
            .handleErrorWith(s => logger.error(s)("Error in database layer") >> s.raiseError[F, Unit])
      }

    private def storeInS3(globalSnapshotWithState: GlobalSnapshotWithState, hasher: Hasher[F]) =
      s3DAO
        .uploadSnapshot(globalSnapshotWithState.snapshot, hasher.getLogic(globalSnapshotWithState.snapshot.ordinal))
        .void

    private def splitData(globalSnapshotWithState: GlobalSnapshotWithState, d: LocalDateTime, hasher: Hasher[F]) = (
      globalMapper.mapGlobalSnapshot(globalSnapshotWithState, d, txHasher, hasher),
      currencyMapper.mapCurrencySnapshots(globalSnapshotWithState, d, txHasher, hasher)
    ).tupled

    private def store(globalSnapshotWithState: GlobalSnapshotWithState, hasher: Hasher[F]): F[Unit] =
      storeInS3(globalSnapshotWithState, hasher).whenA(configuration.s3.uploadEnabled) >> Clock[F].realTime.map { d =>
        val instant = Instant.ofEpochMilli(d.toMillis)
        LocalDateTime.ofInstant(instant, ZoneId.systemDefault())
      }.flatMap(splitData(globalSnapshotWithState, _, hasher))
        .flatMap { case (globalData, metagraphData) =>
          Async[F].delay {
            if (metagraphData.snapshots.isEmpty && globalSnapshotWithState.currencySnapshots.nonEmpty)
              throw new Exception(s"No MG snapshots for ${globalSnapshotWithState.currencySnapshots}")
            else ()
          } >>
            storeInPostgres(globalData, metagraphData)
        }
        .void

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

    val runtime: Stream[F, Unit] = {
      for {
        queue <- Stream.eval(Queue.bounded[F, GlobalSnapshotWithState](configuration.node.pullLimit.value.toInt * 2))

        // Producer stream - pulls and processes snapshots
        producer = Stream
          .awakeEvery(configuration.node.pullInterval)
          .evalTap { _ =>
            queue.size.flatMap { size =>
              logger.info(s"Producer: Starting pull cycle. Pulling: ${configuration.node.pullLimit.value}. Current queue size: $size")
            }
          }
          .evalMap { _ =>
            lastIncrementalGlobalSnapshotStorage.getCombined.flatMap {
              case Some((lastSnapshot, lastState)) =>
                logger.info(s"Producer: Found last snapshot ${getSnapshotReference(lastSnapshot)}") >>
                  l0Service.pullGlobalSnapshots
                    .map(
                      _.leftMap(_ =>
                        new Throwable(s"Existence of last snapshot has been checked. It shouldn't happen!")
                      )
                    )
                    .flatMap(_.liftTo[F])
                    .flatMap { incrementalSnapshots =>
                      logger.info(s"Producer: Pulled ${incrementalSnapshots.size} snapshots") >>
                        incrementalSnapshots.foldM(ProcessedSnapshots(lastSnapshot.signed, lastState, List.empty)) {
                          (processedSnapshots, snapshot) =>
                            tessellationServices.globalSnapshotContextService
                              .createContext(
                                processedSnapshots.lastState,
                                processedSnapshots.lastSnapshot,
                                snapshot,
                                l0Service.pullGlobalSnapshot,
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
                    .handleErrorWith { e =>
                      logger.error(e)("Producer: Error pulling snapshots, will retry in next cycle") >>
                        List.empty[GlobalSnapshotWithState].pure[F]
                    }

              case None =>
                logger.info("Producer: No last snapshot found, checking full snapshot") >>
                  lastFullGlobalSnapshotStorage.get.flatMap {
                    case Some(signedFullGlobalSnapshot) =>
                      logger.info(s"Producer: Found full snapshot ${signedFullGlobalSnapshot.value.ordinal}") >>
                        l0Service
                          .pullGlobalSnapshot(signedFullGlobalSnapshot.value.ordinal.next)
                          .map(
                            _.map(nextSnapshot =>
                              GlobalSnapshotWithState(
                                nextSnapshot,
                                None,
                                signedFullGlobalSnapshot.value.info,
                                Map.empty,
                                LocalDateTime.now()
                              )
                            )
                          )
                          .map(_.toList)
                          .handleErrorWith { e =>
                            logger.error(e)("Producer: Error pulling initial snapshot, will retry in next cycle") >>
                              List.empty[GlobalSnapshotWithState].pure[F]
                          }
                    case None =>
                      logger.error("Producer: No snapshots found at all!") >>
                        List.empty[GlobalSnapshotWithState].pure[F]
                  }
            }
          }
          .evalTap { snapshots =>
            logger.info(s"Producer: Processing ${snapshots.size} snapshots") >>
              snapshots.traverse { case GlobalSnapshotWithState(snapshot, _, _, _, _) =>
                logger.info(s"Producer: Offering snapshot to queue: ${getSnapshotReference(snapshot).show}")
              }
          }
          .flatMap(Stream.emits)
          .evalTap { snapshot =>
            queue.offer(snapshot).flatMap { _ =>
              logger.info(s"Producer: Added snapshot to queue (offered ${getSnapshotReference(snapshot.snapshot)})")
            }
          }
          .drain

        // Consumer stream - stores snapshots
        consumer = Stream
          .fromQueueUnterminated(queue)
          .evalTap { snapshot =>
            queue.size.flatMap { size =>
              logger.info(
                s"Consumer: Starting to process snapshot ${getSnapshotReference(snapshot.snapshot)}. Queue size: $size"
              )
            }
          }
          .evalMap { snapshot =>
            val hasher = HasherSelector[F].getForOrdinal(snapshot.snapshot.ordinal)
            logger.info(s"Producer: Processing snapshot ${getSnapshotReference(snapshot.snapshot)}") >>
              process(snapshot, hasher).handleErrorWith { e =>
                  logger.error(e)(
                    s"Producer: Error processing snapshot ${getSnapshotReference(snapshot.snapshot)}, skipping"
                  ) >>
                    ().pure[F]
                }
                .as(snapshot)
          }
          .drain

        // Run all streams concurrently
        _ <- Stream(producer, consumer).parJoin(2)
      } yield ()
    }

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
