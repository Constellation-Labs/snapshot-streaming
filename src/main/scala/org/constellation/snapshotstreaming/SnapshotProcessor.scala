package org.constellation.snapshotstreaming

import java.time.{Instant, LocalDateTime, ZoneId}
import cats.data.{NonEmptyList, Validated}
import cats.effect._
import cats.effect.implicits.clockOps
import cats.effect.std.{Console, Queue, Random}
import cats.syntax.all._
import cats.Parallel
import fs2.Stream
import fs2.io.file.{Files, Flags, Path}
import fs2.io.net.Network
import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshot, CurrencySnapshotInfo}
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
import io.constellationnetwork.schema.{CurrencyStateProofSelector, GlobalIncrementalSnapshot, GlobalSnapshot, GlobalSnapshotInfo, GlobalSnapshotInfoV2, GlobalSnapshotStateProof, GlobalStateProofSelector, SnapshotOrdinal, StateProofSelector}
import io.constellationnetwork.security._
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary
import io.constellationnetwork.merkletree.StateProofValidator
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax.GlobalSnapshotInfoMptOps
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.MptRoot
import io.constellationnetwork.security.mpt.producer.FileSystemMerklePatriciaProducer
import org.constellation.snapshotstreaming.db.SnapshotDAO
import org.constellation.snapshotstreaming.mapper.{CurrencySnapshotMapper, GlobalSnapshotMapper}
import org.constellation.snapshotstreaming.opensearch.OpensearchDAO
import org.constellation.snapshotstreaming.s3.S3DAO
import org.constellation.snapshotstreaming.schema.schema.{GlobalData, MetagraphData}
import org.constellation.snapshotstreaming.storage.{FileBasedLastGlobalFullSnapshotStorage, FileBasedLastGlobalIncrementalSnapshotStorage, SnapshotWithState}
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
  )(
    implicit globalStateProofSelector: GlobalStateProofSelector,
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
      mptProducer <- Resource.eval(HasherSelector[F].withCurrent { implicit hasher => FileSystemMerklePatriciaProducer.make[F](sharedConfig.snapshot.mptSnapshotInfoPath)})
      mptStore <- Resource.eval(HasherSelector[F].withCurrent { implicit hasher =>
        MptStore.make[F, GlobalStateKey](
          mptProducer,
          GlobalStateKey.toHex[F]
        )
      })
      lastIncrementalGlobalSnapshotStorage <- Resource.eval(fsGlobalIncrementalStorage(configuration, mptStore))
      l0Service = GlobalL0Service
        .make[F](
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
      l0Service,
      s3DAO,
      snapshotDAO,
      GlobalSnapshotMapper.make(Configuration.nodeSharedConfig(configuration.environment, sharedConfig)),
      CurrencySnapshotMapper.make(),
      txHasher,
      tesselationServices,
      lastFullGlobalSnapshotStorage,
      mptStore
    )

  private def fsGlobalIncrementalStorage[F[_]: Async: Parallel: HasherSelector: Files: KryoSerializer](
    configuration: SnapshotStreamingConfig,
    mptStore: MptStore[F, GlobalStateKey]
  )(implicit stateProofSelector: GlobalStateProofSelector) =
    FileBasedLastGlobalIncrementalSnapshotStorage.make[F](configuration.lastIncrementalSnapshotPath, mptStore)

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
    lastFullGlobalSnapshotStorage: FileBasedLastGlobalFullSnapshotStorage[F],
    mptStore: MptStore[F, GlobalStateKey]
  )(implicit stateProofSelector: GlobalStateProofSelector): SnapshotProcessor[F] = new SnapshotProcessor[F] {
    private implicit val logger = Slf4jLogger.getLogger[F]

    private def logMemoryUsage(context: String): F[Unit] = Async[F].delay {
      val rt = Runtime.getRuntime
      val usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024
      val maxMB = rt.maxMemory() / 1024 / 1024
      (usedMB, maxMB)
    }.flatMap { case (usedMB, maxMB) =>
      logger.info(s"[MEMORY][$context] Used: ${usedMB}MB / Max: ${maxMB}MB (${(usedMB * 100) / maxMB}%)")
    }

    private def estimateSnapshotSize(gsws: GlobalSnapshotWithState): String = {
      val currencySnapshotsCount = gsws.currencySnapshots.values.map(_.size).sum
      val metagraphsCount = gsws.currencySnapshots.size
      s"metagraphs=$metagraphsCount, currencySnapshots=$currencySnapshotsCount, hasPrevInfo=${gsws.maybePrevSnapshotInfo.isDefined}"
    }

    private def storeInPostgres(global: GlobalData, metagraph: MetagraphData) =
      (snapshotDAO.insertGlobalData(global, metagraph.snapshots.size) >> snapshotDAO
        .insertMetagraphData(global.snapshot.hash, metagraph)
        .whenA(metagraph.snapshots.nonEmpty)).timed.flatMap { t =>
        logger.info(
          s"[POSTGRES] Snapshot ${global.snapshot.ordinal} (hash: ${global.snapshot.hash.show}) stored in ${t._1.toSeconds}s"
        ) >>
          logger.info(s"[POSTGRES] Metagraph snapshots for currencies ${metagraph.snapshots.map(_.identifier)}")
            .handleErrorWith(s => logger.error(s)("[POSTGRES] Error in database layer") >> s.raiseError[F, Unit])
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
            s"[PROCESS] Global Snapshot ${snapshot.ordinal.value.value} with logic=${hasher.getLogic(snapshot.ordinal)}"
          ) >>
            (hasher.getLogic(snapshot.ordinal) match {
              case JsonHash => StateProofValidator.validate(snapshot, snapshotInfo, mptStore)
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
                  s"[PROCESS] Pulled snapshot doesn't form a correct chain, ignoring! Last: ${getSnapshotReference(last)} pulled: ${getSnapshotReference(snapshot)}"
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
                          .onError(e => logger.error(e)(s"[PROCESS] Failure setting initial global snapshot!"))
                    case Some(last) =>
                      logger.warn(
                        s"[PROCESS] Pulled snapshot doesn't form a correct chain, ignoring! Last: ${getSnapshotReference(last)} pulled: ${getSnapshotReference(snapshot)}"
                      )
                    case None =>
                      new Throwable(
                        s"Neither last processed snapshot nor initial snapshot were found during snapshot processing!"
                      ).raiseError[F, Unit]
                  }
            }
          case Validated.Invalid(e) =>
            logger.warn(
              s"[PROCESS] Calculated stateProof does not match state from snapshot: ${e}."
            )
        }
    }

    val runtime: Stream[F, Unit] = {
      for {
        _ <- Stream.eval(logger.info("[INIT] Starting SnapshotProcessor runtime"))

        queueCapacity = configuration.node.pullLimit.value.toInt
        queue <- Stream.eval(Queue.bounded[F, GlobalSnapshotWithState](queueCapacity))
        _ <- Stream.eval(logger.info(s"[INIT] Queue created with capacity: $queueCapacity"))

        incrementalCombined <- Stream.eval(lastIncrementalGlobalSnapshotStorage.getCombined)
        initialState = incrementalCombined.map { case (hashedSnapshot, state) => (hashedSnapshot.signed, state) }

        _ <- Stream.eval(logMemoryUsage("INIT_START"))

        _ <- Stream.eval {
          initialState match {
            case Some((snapshot, state)) =>
              for {
                _ <- logger.info(s"[INIT] Found initial state at ordinal ${snapshot.ordinal}")
                kvPairs <- HasherSelector[F].withCurrent(implicit hasher => state.allStateEntries[F])
                _ <- logger.info(s"[INIT] Syncing ${kvPairs.keys.size} state entries")
                _ <- logMemoryUsage("BEFORE_SYNC")
                _ <- mptStore.syncFull(kvPairs, snapshot.ordinal)
                _ <- logMemoryUsage("AFTER_SYNC")
              } yield ()
            case None =>
              logger.info("[INIT] No initial state found") >> Async[F].unit
          }
        }

        producer = Stream
          .awakeEvery(configuration.node.pullInterval)
          .evalTap { _ =>
            queue.size.flatMap { size =>
              logger.info(
                s"[PRODUCER] Starting pull cycle. Pull limit: ${configuration.node.pullLimit.value}, Queue size: $size/$queueCapacity"
              ) >> logMemoryUsage("PRODUCER_CYCLE_START")
            }
          }
          .evalMapAccumulate(initialState) {
            case (Some((lastSnapshot, lastState)), _) =>
              l0Service
                .pullGlobalSnapshots(lastSnapshot.ordinal)
                .map(
                  _.leftMap(_ => new Throwable(s"Existence of last snapshot has been checked. It shouldn't happen!"))
                )
                .flatMap(_.liftTo[F])
                .flatMap { incrementalSnapshots =>
                  logger.info(s"[PRODUCER] Pulled ${incrementalSnapshots.size} snapshots from L0") >>
                    logMemoryUsage("AFTER_PULL") >>
                    incrementalSnapshots.foldM((lastSnapshot, lastState, 0)) {
                      case ((prevSnapshot, prevState, count), snapshot) =>
                        for {
                          _ <- logger.debug(s"[PRODUCER] Creating context for snapshot ${snapshot.ordinal}")
                          globalSnapshotsWithState <- tessellationServices.globalSnapshotContextService
                            .createContext(
                              prevState,
                              prevSnapshot,
                              snapshot,
                              l0Service.pullGlobalSnapshot,
                              LocalDateTime.now()
                            )
                          _ <- logger.info(
                            s"[PRODUCER] Context created for ${snapshot.ordinal}: ${estimateSnapshotSize(globalSnapshotsWithState)}"
                          )
                          _ <- validateMetagraphSnapshots(globalSnapshotsWithState)
                          _ <- queue.offer(globalSnapshotsWithState)
                          _ <- logger.info(s"[PRODUCER] Queued snapshot ${snapshot.ordinal}")
                          _ <- logMemoryUsage(s"AFTER_QUEUE_${snapshot.ordinal.value.value}")
                        } yield (snapshot.signed, globalSnapshotsWithState.snapshotInfo, count + 1)
                    }
                }
                .flatTap { case (_, _, count) =>
                  logger.info(s"[PRODUCER] Cycle complete. Processed and queued $count snapshots") >>
                    logMemoryUsage("PRODUCER_CYCLE_END")
                }
                .map { case (lastSnap, lastSt, _) =>
                  (Option((lastSnap, lastSt)), ())
                }
                // CRITICAL FIX: Handle errors gracefully - don't kill the stream
                .handleErrorWith { error =>
                  logger.warn(s"[PRODUCER] Pull cycle failed, will retry next interval: ${error.getMessage}") >>
                    logMemoryUsage("PRODUCER_ERROR_RECOVERY") >>
                    // Return the same state to retry on next cycle
                    (Option((lastSnapshot, lastState)), ()).pure[F]
                }

            case (None, _) =>
              logger.info("[PRODUCER] No last snapshot found, checking full snapshot") >>
                lastFullGlobalSnapshotStorage.get.flatMap {
                  case Some(signedFullGlobalSnapshot) =>
                    logger.info(s"[PRODUCER] Found full snapshot at ordinal ${signedFullGlobalSnapshot.value.ordinal}") >>
                      l0Service
                        .pullGlobalSnapshot(signedFullGlobalSnapshot.value.ordinal.next)
                        .flatMap(
                          _.map(nextSnapshot =>
                            GlobalSnapshotWithState(
                              nextSnapshot,
                              None,
                              signedFullGlobalSnapshot.value.info.toGlobalSnapshotInfo,
                              Map.empty,
                              LocalDateTime.now()
                            )
                          ).traverse { globalSnapshotsWithState =>
                            validateMetagraphSnapshots(globalSnapshotsWithState) >>
                              queue.offer(globalSnapshotsWithState) >>
                              logger.info(
                                s"[PRODUCER] Queued initial snapshot ${getSnapshotReference(globalSnapshotsWithState.snapshot)}"
                              ) >> globalSnapshotsWithState.pure
                          }
                        )
                        .map(s => (s.map(gsws => (gsws.snapshot.signed, gsws.snapshotInfo)), ()))
                        // CRITICAL FIX: Handle errors for initial snapshot pull too
                        .handleErrorWith { error =>
                          logger.warn(s"[PRODUCER] Initial pull failed, will retry: ${error.getMessage}") >>
                            (Option.empty[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)], ()).pure[F]
                        }

                  case None =>
                    logger.error("[PRODUCER] No snapshots found at all!") >>
                      (Option.empty[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)], ()).pure[F]
                }
          }
          .drain

        consumer = Stream
          .fromQueueUnterminated(queue)
          .evalTap { snapshot =>
            queue.size.flatMap { size =>
              logger.info(
                s"[CONSUMER] Dequeued snapshot ${snapshot.snapshot.ordinal.value.value}. Queue: $size/$queueCapacity remaining"
              ) >> logMemoryUsage("CONSUMER_DEQUEUE")
            }
          }
          .evalMap { snapshot =>
            val hasher = HasherSelector[F].getForOrdinal(snapshot.snapshot.ordinal)
            val ordinal = snapshot.snapshot.ordinal.value.value

            for {
              _ <- logger.info(s"[CONSUMER] Processing snapshot $ordinal (${estimateSnapshotSize(snapshot)})")
              startTime <- Clock[F].monotonic
              _ <- retryF(process(snapshot, hasher)).handleErrorWith { e =>
                logger.error(e)(s"[CONSUMER] Unrecoverable error processing snapshot $ordinal") *>
                  e.raiseError[F, Unit]
              }
              endTime <- Clock[F].monotonic
              duration = (endTime - startTime).toSeconds
              _ <- logger.info(s"[CONSUMER] Completed snapshot $ordinal in ${duration}s")
              _ <- logMemoryUsage("CONSUMER_COMPLETE")
            } yield ()
          }
          .drain

        _ <- Stream(producer, consumer).parJoin(2)
      } yield ()
    }

    private def validateMetagraphSnapshots(globalSnapshotsWithState: GlobalSnapshotWithState): F[Unit] = {
      val mgSnapshots = globalSnapshotsWithState.currencySnapshots.map(_._2.length).sum
      val channels = globalSnapshotsWithState.snapshot.stateChannelSnapshots.map(_._2.length).sum
      logger.debug(s"[VALIDATE] Metagraph snapshots: $mgSnapshots, State channels: $channels") >>
        Async[F]
          .raiseError(new RuntimeException(s"Metagraph ($mgSnapshots) and state channel ($channels) count don't match"))
          .unlessA(channels == mgSnapshots)
    }
  }
  case class GlobalSnapshotWithState(
    snapshot: Hashed[GlobalIncrementalSnapshot],
    maybePrevSnapshotInfo: Option[GlobalSnapshotInfo],
    snapshotInfo: GlobalSnapshotInfo,
    currencySnapshots: Map[Address, NonEmptyList[
      Either[Hashed[CurrencySnapshot], (Hashed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo, Signed[StateChannelSnapshotBinary])]
    ]],
    ts: LocalDateTime
  )
}