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
  GlobalSnapshotInfoV2,
  SnapshotOrdinal
}
import io.constellationnetwork.security._
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary
import io.constellationnetwork.merkletree.StateProofValidator
import org.constellation.snapshotstreaming.db.SnapshotDAO
import org.constellation.snapshotstreaming.mapper.{CurrencySnapshotMapper, GlobalSnapshotMapper}
import org.constellation.snapshotstreaming.opensearch.OpensearchDAO
import org.constellation.snapshotstreaming.s3.S3DAO
import io.constellationnetwork.json.JsonBrotliBinarySerializer
import io.circe.Decoder
import org.constellation.snapshotstreaming.schema.schema.{GlobalData, MetagraphData}
import org.constellation.snapshotstreaming.storage.{
  FileBasedLastGlobalFullSnapshotStorage,
  FileBasedLastGlobalIncrementalSnapshotStorage,
  SnapshotWithState
}
import org.http4s.ember.client.EmberClientBuilder
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.otel4s.trace.Tracer

import scala.util.Try

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
      jsonBrotliBinarySerializer <- Resource.eval(JsonBrotliBinarySerializer.forSync[F])
    } yield make(
      configuration,
      lastIncrementalGlobalSnapshotStorage,
      l0Service,
      snapshotDAO,
      GlobalSnapshotMapper.make(Configuration.nodeSharedConfig(configuration.environment, sharedConfig)),
      CurrencySnapshotMapper.make(),
      txHasher,
      tesselationServices,
      lastFullGlobalSnapshotStorage,
      jsonBrotliBinarySerializer
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
    snapshotDAO: SnapshotDAO[F],
    globalMapper: GlobalSnapshotMapper[F],
    currencyMapper: CurrencySnapshotMapper[F],
    txHasher: Hasher[F],
    tessellationServices: TessellationServices[F],
    lastFullGlobalSnapshotStorage: FileBasedLastGlobalFullSnapshotStorage[F],
    jsonBrotliBinarySerializer: JsonBrotliBinarySerializer[F]
  ): SnapshotProcessor[F] = new SnapshotProcessor[F] {
    private implicit val logger = Slf4jLogger.getLogger[F]

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

    private def splitData(snapshot: Hashed[GlobalIncrementalSnapshot], ccys: List[Hashed[CurrencyIncrementalSnapshot]], d: LocalDateTime, hasher: Hasher[F]) = (
      globalMapper.mapGlobalSnapshot(snapshot, d, txHasher, hasher),
      currencyMapper.mapCurrencySnapshots(ccys, d, txHasher, hasher)
    ).tupled

    private def store(snapshot: Hashed[GlobalIncrementalSnapshot], ccys: List[Hashed[CurrencyIncrementalSnapshot]], hasher: Hasher[F]): F[Unit] =
      Clock[F].realTime.map { d =>
        val instant = Instant.ofEpochMilli(d.toMillis)
        LocalDateTime.ofInstant(instant, ZoneId.systemDefault())
      }.flatMap(splitData(snapshot, ccys, _, hasher))
        .flatMap { case (globalData, metagraphData) =>
          Async[F].delay {
            if (metagraphData.snapshots.isEmpty && snapshot.stateChannelSnapshots.nonEmpty)
              throw new Exception(s"No MG snapshots for ${snapshot.stateChannelSnapshots}")
            else ()
          } >>
            storeInPostgres(globalData, metagraphData)
        }
        .void

    def deserialize[A: Decoder](binary: Signed[StateChannelSnapshotBinary]): F[Option[A]] =
      jsonBrotliBinarySerializer.deserialize[A](binary.value.content).map(_.toOption)


    val runtime: Stream[F, Unit] =
      for {
        queue <- Stream.eval(
          Queue.bounded[F, (Hashed[GlobalIncrementalSnapshot], List[Hashed[CurrencyIncrementalSnapshot]])](
            configuration.node.pullLimit.value.toInt * 2
          )
        )

        incrementalCombined <- Stream.eval(lastIncrementalGlobalSnapshotStorage.getCombined)
        initialState = incrementalCombined.map { case (hashedSnapshot, _) => hashedSnapshot.signed }
        // Producer stream - pulls and processes snapshots
        producer = Stream
          .awakeEvery(configuration.node.pullInterval)
          .evalTap { _ =>
            queue.size.flatMap { size =>
              logger.info(
                s"Producer: Starting pull cycle. Pulling: ${configuration.node.pullLimit.value}. Current queue size: $size"
              )
            }
          }
          .evalMap(_ => lastIncrementalGlobalSnapshotStorage.getOrdinal)
          .evalMap { lastSnapshot =>
            val lastOrdinal = lastSnapshot.getOrElse(SnapshotOrdinal.MinValue)
            l0Service
              .pullGlobalSnapshots(lastOrdinal)
              .map(
                _.leftMap(_ => new Throwable(s"Existence of last snapshot has been checked. It shouldn't happen!"))
              )
              .flatMap(_.liftTo[F])
              .flatMap { incrementalSnapshots =>
                logger.info(s"Producer: Pulled ${incrementalSnapshots.size} snapshots") >>
                  incrementalSnapshots.traverse { snapshot =>
                    val reversedStateChannelSnapshots = snapshot.signed.value.stateChannelSnapshots.map {
                      case (address, snapshots) =>
                        address -> snapshots.reverse
                    }
                    val currencySnapshots = reversedStateChannelSnapshots.values.toList.flatTraverse(
                      _.traverse(deserialize[Signed[CurrencyIncrementalSnapshot]]).map(x => x.toList.flatten).flatMap(_.traverse { s =>
                        HasherSelector[F]
                          .forOrdinal(snapshot.ordinal) { implicit hasher =>
                            s.toHashed
                          }
                      })
                    )

                    val x = currencySnapshots.map(cs => (snapshot, cs))
                    x
                  }
              }
          }
          .flatMap(Stream.emits)
          .evalMap { snapshot =>
            queue.offer(snapshot).flatMap { _ =>
              logger.info(
                s"Producer: Added snapshot to queue (offered ${getSnapshotReference(snapshot._1)})"
              )
            }

          }
          .drain

        // Consumer stream - stores snapshots
        consumer = Stream
          .fromQueueUnterminated(queue)
          .evalTap { case (snapshot, currencySnapshots) =>
            queue.size.flatMap { size =>
              logger.info(
                s"Consumer: Starting to process snapshot ${getSnapshotReference(snapshot)}. Queue size: $size"
              )
            }
          }
          .evalMap { case (snapshot, currencySnapshots) =>
            val hasher = HasherSelector[F].getForOrdinal(snapshot.ordinal)
            logger.info(s"Consumer: Processing snapshot ${getSnapshotReference(snapshot)}") >>
              retryF(
                store(snapshot, currencySnapshots , hasher).timedLog(s"Consumer: processed snapshot ${snapshot.ordinal.value}")
              ).handleErrorWith { e =>
                logger.error(e)(
                  s"Consumer: unrecoverable error processing snapshot ${getSnapshotReference(snapshot)}"
                ) *> e.raiseError[F, Unit]
              }.as(snapshot)
          }
          .drain

        // Run all streams concurrently
        _ <- Stream(producer, consumer).parJoin(2)
      } yield ()

  }

  case class GlobalSnapshotWithState(
    snapshot: Hashed[GlobalIncrementalSnapshot],
    currencySnapshots: Map[Address, NonEmptyList[
      Either[Hashed[
        CurrencySnapshot
      ], (Hashed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo, Signed[StateChannelSnapshotBinary])]
    ]],
    ts: LocalDateTime
  )

}
