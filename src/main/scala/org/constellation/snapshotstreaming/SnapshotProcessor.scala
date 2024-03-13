package org.constellation.snapshotstreaming

import java.util.Date

import cats.Applicative
import cats.data.{NonEmptyList, NonEmptyMap, Validated}
import cats.effect._
import cats.effect.std.Random
import cats.syntax.applicative._
import cats.syntax.applicativeError._
import cats.syntax.either._
import cats.syntax.flatMap._
import cats.syntax.foldable._
import cats.syntax.functor._
import cats.syntax.option._
import cats.syntax.order._
import cats.syntax.show._
import cats.syntax.traverse._

import org.tessellation.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshot, CurrencySnapshotInfo}
import org.tessellation.ext.cats.syntax.next._
import org.tessellation.kryo.KryoSerializer
import org.tessellation.merkletree.StateProofValidator
import org.tessellation.node.shared.config.types.SnapshotSizeConfig
import org.tessellation.node.shared.domain.snapshot.Validator
import org.tessellation.node.shared.domain.snapshot.services.GlobalL0Service
import org.tessellation.node.shared.domain.snapshot.storage.LastSnapshotStorage
import org.tessellation.node.shared.http.p2p.clients.L0GlobalSnapshotClient
import org.tessellation.node.shared.infrastructure.cluster.storage.L0ClusterStorage
import org.tessellation.schema.SnapshotReference.{fromHashedSnapshot => getSnapshotReference}
import org.tessellation.schema.address.Address
import org.tessellation.schema.peer.{L0Peer, PeerId}
import org.tessellation.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo}
import org.tessellation.security._
import org.tessellation.security.signature.Signed

import com.sksamuel.elastic4s.ElasticDsl.bulk
import fs2.Stream
import org.constellation.snapshotstreaming.opensearch.{OpensearchDAO, UpdateRequestBuilder}
import org.constellation.snapshotstreaming.s3.S3DAO
import org.http4s.ember.client.EmberClientBuilder
import org.typelevel.log4cats.slf4j.Slf4jLogger

trait SnapshotProcessor[F[_]] {
  val runtime: Stream[F, Unit]
}

object SnapshotProcessor {

  def make[F[_]: Async: KryoSerializer: SecurityProvider: Random: Hasher](
    configuration: Configuration,
    hashSelect: HashSelect,
    snapshotSizeConfig: SnapshotSizeConfig
  ): Resource[F, SnapshotProcessor[F]] =
    for {
      client <- EmberClientBuilder
        .default[F]
        .withTimeout(configuration.httpClientTimeout)
        .withIdleTimeInPool(configuration.httpClientIdleTime)
        .build
      opensearchDAO <- OpensearchDAO.make[F](configuration.opensearchUrl)
      s3DAO <- S3DAO.make[F](configuration)
      globalSnapshotClient = L0GlobalSnapshotClient.make[F](client)
      l0ClusterStorage <- Resource.eval {
        Ref.of[F, NonEmptyMap[PeerId, L0Peer]](configuration.l0Peers).map(L0ClusterStorage.make(_))
      }
      lastIncrementalGlobalSnapshotStorage <- Resource.eval {
        FileBasedLastIncrementalGlobalSnapshotStorage.make[F](configuration.lastIncrementalSnapshotPath, hashSelect)
      }
      l0Service = GlobalL0Service
        .make[F](
          globalSnapshotClient,
          l0ClusterStorage,
          lastIncrementalGlobalSnapshotStorage,
          configuration.pullLimit.some,
          configuration.l0Peers.keys.some,
          hashSelect
        )
      requestBuilder = UpdateRequestBuilder.make[F](configuration)
      tesselationServices <- Resource.eval(TessellationServices.make[F](configuration, hashSelect, snapshotSizeConfig))
      lastFullGlobalSnapshotStorage = FileBasedLastFullGlobalSnapshotStorage.make[F](configuration.lastFullSnapshotPath)
    } yield make(
      configuration,
      lastIncrementalGlobalSnapshotStorage,
      l0Service,
      opensearchDAO,
      s3DAO,
      requestBuilder,
      tesselationServices,
      lastFullGlobalSnapshotStorage,
      hashSelect
    )

  def make[F[_]: Async: KryoSerializer: Hasher](
    configuration: Configuration,
    lastIncrementalGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    l0Service: GlobalL0Service[F],
    opensearchDAO: OpensearchDAO[F],
    s3DAO: S3DAO[F],
    updateRequestBuilder: UpdateRequestBuilder[F],
    tessellationServices: TessellationServices[F],
    lastFullGlobalSnapshotStorage: FileBasedLastFullGlobalSnapshotStorage[F],
    hashSelect: HashSelect
  ): SnapshotProcessor[F] = new SnapshotProcessor[F] {
    val logger = Slf4jLogger.getLogger[F]

    private def prepareAndExecuteBulkUpdate(
      globalSnapshotWithState: GlobalSnapshotWithState
    ): F[Unit] =
      globalSnapshotWithState.pure[F].flatMap {
        case state @ GlobalSnapshotWithState(snapshot, _, _) =>
          Clock[F].realTime
            .map(d => new Date(d.toMillis))
            .flatMap(updateRequestBuilder.bulkUpdateRequests(state, _))
            .flatMap(_.traverse(br => opensearchDAO.sendToOpensearch(bulk(br).refreshImmediately)))
            .flatMap(_ =>
              logger.info(
                s"Snapshot ${snapshot.ordinal.value.value} (hash: ${snapshot.hash.show.take(8)}) sent to opensearch."
              )
            )
            .void
      }

    private def process(globalSnapshotWithState: GlobalSnapshotWithState): F[Unit] =
      globalSnapshotWithState.pure[F].flatMap {
        case state @ GlobalSnapshotWithState(snapshot, snapshotInfo, _) =>
          StateProofValidator.validate(snapshot, snapshotInfo, hashSelect).flatMap {
            case Validated.Valid(()) =>
              lastIncrementalGlobalSnapshotStorage.get.flatMap {
                case Some(last) if Validator.isNextSnapshot(last, snapshot.signed.value) =>
                  s3DAO.uploadSnapshot(snapshot) >>
                    prepareAndExecuteBulkUpdate(state) >>
                    lastIncrementalGlobalSnapshotStorage.set(snapshot, snapshotInfo)

                case Some(last) =>
                  logger.warn(
                    s"Pulled snapshot doesn't form a correct chain, ignoring! Last: ${getSnapshotReference(last)} pulled: ${getSnapshotReference(snapshot)}"
                  )

                case None =>
                  lastFullGlobalSnapshotStorage.get.flatMap(_.traverse(_.toHashed)).flatMap {
                    case Some(last) if Validator.isNextSnapshot(last, snapshot.signed.value) =>
                      s3DAO.uploadSnapshot(snapshot) >>
                        prepareAndExecuteBulkUpdate(state) >>
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
                      )
                        .raiseError[F, Unit]
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
        .awakeEvery(configuration.pullInterval)
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
                        tessellationServices.globalSnapshotContextService.createContext(
                          processedSnapshots.lastState,
                          processedSnapshots.lastSnapshot,
                          snapshot
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
                    .map(_.map(nextSnapshot =>
                      GlobalSnapshotWithState(nextSnapshot, signedFullGlobalSnapshot.value.info, Map.empty)
                    ))
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
          snapshots.traverse { case GlobalSnapshotWithState(snapshot, _, _) =>
            logger.info(s"Pulled following global snapshot: ${getSnapshotReference(snapshot).show}")
          }
        }
        .evalMap {
          _.tailRecM {
            case (state @ GlobalSnapshotWithState(snapshot, _, _)) :: nextSnapshots
                if configuration.terminalSnapshotOrdinal.forall(snapshot.ordinal <= _) =>
              process(state).as {
                if (configuration.terminalSnapshotOrdinal.forall(snapshot.ordinal < _))
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
    snapshotInfo: GlobalSnapshotInfo,
    currencySnapshots: Map[Address, NonEmptyList[Either[Hashed[CurrencySnapshot], (Hashed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]]]
  )
  case class ProcessedSnapshots(
    lastSnapshot: Signed[GlobalIncrementalSnapshot],
    lastState: GlobalSnapshotInfo,
    snapshotsWithState: List[GlobalSnapshotWithState]
  )

}
