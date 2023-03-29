package org.constellation.snapshotstreaming

import java.util.Date

import cats.Applicative
import cats.data.{NonEmptyMap, Validated}
import cats.effect._
import cats.effect.std.Random
import cats.syntax.applicativeError._
import cats.syntax.contravariantSemigroupal._
import cats.syntax.either._
import cats.syntax.flatMap._
import cats.syntax.foldable._
import cats.syntax.functor._
import cats.syntax.option._
import cats.syntax.order._
import cats.syntax.show._
import cats.syntax.traverse._

import org.tessellation.ext.cats.syntax.next._
import org.tessellation.kryo.KryoSerializer
import org.tessellation.merkletree.StateProofValidator
import org.tessellation.schema.SnapshotReference.{fromHashedSnapshot => getSnapshotReference}
import org.tessellation.schema.peer.{L0Peer, PeerId}
import org.tessellation.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo}
import org.tessellation.sdk.domain.snapshot.Validator
import org.tessellation.sdk.domain.snapshot.services.GlobalL0Service
import org.tessellation.sdk.domain.snapshot.storage.LastSnapshotStorage
import org.tessellation.sdk.http.p2p.clients.L0GlobalSnapshotClient
import org.tessellation.sdk.infrastructure.cluster.storage.L0ClusterStorage
import org.tessellation.security.{Hashed, SecurityProvider}

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

  def make[F[_]: Async: KryoSerializer: SecurityProvider: Random](
    configuration: Configuration
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
        FileBasedLastIncrementalGlobalSnapshotStorage.make[F](configuration.lastIncrementalSnapshotPath)
      }
      l0Service = GlobalL0Service
        .make[F](
          globalSnapshotClient,
          l0ClusterStorage,
          lastIncrementalGlobalSnapshotStorage,
          configuration.pullLimit.some
        )
      requestBuilder = UpdateRequestBuilder.make[F](configuration)
      tesselationServices = TessellationServices.make[F](configuration)
      lastFullGlobalSnapshotStorage = FileBasedLastFullGlobalSnapshotStorage.make[F](configuration.lastFullSnapshotPath)
    } yield make(
      configuration,
      lastIncrementalGlobalSnapshotStorage,
      l0Service,
      opensearchDAO,
      s3DAO,
      requestBuilder,
      tesselationServices,
      lastFullGlobalSnapshotStorage
    )

  def make[F[_]: Async: KryoSerializer](
    configuration: Configuration,
    lastIncrementalGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    l0Service: GlobalL0Service[F],
    opensearchDAO: OpensearchDAO[F],
    s3DAO: S3DAO[F],
    updateRequestBuilder: UpdateRequestBuilder[F],
    tessellationServices: TessellationServices[F],
    lastFullGlobalSnapshotStorage: FileBasedLastFullGlobalSnapshotStorage[F]
  ): SnapshotProcessor[F] = new SnapshotProcessor[F] {
    val logger = Slf4jLogger.getLogger[F]

    private def prepareAndExecuteBulkUpdate(
      snapshot: Hashed[GlobalIncrementalSnapshot],
      snapshotInfo: GlobalSnapshotInfo
    ): F[Unit] =
      Clock[F].realTime
        .map(d => new Date(d.toMillis))
        .flatMap(updateRequestBuilder.bulkUpdateRequests(snapshot, snapshotInfo, _))
        .flatMap(_.traverse(br => opensearchDAO.sendToOpensearch(bulk(br).refreshImmediately)))
        .flatMap(_ =>
          logger.info(
            s"Snapshot ${snapshot.ordinal.value.value} (hash: ${snapshot.hash.show.take(8)}) sent to opensearch."
          )
        )
        .void

    private def process(snapshot: Hashed[GlobalIncrementalSnapshot], snapshotInfo: GlobalSnapshotInfo): F[Unit] =
      StateProofValidator.validate(snapshot, snapshotInfo).flatMap {
        case Validated.Valid(()) =>
          lastIncrementalGlobalSnapshotStorage.get.flatMap {
            case Some(last) if Validator.isNextSnapshot(last, snapshot.signed.value) =>
              s3DAO.uploadSnapshot(snapshot) >>
                prepareAndExecuteBulkUpdate(snapshot, snapshotInfo) >>
                lastIncrementalGlobalSnapshotStorage.set(snapshot, snapshotInfo)

            case Some(last) =>
              logger.warn(
                s"Pulled snapshot doesn't form a correct chain, ignoring! Last: ${getSnapshotReference(last)} pulled: ${getSnapshotReference(snapshot)}"
              )

            case None =>
              lastFullGlobalSnapshotStorage.get.flatMap(_.traverse(_.toHashed)).flatMap {
                case Some(last) if Validator.isNextSnapshot(last, snapshot.signed.value) =>
                  s3DAO.uploadSnapshot(snapshot) >>
                    prepareAndExecuteBulkUpdate(snapshot, snapshotInfo) >>
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

    val runtime: Stream[F, Unit] =
      Stream
        .awakeEvery(configuration.pullInterval)
        .evalMap { _ =>
          lastIncrementalGlobalSnapshotStorage.getCombined.flatMap {
            case Some((lastSnapshot, lastState)) =>
              l0Service.pullGlobalSnapshots
                .map(
                  _.leftMap(_ => new Throwable(s"Existance of last snapshot has been checked. It shouldn't happen!"))
                )
                .flatMap(_.liftTo[F])
                .flatMap { incrementalSnapshots =>
                  incrementalSnapshots.foldM(ProcessedSnapshots(lastSnapshot.signed.value, lastState, List.empty)) {
                    (processedSnapshots, snapshot) =>
                      tessellationServices.globalSnapshotContextFns
                        .createContext(
                          processedSnapshots.lastState,
                          processedSnapshots.lastSnapshot,
                          snapshot.signed
                        )
                        .map { snapshotInfo =>
                          ProcessedSnapshots(
                            snapshot.signed.value,
                            snapshotInfo,
                            processedSnapshots.snapshotsWithState.appended((snapshot, snapshotInfo))
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
                    .flatMap(
                      _.traverse(nextSnapshot =>
                        GlobalIncrementalSnapshot
                          .fromGlobalSnapshot(signedFullGlobalSnapshot.value)
                          .flatMap(previousSnapshot =>
                            tessellationServices.globalSnapshotContextFns
                              .createContext(
                                signedFullGlobalSnapshot.value.info,
                                previousSnapshot,
                                nextSnapshot.signed
                              )
                              .map(nextSnapshotInfo => (nextSnapshot, nextSnapshotInfo))
                          )
                      )
                    )
                    .map(_.toList)
                case None =>
                  new Throwable(
                    s"Neither last processed snapshot nor initial snapshot were found during on disk!"
                  )
                    .raiseError[F, List[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]]
              }
          }
        }
        .evalTap { snapshots =>
          snapshots.traverse { s =>
            logger.info(s"Pulled following global snapshot: ${getSnapshotReference(s._1).show}")
          }
        }
        .evalMap {
          _.tailRecM {
            case (snapshot, snapshotInfo) :: nextSnapshots
                if configuration.terminalSnapshotOrdinal.forall(snapshot.ordinal <= _) =>
              process(snapshot, snapshotInfo).as {
                if (configuration.terminalSnapshotOrdinal.forall(snapshot.ordinal < _))
                  nextSnapshots.asLeft[Boolean]
                else
                  false.asRight[List[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]]
              }.handleErrorWith { e =>
                logger
                  .warn(e)(s"Snapshot processing failed for ${getSnapshotReference(snapshot)}")
                  .as(
                    true.asRight[List[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]]
                  )
              }
            case leftToProcess =>
              Applicative[F].pure(
                leftToProcess.isEmpty.asRight[List[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]]
              )
          }
        }
        .takeWhile(identity)
        .as(())

  }

  case class ProcessedSnapshots(
    lastSnapshot: GlobalIncrementalSnapshot,
    lastState: GlobalSnapshotInfo,
    snapshotsWithState: List[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]
  )

}
