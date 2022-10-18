package org.constellation.snapshotstreaming

import java.util.Date

import cats.Applicative
import cats.data.NonEmptyMap
import cats.effect.std.Random
import cats.effect.{Async, Clock, Ref, Resource}
import cats.syntax.applicativeError._
import cats.syntax.either._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.option._
import cats.syntax.order._
import cats.syntax.show._
import cats.syntax.traverse._
import org.tessellation.sdk.domain.snapshot.services.L0Service
import org.tessellation.sdk.domain.snapshot.storage.LastGlobalSnapshotStorage
import org.tessellation.sdk.http.p2p.clients.L0GlobalSnapshotClient
import org.tessellation.sdk.domain.snapshot.Validator
import org.tessellation.dag.snapshot.GlobalSnapshot
import org.tessellation.dag.snapshot.GlobalSnapshotReference.{fromHashedGlobalSnapshot => getSnapshotReference}
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.peer.{L0Peer, PeerId}
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

  def make[F[_]: Async: KryoSerializer: SecurityProvider: Random](configuration: Configuration): Resource[F, SnapshotProcessor[F]] =
    for {
      client <- EmberClientBuilder
        .default[F]
        .withTimeout(configuration.httpClientTimeout)
        .withIdleTimeInPool(configuration.httpClientIdleTime)
        .build
      opensearchDAO <- OpensearchDAO.make[F](configuration.opensearchUrl)
      s3DAO <- S3DAO.make[F](configuration)
      l0GlobalSnapshotClient = L0GlobalSnapshotClient.make[F](client)
      l0ClusterStorage <- Resource.eval {
        Ref.of[F, NonEmptyMap[PeerId, L0Peer]](configuration.l0Peers).map(L0ClusterStorage.make(_))
      }
      lastSnapshotStorage <- Resource.eval {
        FileBasedLastGlobalSnapshotStorage.make[F](configuration.lastSnapshotPath)
      }
      l0Service = L0Service.make[F](l0GlobalSnapshotClient, l0ClusterStorage, lastSnapshotStorage, configuration.pullLimit.some)
      requestBuilder = UpdateRequestBuilder.make[F](configuration)
    } yield make(configuration, lastSnapshotStorage, l0Service, opensearchDAO, s3DAO, requestBuilder)

  def make[F[_]: Async](
    configuration: Configuration,
    lastGlobalSnapshotStorage: LastGlobalSnapshotStorage[F],
    l0Service: L0Service[F],
    opensearchDAO: OpensearchDAO[F],
    s3DAO: S3DAO[F],
    updateRequestBuilder: UpdateRequestBuilder[F]
  ): SnapshotProcessor[F] = new SnapshotProcessor[F] {

    val logger = Slf4jLogger.getLogger[F]

    private def prepareAndExecuteBulkUpdate(snapshot: Hashed[GlobalSnapshot]): F[Unit] =
      Clock[F].realTime
        .map(d => new Date(d.toMillis))
        .flatMap(updateRequestBuilder.bulkUpdateRequests(snapshot, _))
        .flatMap(_.traverse(br => opensearchDAO.sendToOpensearch(bulk(br).refreshImmediately)))
        .flatMap(_ => logger.info(s"Snapshot ${snapshot.ordinal.value.value} (hash: ${snapshot.hash.show.take(8)}) sent to opensearch."))
        .void

    private def process(snapshot: Hashed[GlobalSnapshot]): F[Unit] =
      lastGlobalSnapshotStorage.get.flatMap {
        case Some(last) if Validator.isNextSnapshot(last, snapshot) =>
            s3DAO.uploadSnapshot(snapshot) >>
              prepareAndExecuteBulkUpdate(snapshot) >>
              lastGlobalSnapshotStorage.set(snapshot)

        case Some(last) =>
          logger.warn(s"Pulled snapshot doesn't form a correct chain, ignoring! Last: ${getSnapshotReference(last)} pulled: ${getSnapshotReference(snapshot)}")

        case None if configuration.initialSnapshot.exists(initial => initial.hash === snapshot.hash && initial.ordinal === snapshot.ordinal) =>
            s3DAO.uploadSnapshot(snapshot) >>
              prepareAndExecuteBulkUpdate(snapshot) >>
              lastGlobalSnapshotStorage.setInitial(snapshot)

        case None =>
          new Throwable(s"Neither last processed snapshot nor initial snapshot were found during snapshot processing!").raiseError[F, Unit]

      }

    val runtime: Stream[F, Unit] =
      Stream
        .awakeEvery(configuration.pullInterval)
        .evalMap { _ =>
          lastGlobalSnapshotStorage.get.flatMap { last =>
            (last, configuration.initialSnapshot) match {
              case (Some(_), _) =>
                l0Service.pullGlobalSnapshots
              case (None, Some(initial)) =>
                l0Service.pullGlobalSnapshot(initial.ordinal)
                  .map(_.toList)
              case (None, None) =>
                new Throwable(s"Neither last processed snapshot nor initial snapshot were found during start!").raiseError[F, List[Hashed[GlobalSnapshot]]]
            }
          }
        }
        .evalTap {
          _.traverse { s =>
            logger.info(s"Pulled following global snapshot: ${getSnapshotReference(s).show}")
          }
        }
        .evalMap { snapshots =>
          snapshots.tailRecM {
            case snapshot :: nextSnapshots if configuration.terminalSnapshotOrdinal.forall(snapshot.ordinal <= _) =>
              process(snapshot)
                .as {
                  if (configuration.terminalSnapshotOrdinal.forall(snapshot.ordinal < _))
                    nextSnapshots.asLeft[Boolean]
                  else
                    false.asRight[List[Hashed[GlobalSnapshot]]]
                }
                .handleErrorWith { e =>
                  logger.warn(e)(s"Snapshot processing failed for ${getSnapshotReference(snapshot)}")
                    .as(true.asRight[List[Hashed[GlobalSnapshot]]])
                }

            case leftToProcess =>
              Applicative[F].pure(leftToProcess.isEmpty.asRight[List[Hashed[GlobalSnapshot]]])
          }
        }
        .takeWhile(identity)
        .as(())
  }
}
