package org.constellation.snapshotstreaming

import java.util.Date

import cats.data.{NonEmptyList, NonEmptyMap, Validated}
import cats.effect._
import cats.effect.std.Random
import cats.effect.syntax.all._
import cats.syntax.all._
import cats.{Applicative, Parallel}

import io.constellationnetwork.currency.schema.currency.{
  CurrencyIncrementalSnapshot,
  CurrencySnapshot,
  CurrencySnapshotInfo
}
import io.constellationnetwork.ext.cats.syntax.next._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.merkletree.StateProofValidator
import io.constellationnetwork.node.shared.domain.snapshot.Validator
import io.constellationnetwork.node.shared.domain.snapshot.services.GlobalL0Service
import io.constellationnetwork.node.shared.domain.snapshot.storage.LastSnapshotStorage
import io.constellationnetwork.node.shared.http.p2p.clients.L0GlobalSnapshotClient
import io.constellationnetwork.node.shared.infrastructure.cluster.storage.L0ClusterStorage
import io.constellationnetwork.schema.SnapshotReference.{fromHashedSnapshot => getSnapshotReference}
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.peer.{L0Peer, PeerId}
import io.constellationnetwork.security._
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import com.sksamuel.elastic4s.ElasticDsl.bulk
import com.sksamuel.elastic4s.requests.update.UpdateRequest
import fs2.Stream
import org.constellation.snapshotstreaming.opensearch.mapper.{CurrencySnapshotMapper, GlobalSnapshotMapper}
import org.constellation.snapshotstreaming.opensearch.{OpensearchDAO, UpdateRequestBuilder}
import org.constellation.snapshotstreaming.s3.S3DAO
import org.constellation.snapshotstreaming.storage.{
  FileBasedLastGlobalFullSnapshotStorage,
  FileBasedLastGlobalIncrementalSnapshotStorage
}
import org.http4s.ember.client.EmberClientBuilder
import org.typelevel.log4cats.slf4j.Slf4jLogger

trait SnapshotProcessor[F[_]] {
  val runtime: Stream[F, Unit]
}

object SnapshotProcessor {

  def make[F[_]: Async: Parallel: KryoSerializer: JsonSerializer: SecurityProvider: Random: HasherSelector](
    configuration: Configuration,
    txHasher: Hasher[F]
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
        FileBasedLastGlobalIncrementalSnapshotStorage.make[F](configuration.lastIncrementalSnapshotPath)
      }
      l0Service = GlobalL0Service
        .make[F](
          globalSnapshotClient,
          l0ClusterStorage,
          lastIncrementalGlobalSnapshotStorage,
          configuration.pullLimit.some,
          configuration.l0Peers.keys.some
        )
      requestBuilder = UpdateRequestBuilder.make(
        GlobalSnapshotMapper.make(),
        CurrencySnapshotMapper.make(),
        configuration,
        txHasher: Hasher[F]
      )
      tesselationServices <- Resource.eval(
        TessellationServices.make[F](configuration)
      )
      lastFullGlobalSnapshotStorage = FileBasedLastGlobalFullSnapshotStorage.make[F, GlobalSnapshot](
        configuration.lastFullSnapshotPath
      )
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

  def make[F[_]: Async: Parallel: HasherSelector](
    configuration: Configuration,
    lastIncrementalGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    l0Service: GlobalL0Service[F],
    opensearchDAO: OpensearchDAO[F],
    s3DAO: S3DAO[F],
    updateRequestBuilder: UpdateRequestBuilder[F],
    tessellationServices: TessellationServices[F],
    lastFullGlobalSnapshotStorage: FileBasedLastGlobalFullSnapshotStorage[F]
  ): SnapshotProcessor[F] = new SnapshotProcessor[F] {
    val logger = Slf4jLogger.getLogger[F]

    private def logGroupedRequests(br: Seq[UpdateRequest], mode: String): F[Unit] = {
      val groupedBr = br.groupBy(_.index.index)
      groupedBr.toList.traverse_ { case (index, group) =>
        logger.info(s"Processing $mode group for index: $index with ${group.size} requests")
      }
    }

    private def prepareAndExecuteBulkUpdate(
      globalSnapshotWithState: GlobalSnapshotWithState,
      hasher: Hasher[F]
    ): F[Unit] =
      globalSnapshotWithState.pure[F].flatMap { case state @ GlobalSnapshotWithState(snapshot, _, _, _) =>
        Clock[F].realTime
          .map(d => new Date(d.toMillis))
          .flatMap(updateRequestBuilder.bulkUpdateRequests(state, _, hasher))
          .flatMap { requests =>
            for {
              _ <- logger.info("Starting to send parallel bulk updates to Opensearch")
              _ <- requests.parallelRequests.parTraverse { br =>
                logGroupedRequests(br, "parallel") >>
                  opensearchDAO.sendToOpensearch(bulk(br))
              }.timed.flatTap { case (elapsedTime, _) =>
                logger.info(s"Parallel bulk update operation took ${elapsedTime.toMillis} ms")
              }

              _ <- logger.info("Starting to send sequential bulk updates to Opensearch")
              _ <- requests.sequentialRequests.traverse { br =>
                logGroupedRequests(br, "sequential") >>
                  opensearchDAO.sendToOpensearch(bulk(br))
              }.timed.flatTap { case (elapsedTime, _) =>
                logger.info(s"Sequential bulk update operation took ${elapsedTime.toMillis} ms")
              }
            } yield ()
          }
          .flatMap(_ =>
            logger.info(
              s"Snapshot ${snapshot.ordinal.value.value} (hash: ${snapshot.hash.show.take(8)}) sent to opensearch."
            )
          )
          .void
      }

    private def process(globalSnapshotWithState: GlobalSnapshotWithState, hasher: Hasher[F]): F[Unit] =
      globalSnapshotWithState.pure[F].flatMap { case state @ GlobalSnapshotWithState(snapshot, _, snapshotInfo, _) =>
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
                  s3DAO.uploadSnapshot(snapshot, hasher.getLogic(snapshot.ordinal)) >>
                    prepareAndExecuteBulkUpdate(state, hasher) >>
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
                        s3DAO.uploadSnapshot(snapshot, hasher.getLogic(snapshot.ordinal)) >>
                          prepareAndExecuteBulkUpdate(state, hasher) >>
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
                      tessellationServices.globalSnapshotContextService
                        .createContext(
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
                    .map(
                      _.map(nextSnapshot =>
                        GlobalSnapshotWithState(nextSnapshot, None, signedFullGlobalSnapshot.value.info, Map.empty)
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
          snapshots.traverse { case GlobalSnapshotWithState(snapshot, _, _, _) =>
            logger.info(s"Pulled following global snapshot: ${getSnapshotReference(snapshot).show}")
          }
        }
        .evalMap {
          _.tailRecM {
            case (state @ GlobalSnapshotWithState(snapshot, _, _, _)) :: nextSnapshots
                if configuration.terminalSnapshotOrdinal.forall(snapshot.ordinal <= _) =>
              val hasher = HasherSelector[F].getForOrdinal(snapshot.ordinal)
              process(state, hasher).as {
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
    maybePrevSnapshotInfo: Option[GlobalSnapshotInfo],
    snapshotInfo: GlobalSnapshotInfo,
    currencySnapshots: Map[Address, NonEmptyList[
      Either[Hashed[
        CurrencySnapshot
      ], (Hashed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo, Signed[StateChannelSnapshotBinary])]
    ]]
  )

  case class ProcessedSnapshots(
    lastSnapshot: Signed[GlobalIncrementalSnapshot],
    lastState: GlobalSnapshotInfo,
    snapshotsWithState: List[GlobalSnapshotWithState]
  )

}
