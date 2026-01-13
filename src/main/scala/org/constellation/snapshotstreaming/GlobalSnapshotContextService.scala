package org.constellation.snapshotstreaming

import cats.Parallel
import cats.effect.kernel.Async
import cats.syntax.all._
import io.constellationnetwork.currency.schema.currency.{
  CurrencyIncrementalSnapshot,
  CurrencySnapshot,
  CurrencySnapshotInfo
}
import io.constellationnetwork.node.shared.domain.snapshot.services.GlobalL0Service
import io.constellationnetwork.node.shared.domain.snapshot.storage.{LastNGlobalSnapshotStorage, LastSnapshotStorage}
import io.constellationnetwork.node.shared.infrastructure.snapshot.GlobalSnapshotContextFunctions
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.GlobalSnapshotStateChannelEventsProcessor
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, HasherSelector}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary
import org.constellation.snapshotstreaming.SnapshotProcessor.GlobalSnapshotWithState
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.LocalDateTime

trait GlobalSnapshotContextService[F[_]] {

  def createContext(
    context: GlobalSnapshotInfo,
    lastArtifact: Signed[GlobalIncrementalSnapshot],
    artifact: Hashed[GlobalIncrementalSnapshot],
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]],
    dt: LocalDateTime
  ): F[GlobalSnapshotWithState]

}

object GlobalSnapshotContextService {

  def make[F[_]: Async: Parallel: HasherSelector](
    globalSnapshotStateChannelEventsProcessor: GlobalSnapshotStateChannelEventsProcessor[F],
    globalSnapshotContextFns: GlobalSnapshotContextFunctions[F],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F,GlobalIncrementalSnapshot, GlobalSnapshotInfo],
  ): GlobalSnapshotContextService[F] =
    new GlobalSnapshotContextService[F] {

      def createContext(
        context: GlobalSnapshotInfo,
        lastArtifact: Signed[GlobalIncrementalSnapshot],
        artifact: Hashed[GlobalIncrementalSnapshot],
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]],
        dt: LocalDateTime
      ): F[GlobalSnapshotWithState] =
        for {
          lastNGlobalSnapshots <- lastNGlobalSnapshotStorage.getLastN
          lastArtifactHashed <- HasherSelector[F].forOrdinal(artifact.ordinal) { implicit hasher =>
            lastArtifact.toHashed
          }
          _ <-
            if (lastNGlobalSnapshots.isEmpty) {
              lastNGlobalSnapshotStorage.setInitial(lastArtifactHashed, context) >>
                lastGlobalSnapshotStorage.setInitial(lastArtifactHashed, context)
            } else {
              ().pure
            }

          newContext <- HasherSelector[F].forOrdinal(artifact.ordinal) { implicit hasher =>
            globalSnapshotContextFns.createContext(
              context,
              lastArtifact,
              artifact.signed,
              getGlobalSnapshotByOrdinal
            )
          }
          reversedStateChannelSnapshots = artifact.signed.value.stateChannelSnapshots.map { case (address, snapshots) =>
            address -> snapshots.reverse
          }

          result <- HasherSelector[F].forOrdinal(artifact.ordinal) { implicit hasher =>
            globalSnapshotStateChannelEventsProcessor
              .processCurrencySnapshots(
                artifact.ordinal,
                context,
                reversedStateChannelSnapshots,
                getGlobalSnapshotByOrdinal
              )
              .flatMap { response =>
                response.mapFilter { case (snapshots, _) =>
                  snapshots.collect { case (binary, Some(currencySnapshotWithState)) =>
                    (binary, currencySnapshotWithState)
                  }.toNel
                }.traverse(_.traverse { case (binary, currencySnapshotWithState) =>
                  for {
                    result <- currencySnapshotWithState match {
                      case Left(full) =>
                        full.toHashed.map(
                          _.asLeft[
                            (
                              Hashed[CurrencyIncrementalSnapshot],
                              CurrencySnapshotInfo,
                              Signed[StateChannelSnapshotBinary]
                            )
                          ]
                        )
                      case Right((inc, info)) =>
                        inc.toHashed.map(hashed => (hashed, info, binary).asRight[Hashed[CurrencySnapshot]])
                    }
                  } yield result
                })
              }
              .map(GlobalSnapshotWithState(artifact, context.some, newContext, _, dt))
          }

          _ <- lastNGlobalSnapshotStorage.set(result.snapshot, result.snapshotInfo)
          _ <- lastGlobalSnapshotStorage.set(result.snapshot, result.snapshotInfo)
        } yield result

    }

}
