package org.constellation.snapshotstreaming

import cats.Parallel
import cats.data.NonEmptyList
import cats.effect.kernel.Async
import cats.syntax.all._

import java.time.LocalDateTime

import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshot, CurrencySnapshotInfo}
import io.constellationnetwork.node.shared.domain.snapshot.storage.{LastNGlobalSnapshotStorage, LastSnapshotStorage}
import io.constellationnetwork.node.shared.infrastructure.snapshot.GlobalSnapshotContextFunctions
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.GlobalSnapshotStateChannelEventsProcessor
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, HasherSelector}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

/** Intermediate result from createContext - mptRoot is added by caller */
case class GlobalSnapshotContextResult(
  snapshot: Hashed[GlobalIncrementalSnapshot],
  maybePrevSnapshotInfo: Option[GlobalSnapshotInfo],
  snapshotInfo: GlobalSnapshotInfo,
  currencySnapshots: Map[Address, NonEmptyList[
    Either[Hashed[CurrencySnapshot], (Hashed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo, Signed[StateChannelSnapshotBinary])]
  ]],
  ts: LocalDateTime
)

trait GlobalSnapshotContextService[F[_]] {

  def createContext(
    context: GlobalSnapshotInfo,
    lastArtifact: Signed[GlobalIncrementalSnapshot],
    artifact: Hashed[GlobalIncrementalSnapshot],
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]],
    dt: LocalDateTime
  ): F[GlobalSnapshotContextResult]

}

object GlobalSnapshotContextService {

  def make[F[_]: Async: Parallel: HasherSelector](
    globalSnapshotStateChannelEventsProcessor: GlobalSnapshotStateChannelEventsProcessor[F],
    globalSnapshotContextFns: GlobalSnapshotContextFunctions[F],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo]
  ): GlobalSnapshotContextService[F] =
    new GlobalSnapshotContextService[F] {

      def createContext(
        context: GlobalSnapshotInfo,
        lastArtifact: Signed[GlobalIncrementalSnapshot],
        artifact: Hashed[GlobalIncrementalSnapshot],
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]],
        dt: LocalDateTime
      ): F[GlobalSnapshotContextResult] =
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
              ().pure[F]
            }

          newContext <- HasherSelector[F].forOrdinal(artifact.ordinal) { implicit hasher =>
            globalSnapshotContextFns.createContext(
              context,
              lastArtifact,
              artifact.signed,
              getGlobalSnapshotByOrdinal
            )
          }

          reversedStateChannelSnapshots = artifact.signed.value.stateChannelSnapshots.map {
            case (address, snapshots) => address -> snapshots.reverse
          }

          currencySnapshots <- HasherSelector[F].forOrdinal(artifact.ordinal) { implicit hasher =>
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
                  currencySnapshotWithState match {
                    case Left(full) =>
                      full.toHashed.map(
                        _.asLeft[(Hashed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo, Signed[StateChannelSnapshotBinary])]
                      )
                    case Right((inc, info)) =>
                      inc.toHashed.map(hashed => (hashed, info, binary).asRight[Hashed[CurrencySnapshot]])
                  }
                })
              }
          }

          result = GlobalSnapshotContextResult(
            artifact,
            context.some,
            newContext,
            currencySnapshots,
            dt
          )

          _ <- lastNGlobalSnapshotStorage.set(artifact, newContext)
          _ <- lastGlobalSnapshotStorage.set(artifact, newContext)
        } yield result

    }

}