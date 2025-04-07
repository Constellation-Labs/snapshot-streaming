package org.constellation.snapshotstreaming

import cats.effect.kernel.Async
import cats.syntax.all._
import org.tessellation.currency.schema.currency.CurrencyIncrementalSnapshot
import org.tessellation.currency.schema.currency.CurrencySnapshot
import org.tessellation.currency.schema.currency.CurrencySnapshotInfo
import org.tessellation.node.shared.infrastructure.snapshot.GlobalSnapshotContextFunctions
import org.tessellation.node.shared.infrastructure.snapshot.GlobalSnapshotStateChannelEventsProcessor
import org.tessellation.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, SnapshotOrdinal}
import org.tessellation.security.signature.Signed
import org.tessellation.security.Hashed
import org.tessellation.security.HasherSelector
import org.constellation.snapshotstreaming.SnapshotProcessorS3.GlobalSnapshotWithState
import org.tessellation.statechannel.StateChannelSnapshotBinary

import java.time.LocalDateTime

trait GlobalSnapshotContextService[F[_]] {

  def createContext(
    context: GlobalSnapshotInfo,
    lastArtifact: Signed[GlobalIncrementalSnapshot],
    artifact: Hashed[GlobalIncrementalSnapshot],
    dt: LocalDateTime
  ): F[GlobalSnapshotWithState]

}

object GlobalSnapshotContextService {

  def make[F[_]: Async: HasherSelector](
    globalSnapshotStateChannelEventsProcessor: GlobalSnapshotStateChannelEventsProcessor[F],
    globalSnapshotContextFns: GlobalSnapshotContextFunctions[F]
  ): GlobalSnapshotContextService[F] =
    new GlobalSnapshotContextService[F] {

      def noOp(s: SnapshotOrdinal): F[Option[Hashed[GlobalIncrementalSnapshot]]] =
        none[Hashed[GlobalIncrementalSnapshot]].pure

      def createContext(
        context: GlobalSnapshotInfo,
        lastArtifact: Signed[GlobalIncrementalSnapshot],
        artifact: Hashed[GlobalIncrementalSnapshot],
        dt: LocalDateTime
      ): F[GlobalSnapshotWithState] =
        HasherSelector[F]
          .forOrdinal(artifact.ordinal) { implicit hasher =>
            globalSnapshotContextFns.createContext(context, lastArtifact, artifact.signed)
          }
          .flatMap { newContext =>
            HasherSelector[F].forOrdinal(artifact.ordinal) { implicit hasher =>
              // TODO: Instead of reversing here we should fix `allowedForProcessing` in acceptance manager so it preserves the order
              val reversedStateChannelSnapshots = artifact.signed.value.stateChannelSnapshots.map {
                case (address, snapshots) => address -> snapshots.reverse
              }
              val snapshotOrdinal = artifact.ordinal
              val lastGlobalSnapshotInfo = context
              val scSnapshots = reversedStateChannelSnapshots
              globalSnapshotStateChannelEventsProcessor
                .processCurrencySnapshots(
                  snapshotOrdinal,
                  lastGlobalSnapshotInfo,
                  scSnapshots
                )
                .flatMap {
                  _.mapFilter { case (snapshots, _) =>
                    snapshots.collect { case (binary, Some(currencySnapshotWithState)) =>
                      (binary, currencySnapshotWithState)
                    }.toNel
                  }.traverse(_.traverse { case (binary, currencySnapshotWithState) =>
                    currencySnapshotWithState match {
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
                  })
                }
                .map(GlobalSnapshotWithState(artifact, context.some, newContext, _, dt))
            }
          }

    }

}
