package org.constellation.snapshotstreaming

import cats.effect.kernel.Async
import cats.syntax.all._
import io.constellationnetwork.currency.schema.currency.CurrencyIncrementalSnapshot
import io.constellationnetwork.currency.schema.currency.CurrencySnapshot
import io.constellationnetwork.currency.schema.currency.CurrencySnapshotInfo
import io.constellationnetwork.node.shared.infrastructure.snapshot.GlobalSnapshotContextFunctions
import io.constellationnetwork.node.shared.infrastructure.snapshot.GlobalSnapshotStateChannelEventsProcessor
import io.constellationnetwork.schema.GlobalIncrementalSnapshot
import io.constellationnetwork.schema.GlobalSnapshotInfo
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.Hashed
import io.constellationnetwork.security.HasherSelector
import org.constellation.snapshotstreaming.SnapshotProcessor.GlobalSnapshotWithState
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

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

              globalSnapshotStateChannelEventsProcessor
                .processCurrencySnapshots(artifact.ordinal, context, reversedStateChannelSnapshots)
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
