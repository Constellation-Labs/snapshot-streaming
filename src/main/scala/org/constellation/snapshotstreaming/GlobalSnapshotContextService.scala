package org.constellation.snapshotstreaming

import cats.effect.kernel.Async
import cats.syntax.all._
import org.tessellation.currency.schema.currency.CurrencyIncrementalSnapshot
import org.tessellation.currency.schema.currency.CurrencySnapshot
import org.tessellation.currency.schema.currency.CurrencySnapshotInfo
import org.tessellation.kryo.KryoSerializer
import org.tessellation.node.shared.infrastructure.snapshot.GlobalSnapshotContextFunctions
import org.tessellation.node.shared.infrastructure.snapshot.GlobalSnapshotStateChannelEventsProcessor
import org.tessellation.schema.GlobalIncrementalSnapshot
import org.tessellation.schema.GlobalSnapshotInfo
import org.tessellation.security.signature.Signed
import org.tessellation.security.Hashed
import org.tessellation.security.HasherSelector
import org.constellation.snapshotstreaming.SnapshotProcessor.GlobalSnapshotWithState
import org.tessellation.statechannel.StateChannelSnapshotBinary

trait GlobalSnapshotContextService[F[_]] {

  def createContext(
    context: GlobalSnapshotInfo,
    lastArtifact: Signed[GlobalIncrementalSnapshot],
    artifact: Hashed[GlobalIncrementalSnapshot]
  ): F[GlobalSnapshotWithState]

}

object GlobalSnapshotContextService {

  def make[F[_]: Async: KryoSerializer: HasherSelector](
    globalSnapshotStateChannelEventsProcessor: GlobalSnapshotStateChannelEventsProcessor[F],
    globalSnapshotContextFns: GlobalSnapshotContextFunctions[F]
  ): GlobalSnapshotContextService[F] =
    new GlobalSnapshotContextService[F] {

      def createContext(
        context: GlobalSnapshotInfo,
        lastArtifact: Signed[GlobalIncrementalSnapshot],
        artifact: Hashed[GlobalIncrementalSnapshot]
      ): F[GlobalSnapshotWithState] =
        HasherSelector[F]
          .forOrdinal(artifact.ordinal) { implicit hasher =>
            globalSnapshotContextFns.createContext(context, lastArtifact, artifact.signed)
          }
          .flatMap { newContext =>
            HasherSelector[F].forOrdinal(artifact.ordinal) { implicit hasher =>
              globalSnapshotStateChannelEventsProcessor
                .processCurrencySnapshots(artifact.ordinal, context, artifact.signed.value.stateChannelSnapshots)
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
                .map(GlobalSnapshotWithState(artifact, context.some, newContext, _))
            }
          }

    }

}
