package org.constellation.snapshotstreaming

import cats.effect.kernel.Async
import cats.syntax.either._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._

import org.tessellation.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshot, CurrencySnapshotInfo}
import org.tessellation.kryo.KryoSerializer
import org.tessellation.node.shared.infrastructure.snapshot.{GlobalSnapshotContextFunctions, GlobalSnapshotStateChannelEventsProcessor}
import org.tessellation.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo}
import org.tessellation.security.signature.Signed
import org.tessellation.security.{Hashed, Hasher}

import org.constellation.snapshotstreaming.SnapshotProcessor.GlobalSnapshotWithState

trait GlobalSnapshotContextService[F[_]] {

  def createContext(context: GlobalSnapshotInfo, lastArtifact: Signed[GlobalIncrementalSnapshot], artifact: Hashed[GlobalIncrementalSnapshot]): F[GlobalSnapshotWithState]
}

object GlobalSnapshotContextService {

  def make[F[_]: Async: KryoSerializer: Hasher](
    globalSnapshotStateChannelEventsProcessor: GlobalSnapshotStateChannelEventsProcessor[F],
    globalSnapshotContextFns: GlobalSnapshotContextFunctions[F]
  ): GlobalSnapshotContextService[F] =
    new GlobalSnapshotContextService[F] {
      def createContext(context: GlobalSnapshotInfo, lastArtifact: Signed[GlobalIncrementalSnapshot], artifact: Hashed[GlobalIncrementalSnapshot]): F[GlobalSnapshotWithState] =
        globalSnapshotContextFns.createContext(context, lastArtifact, artifact.signed)
          .flatMap { newContext =>
            globalSnapshotStateChannelEventsProcessor
              .processCurrencySnapshots(context, artifact.signed.value.stateChannelSnapshots)
              .flatMap(_.traverse(_.traverse {
                case Left(full) => full.toHashed.map(_.asLeft[(Hashed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)])
                case Right((inc, info)) => inc.toHashed.map((_, info).asRight[Hashed[CurrencySnapshot]])
              }))
              .map(GlobalSnapshotWithState(artifact, newContext, _))
          }
    }
}
