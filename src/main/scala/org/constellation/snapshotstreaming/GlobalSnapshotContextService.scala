package org.constellation.snapshotstreaming

import cats.Parallel
import cats.effect.kernel.Async
import cats.syntax.all._
import io.constellationnetwork.currency.schema.currency.CurrencyIncrementalSnapshot
import io.constellationnetwork.currency.schema.currency.CurrencySnapshot
import io.constellationnetwork.currency.schema.currency.CurrencySnapshotInfo
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.snapshot.services.GlobalL0Service
import io.constellationnetwork.node.shared.infrastructure.snapshot.GlobalSnapshotContextFunctions
import io.constellationnetwork.node.shared.infrastructure.snapshot.GlobalSnapshotStateChannelEventsProcessor
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, SnapshotOrdinal}
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
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
  ): F[GlobalSnapshotWithState]

}

object GlobalSnapshotContextService {
  var lastArtifactsHashed: List[Hashed[GlobalIncrementalSnapshot]] = List.empty[Hashed[GlobalIncrementalSnapshot]]
  val maxLastArtifacts = 10

  def make[F[_] : Async : Parallel : HasherSelector](
    globalSnapshotStateChannelEventsProcessor: GlobalSnapshotStateChannelEventsProcessor[F],
    globalSnapshotContextFns                 : GlobalSnapshotContextFunctions[F],
    l0Service                                : GlobalL0Service[F],
  ): GlobalSnapshotContextService[F] =
    new GlobalSnapshotContextService[F] {
      def fillLastArtifactsHashed(lastArtifact: Hashed[GlobalIncrementalSnapshot]): F[Unit] = {
        val ordinalsToFetch =
          (1 to maxLastArtifacts).map(lastArtifact.ordinal.value.value - _).toList

        ordinalsToFetch
          .parTraverse { ordinal =>
            l0Service.pullGlobalSnapshot(SnapshotOrdinal.unsafeApply(ordinal))
          }
          .map(_.flatten)
          .map(_.sortBy(_.ordinal.value.value))
          .flatMap { sortedArtifacts =>
            Async[F].delay {
              lastArtifactsHashed = sortedArtifacts
            }
          }
      }


      def createContext(
        context: GlobalSnapshotInfo,
        lastArtifact: Signed[GlobalIncrementalSnapshot],
        artifact: Hashed[GlobalIncrementalSnapshot],
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
      ):
      F[GlobalSnapshotWithState] = {
        for {
          lastArtifactHashed <- HasherSelector[F].forOrdinal(artifact.ordinal) { implicit hasher => lastArtifact.toHashed }
          _ <- if (lastArtifactsHashed.isEmpty) {
            fillLastArtifactsHashed(lastArtifactHashed)
          } else {
            Async[F].delay {
              lastArtifactsHashed = (lastArtifactsHashed :+ lastArtifactHashed).takeRight(maxLastArtifacts)
            }
          }

          newContext <- HasherSelector[F].forOrdinal(artifact.ordinal) { implicit hasher =>
            globalSnapshotContextFns.createContext(
              context,
              lastArtifact,
              artifact.signed,
              lastArtifactsHashed.some,
              getGlobalSnapshotByOrdinal
            )
          }
          reversedStateChannelSnapshots = artifact.signed.value.stateChannelSnapshots.map {
            case (address, snapshots) => address -> snapshots.reverse
          }

          result <- HasherSelector[F].forOrdinal(artifact.ordinal) { implicit hasher =>
            globalSnapshotStateChannelEventsProcessor
              .processCurrencySnapshots(
                artifact.ordinal,
                context,
                reversedStateChannelSnapshots,
                lastArtifactsHashed.some,
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
              .map(GlobalSnapshotWithState(artifact, context.some, newContext, _))
          }
        } yield result
      }
    }

}
