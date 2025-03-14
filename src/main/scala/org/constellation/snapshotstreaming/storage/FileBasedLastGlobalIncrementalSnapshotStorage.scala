package org.constellation.snapshotstreaming.storage

import cats.Applicative
import cats.effect._
import cats.syntax.all._
import fs2.compression.Compression
import fs2.io.file._
import fs2.{Stream, text}
import io.circe.jawn
import io.circe.syntax._
import io.constellationnetwork.ext.kryo._
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.merkletree.StateProofValidator
import io.constellationnetwork.node.shared.domain.snapshot.storage.LastSnapshotStorage
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.height.Height
import io.constellationnetwork.schema.tokenLock.TokenLockOrdinal
import io.constellationnetwork.security._


object FileBasedLastGlobalIncrementalSnapshotStorage {

  TokenLockOrdinal.ordering$macro$9


  def make[F[_]: Async: HasherSelector: Files: KryoSerializer](
    path: Path
  ): F[LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo]] = {

    def deserializeWithJson(data: Array[Byte]) = jawn.decode[SnapshotWithState](new String(data, "UTF-8"))

    def deserializeWithKryoOrJson(data: Array[Byte]) =
      data.fromBinary[SnapshotWithState].handleErrorWith { e =>
        println(s"Failed to deserialize with kryo: ${e.getMessage}")
        jawn.decode[SnapshotWithState](new String(data, "UTF-8"))
      }

    val readSnapshotWithState: F[Option[SnapshotWithState]] =
      Files[F]
        .readAll(path)
        .through(Compression[F].gunzip())
        .flatMap(_.content)
        .compile
        .to(Array)
        .map(deserializeWithJson)
        .flatMap(_.liftTo[F])
        .map(_.some)
        .handleErrorWith {
          case _: NoSuchFileException => Applicative[F].pure(None)
          case e                      => e.raiseError[F, Option[SnapshotWithState]]
        }

    readSnapshotWithState.flatMap(Ref.of[F, Option[SnapshotWithState]](_).map(make(_, path)))
  }

  def make[F[_]: Async: HasherSelector: Files: KryoSerializer](
    cachedSnapshot: Ref[F, Option[SnapshotWithState]],
    path: Path
  ): LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo] =
    new LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo] {

      private def validateStateProof(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] =
        HasherSelector[F].forOrdinal(snapshot.ordinal) { implicit hasher =>
          (hasher.getLogic(snapshot.ordinal) match {
            case JsonHash => StateProofValidator.validate(snapshot, state)
            case KryoHash =>
              StateProofValidator.validate(snapshot, GlobalSnapshotInfoV2.fromGlobalSnapshotInfo(state))
          }).flatMap(Async[F].fromValidated)
        }

      def set(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] =
        validateStateProof(snapshot, state) >> {
          cachedSnapshot.get.flatMap { x =>
            x.map { _ =>
              val snapshotWithState = SnapshotWithState(snapshot, state)
              writeSnapshotWithStateJson(snapshotWithState) >> cachedSnapshot.set(Some(snapshotWithState))
            }.getOrElse(setInitial(snapshot, state))
          }
        }
//          get.flatMap {
//            case Some(last) if isNextSnapshot(last, snapshot.signed.value) =>
//              val snapshotWithState = SnapshotWithState(snapshot, state)
//              writeSnapshotWithStateKryo(snapshotWithState) >> cachedSnapshot.set(Some(snapshotWithState))
//            case Some(last) =>
//              MonadThrow[F].raiseError[Unit](
//                new IllegalStateException(
//                  s"Snapshot is not the next one! last: ${SnapshotReference
//                      .fromHashedSnapshot(last)}, lastHash: ${last.hash}, next: ${SnapshotReference
//                      .fromHashedSnapshot(snapshot)}, prevHash: ${snapshot.signed.value.lastSnapshotHash}"
//                )
//              )
//            case None =>
//              MonadThrow[F].raiseError[Unit](
//                new IllegalStateException("Previous snapshot not found when setting next global snapshot!")
//              )
//          }

      private def writeSnapshotWithStateJson(
        snapshotWithState: SnapshotWithState,
        flags: Flags = Flags(Flag.Write, Flag.Truncate)
      ) =
        Stream
          .emit(snapshotWithState.asJson.spaces2)
          .through(text.utf8.encode)
          .through(Compression[F].gzip())
          .through(Files[F].writeAll(path, flags))
          .compile
          .drain

      private def writeSnapshotWithStateKryo(
        snapshotWithState: SnapshotWithState,
        flags: Flags = Flags(Flag.Write, Flag.Truncate)
      ) =
        Stream
          .evalSeq(snapshotWithState.toBinaryF.map(_.toSeq))
          .through(Compression[F].gzip())
          .through(Files[F].writeAll(path, flags))
          .compile
          .drain

      def setInitial(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] = {
        val snapshotWithState = SnapshotWithState(snapshot, state)
        validateStateProof(snapshot, state) >> writeSnapshotWithStateJson(
          snapshotWithState,
          Flags(Flag.Write, Flag.CreateNew)
        ) >> cachedSnapshot.set(Some(snapshotWithState))
      }

      def get: F[Option[Hashed[GlobalIncrementalSnapshot]]] =
        cachedSnapshot.get.map(_.map(_.snapshot))

      def getCombined: F[Option[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]] =
        cachedSnapshot.get.map(_.map(sws => (sws.snapshot, sws.state)))

      def getCombinedStream: fs2.Stream[F, Option[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]] =
        Stream.eval(getCombined)

      def getOrdinal: F[Option[SnapshotOrdinal]] = get.map(_.map(_.ordinal))

      def getHeight: F[Option[Height]] = get.map(_.map(_.height))

    }

}
