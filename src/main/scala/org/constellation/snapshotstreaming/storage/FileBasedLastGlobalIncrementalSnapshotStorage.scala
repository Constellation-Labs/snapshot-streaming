package org.constellation.snapshotstreaming.storage

import cats.effect.std.Mutex
import cats.effect.Async
import cats.syntax.all._
import cats.Applicative
import cats.MonadThrow
import org.tessellation.schema._
import org.tessellation.security._
import fs2.io.file._
import fs2.Stream
import fs2.text
import io.circe.syntax._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import io.circe.jawn
import org.tessellation.merkletree.StateProofValidator
import org.tessellation.node.shared.domain.snapshot.storage.LastSnapshotStorage
import org.tessellation.node.shared.domain.snapshot.Validator.isNextSnapshot
import org.tessellation.schema.height.Height

object FileBasedLastGlobalIncrementalSnapshotStorage {

  private case class SnapshotWithState(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)

  def make[F[_]: Async: HasherSelector: Files](path: Path): F[LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo]] =
    Mutex[F].map(make(_, path))

  def make[F[_]: Async: HasherSelector: Files](
    mutex: Mutex[F],
    path: Path
  ): LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo] =
    new LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo] {
      private implicit val codec: Codec[Hashed[GlobalIncrementalSnapshot]] = deriveCodec[Hashed[GlobalIncrementalSnapshot]]
      private implicit val snapshotWithInfoCodec: Codec[SnapshotWithState] = deriveCodec[SnapshotWithState]

      private def validateStateProof(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] =
        HasherSelector[F].forOrdinal(snapshot.ordinal) { implicit hasher =>
          (hasher.getLogic(snapshot.ordinal) match {
            case JsonHash => StateProofValidator.validate(snapshot, state)
            case KryoHash => StateProofValidator.validate(snapshot, GlobalSnapshotInfoV2.fromGlobalSnapshotInfo(state))
          }).flatMap(Async[F].fromValidated)
        }

      private def getSnapshotWithState[A](extract: SnapshotWithState => A): F[Option[A]] = Files[F]
        .readAll(path)
        .through(text.utf8.decode)
        .compile
        .toList
        .map(_.mkString)
        .map(jawn.decode[SnapshotWithState])
        .flatMap(_.liftTo[F])
        .map(extract)
        .map(_.some)
        .handleErrorWith {
          case _: NoSuchFileException => Applicative[F].pure(None)
          case e                      => e.raiseError[F, Option[A]]
        }

      def set(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] =
        mutex.lock.surround {
          validateStateProof(snapshot, state) >>
            get.flatMap {
              case Some(last) if isNextSnapshot(last, snapshot.signed.value) =>
                Stream
                  .emit(SnapshotWithState(snapshot, state).asJson.spaces2)
                  .through(text.utf8.encode)
                  .through(Files[F].writeAll(path))
                  .compile
                  .drain
              case Some(last) =>
                MonadThrow[F].raiseError[Unit](
                  new IllegalStateException(
                    s"Snapshot is not the next one! last: ${SnapshotReference
                        .fromHashedSnapshot(last)}, lastHash: ${last.hash}, next: ${SnapshotReference
                        .fromHashedSnapshot(snapshot)}, prevHash: ${snapshot.signed.value.lastSnapshotHash}"
                  )
                )
              case None =>
                MonadThrow[F].raiseError[Unit](
                  new IllegalStateException("Previous snapshot not found when setting next global snapshot!")
                )
            }
        }

      def setInitial(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] =
        mutex.lock.surround {
          validateStateProof(snapshot, state) >>
            Stream
              .emit(SnapshotWithState(snapshot, state).asJson.spaces2)
              .through(text.utf8.encode)
              .through(Files[F].writeAll(path, Flags(Flag.Write, Flag.CreateNew)))
              .compile
              .drain
        }

      def get: F[Option[Hashed[GlobalIncrementalSnapshot]]] =
        getSnapshotWithState(_.snapshot)

      def getCombined: F[Option[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]] =
        getSnapshotWithState(sws => (sws.snapshot, sws.state))

      def getCombinedStream: fs2.Stream[F, Option[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]] = ???

      def getOrdinal: F[Option[SnapshotOrdinal]] = get.map(_.map(_.ordinal))

      def getHeight: F[Option[Height]] = get.map(_.map(_.height))

    }

}
