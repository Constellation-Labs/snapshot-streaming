package org.constellation.snapshotstreaming

import java.nio.file.NoSuchFileException

import cats.effect.Async
import cats.effect.std.Semaphore
import cats.syntax.applicative._
import cats.syntax.applicativeError._
import cats.syntax.either._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.option._
import cats.{Applicative, MonadThrow}

import org.tessellation.kryo.KryoSerializer
import org.tessellation.merkletree.StateProofValidator
import org.tessellation.node.shared.domain.snapshot.Validator.isNextSnapshot
import org.tessellation.node.shared.domain.snapshot.storage.LastSnapshotStorage
import org.tessellation.schema._
import org.tessellation.schema.height.Height
import org.tessellation.security.{HashSelect, Hashed, Hasher}

import fs2.io.file._
import fs2.{Stream, text}
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import io.circe.parser.decode
import io.circe.syntax._

object FileBasedLastIncrementalGlobalSnapshotStorage {

  def make[F[_]: Async: Files: KryoSerializer: Hasher](
    path: Path,
    hashSelect: HashSelect
  ): F[LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo]] =
    Semaphore[F](1)
      .map(make(path, _, hashSelect))

  private def make[F[_]: Async: Files: KryoSerializer: Hasher](
    path: Path,
    semaphore: Semaphore[F],
    hashSelect: HashSelect
  ): LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo] =
    new LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo] {

      implicit val codec: Codec[Hashed[GlobalIncrementalSnapshot]] = deriveCodec[Hashed[GlobalIncrementalSnapshot]]
      implicit val snapshotWithInfoCodec: Codec[SnapshotWithState] = deriveCodec[SnapshotWithState]

      def set(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] =
        semaphore.permit.use { _ =>
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
                  new Throwable(
                    s"Snapshot is not the next one! last: ${SnapshotReference
                        .fromHashedSnapshot(last)}, lastHash: ${last.hash}, next: ${SnapshotReference
                        .fromHashedSnapshot(snapshot)}, prevHash: ${snapshot.signed.value.lastSnapshotHash}"
                  )
                )
              case None =>
                MonadThrow[F].raiseError[Unit](
                  new Throwable("Previous snapshot not found when setting next global snapshot!")
                )
            }
        }

      def setInitial(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] =
        semaphore.permit.use { _ =>
          validateStateProof(snapshot, state) >>
            Stream
              .emit(SnapshotWithState(snapshot, state).asJson.spaces2)
              .through(text.utf8.encode)
              .through(Files[F].writeAll(path, Flags(Flag.Write, Flag.CreateNew)))
              .compile
              .drain
        }

      private def validateStateProof(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo) =
        StateProofValidator
          .validate(snapshot, state, hashSelect)
          .map(_.isValid)
          .flatMap(new Throwable("State proof doesn't match!").raiseError[F, Unit].unlessA)

      def get: F[Option[Hashed[GlobalIncrementalSnapshot]]] =
        getSnasphotWithState(_.snapshot)

      def getCombined: F[Option[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]] =
        getSnasphotWithState(sws => (sws.snapshot, sws.state))

      private def getSnasphotWithState[A](extract: SnapshotWithState => A): F[Option[A]] = Files[F]
        .readAll(path)
        .through(text.utf8.decode)
        .compile
        .toList
        .map(_.mkString)
        .map(decode[SnapshotWithState])
        .flatMap(_.liftTo[F])
        .map(extract)
        .map(_.some)
        .handleErrorWith {
          case _: NoSuchFileException => Applicative[F].pure(None)
          case e                      => e.raiseError[F, Option[A]]
        }

      def getOrdinal: F[Option[SnapshotOrdinal]] =
        get.map(_.map(_.ordinal))

      def getHeight: F[Option[Height]] =
        get.map(_.map(_.height))

    }

  case class SnapshotWithState(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)

}
