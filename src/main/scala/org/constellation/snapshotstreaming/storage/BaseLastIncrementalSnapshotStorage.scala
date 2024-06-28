package org.constellation.snapshotstreaming.storage

import java.nio.file.NoSuchFileException
import cats.effect.Async
import cats.syntax.all._
import cats.Applicative
import cats.MonadThrow
import cats.effect.std.Mutex
import org.tessellation.node.shared.domain.snapshot.Validator.isNextSnapshot
import org.tessellation.node.shared.domain.snapshot.storage.LastSnapshotStorage
import org.tessellation.schema._
import org.tessellation.schema.height.Height
import org.tessellation.security._
import fs2.io.file._
import fs2.Stream
import fs2.text
import io.circe.Codec
import io.circe.parser.decode
import io.circe.syntax._
import org.tessellation.schema.snapshot.IncrementalSnapshot
import org.tessellation.schema.snapshot.SnapshotInfo
import org.tessellation.schema.snapshot.StateProof

abstract class BaseLastIncrementalSnapshotStorage[F[_]: Async, P <: StateProof, S <: IncrementalSnapshot[P], SI <: SnapshotInfo[P]](mutex: Mutex[F], path: Path)(implicit codec: Codec[SnapshotWithState[S, SI]]) extends LastSnapshotStorage[F, S, SI] {

  protected def validateStateProof(snapshot: Hashed[S], state: SI): F[Unit]

  def set(snapshot: Hashed[S], state: SI): F[Unit] =
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

  def setInitial(snapshot: Hashed[S], state: SI): F[Unit] =
    mutex.lock.surround {
      validateStateProof(snapshot, state) >>
        Stream
          .emit(SnapshotWithState(snapshot, state).asJson.spaces2)
          .through(text.utf8.encode)
          .through(Files[F].writeAll(path, Flags(Flag.Write, Flag.CreateNew)))
          .compile
          .drain
    }

  def get: F[Option[Hashed[S]]] =
    getSnapshotWithState(_.snapshot)

  def getCombined: F[Option[(Hashed[S], SI)]] =
    getSnapshotWithState(sws => (sws.snapshot, sws.state))

  def getCombinedStream: Stream[F, Option[(Hashed[S], SI)]] =
    ???

  private def getSnapshotWithState[A](extract: SnapshotWithState[S, SI] => A): F[Option[A]] = Files[F]
    .readAll(path)
    .through(text.utf8.decode)
    .compile
    .toList
    .map(_.mkString)
    .map(decode[SnapshotWithState[S, SI]])
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

object BaseLastIncrementalSnapshotStorage {}

case class SnapshotWithState[S, SI](snapshot: Hashed[S], state: SI)
