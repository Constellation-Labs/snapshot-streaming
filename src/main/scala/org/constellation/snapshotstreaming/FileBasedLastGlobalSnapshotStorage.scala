package org.constellation.snapshotstreaming

import java.nio.file.NoSuchFileException
import cats.effect.Async
import cats.effect.std.Semaphore
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.{Applicative, MonadThrow}
import org.tessellation.sdk.domain.snapshot.storage.LastGlobalSnapshotStorage
import org.tessellation.sdk.domain.snapshot.Validator.isNextSnapshot
import org.tessellation.dag.snapshot.{GlobalSnapshot, SnapshotOrdinal}
import org.tessellation.dag.snapshot.GlobalSnapshotReference.{fromHashedGlobalSnapshot => getSnapshotReference}
import org.tessellation.schema.height.Height
import org.tessellation.security.Hashed
import fs2.io.file._
import fs2.{Stream, text}
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import io.circe.parser.decode
import io.circe.syntax._
import org.typelevel.log4cats.slf4j.Slf4jLogger

object FileBasedLastGlobalSnapshotStorage {

  def make[F[_]: Async: Files](path: Path): F[LastGlobalSnapshotStorage[F]] =
    Semaphore[F](1)
      .map(make(path, _))

  private def make[F[_]: Async: Files](path: Path, semaphore: Semaphore[F]): LastGlobalSnapshotStorage[F] =
    new LastGlobalSnapshotStorage[F] {

      implicit val codec: Codec[Hashed[GlobalSnapshot]] = deriveCodec[Hashed[GlobalSnapshot]]

      private val logger = Slf4jLogger.getLogger[F]

      def set(snapshot: Hashed[GlobalSnapshot]): F[Unit] =
        semaphore.permit.use { _ =>
          get.flatMap {
            case Some(last) if isNextSnapshot(last, snapshot) =>
              Stream
                .emit(snapshot.asJson.spaces2)
                .through(text.utf8.encode)
                .through(Files[F].writeAll(path))
                .compile.drain
            case Some(last) =>
              MonadThrow[F].raiseError[Unit](new Throwable(s"Snapshot is not the next one! last: ${getSnapshotReference(last)} next: ${getSnapshotReference(snapshot)}"))
            case None =>
              MonadThrow[F].raiseError[Unit](new Throwable("Previous snapshot not found when setting next global snapshot!"))
          }
        }

      def setInitial(snapshot: Hashed[GlobalSnapshot]): F[Unit] =
        semaphore.permit.use { _ =>
          Stream
            .emit(snapshot.asJson.spaces2)
            .through(text.utf8.encode)
            .through(Files[F].writeAll(path, Flags(Flag.Write, Flag.CreateNew)))
            .compile.drain
            .onError(e => logger.error(e)(s"Failure setting initial global snapshot!"))
        }

      def get: F[Option[Hashed[GlobalSnapshot]]] =
        Files[F]
          .readAll(path)
          .through(text.utf8.decode)
          .map(decode[Hashed[GlobalSnapshot]])
          .map(_.toOption)
          .compile
          .last
          .map(_.flatten)
          .handleErrorWith {
            case _: NoSuchFileException => Applicative[F].pure(None)
            case e => e.raiseError[F, Option[Hashed[GlobalSnapshot]]]
          }

      def getOrdinal: F[Option[SnapshotOrdinal]] =
        get.map(_.map(_.ordinal))

      def getHeight: F[Option[Height]] =
        get.map(_.map(_.height))
    }
}
