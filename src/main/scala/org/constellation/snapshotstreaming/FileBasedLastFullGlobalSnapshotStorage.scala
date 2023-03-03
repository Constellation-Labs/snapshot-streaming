package org.constellation.snapshotstreaming

import cats.Applicative
import cats.effect.Async
import cats.syntax.applicativeError._
import cats.syntax.either._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.option._

import org.tessellation.schema.GlobalSnapshot
import org.tessellation.security.Hashed
import org.tessellation.security.signature.Signed

import fs2.io.file.{Path, _}
import fs2.{Stream, text}
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import io.circe.parser.decode
import io.circe.syntax._

trait FileBasedLastFullGlobalSnapshotStorage[F[_]] {
  def set(snapshot: Hashed[GlobalSnapshot]): F[Unit]
  def get: F[Option[Signed[GlobalSnapshot]]]
}

object FileBasedLastFullGlobalSnapshotStorage {

  def make[F[_]: Async](path: Path): FileBasedLastFullGlobalSnapshotStorage[F] =
    new FileBasedLastFullGlobalSnapshotStorage[F] {
      implicit val codec: Codec[Hashed[GlobalSnapshot]] = deriveCodec[Hashed[GlobalSnapshot]]

      def set(snapshot: Hashed[GlobalSnapshot]): F[Unit] =
        Stream
          .emit(snapshot.asJson.spaces2)
          .through(text.utf8.encode)
          .through(Files[F].writeAll(path))
          .compile
          .drain

      def get: F[Option[Signed[GlobalSnapshot]]] = Files[F]
        .readAll(path)
        .through(text.utf8.decode)
        .compile
        .toList
        .map(_.mkString)
        .map(decode[Hashed[GlobalSnapshot]])
        .flatMap(_.liftTo[F])
        .map(_.signed)
        .map(_.some)
        .handleErrorWith {
          case _: NoSuchFileException => Applicative[F].pure(None)
          case e                      => e.raiseError[F, Option[Signed[GlobalSnapshot]]]
        }

    }

}
