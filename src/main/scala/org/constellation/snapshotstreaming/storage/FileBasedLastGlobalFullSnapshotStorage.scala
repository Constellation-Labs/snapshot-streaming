package org.constellation.snapshotstreaming.storage

import cats.Applicative
import cats.effect.Async
import cats.syntax.applicativeError._
import cats.syntax.either._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.option._
import org.tessellation.security.Hashed
import org.tessellation.security.signature.Signed
import fs2.io.file.Path
import fs2.io.file._
import fs2.Stream
import fs2.text
import io.circe.parser.decode
import io.circe.syntax._
import io.circe.Decoder
import io.circe.Encoder
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.tessellation.schema.snapshot.Snapshot
import org.tessellation.schema.GlobalSnapshot

trait FileBasedLastGlobalFullSnapshotStorage[F[_]] {
  def set(snapshot: Hashed[GlobalSnapshot]): F[Unit]
  def get: F[Option[Signed[GlobalSnapshot]]]
}

object FileBasedLastGlobalFullSnapshotStorage {

  def make[F[_]: Async, S <: Snapshot: Decoder: Encoder](path: Path): FileBasedLastGlobalFullSnapshotStorage[F] =
    new FileBasedLastGlobalFullSnapshotStorage[F] {
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
