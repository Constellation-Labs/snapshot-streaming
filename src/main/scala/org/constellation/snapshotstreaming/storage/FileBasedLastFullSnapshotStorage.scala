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
import io.circe.generic.semiauto.deriveDecoder
import io.circe.generic.semiauto.deriveEncoder
import org.tessellation.schema.snapshot.Snapshot

trait FileBasedLastFullSnapshotStorage[F[_], S <: Snapshot] {
  def set(snapshot: Hashed[S]): F[Unit]
  def get: F[Option[Signed[S]]]
}

object FileBasedLastFullSnapshotStorage {

  def make[F[_]: Async, S <: Snapshot: Decoder: Encoder](path: Path): FileBasedLastFullSnapshotStorage[F, S] =
    new FileBasedLastFullSnapshotStorage[F, S] {
      implicit val decoder: Decoder[Hashed[S]] = deriveDecoder[Hashed[S]]
      implicit val encoder: Encoder[Hashed[S]] = deriveEncoder[Hashed[S]]

      def set(snapshot: Hashed[S]): F[Unit] =
        Stream
          .emit(snapshot.asJson.spaces2)
          .through(text.utf8.encode)
          .through(Files[F].writeAll(path))
          .compile
          .drain

      def get: F[Option[Signed[S]]] = Files[F]
        .readAll(path)
        .through(text.utf8.decode)
        .compile
        .toList
        .map(_.mkString)
        .map(decode[Hashed[S]])
        .flatMap(_.liftTo[F])
        .map(_.signed)
        .map(_.some)
        .handleErrorWith {
          case _: NoSuchFileException => Applicative[F].pure(None)
          case e                      => e.raiseError[F, Option[Signed[S]]]
        }

    }

}
