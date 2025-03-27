package org.constellation.snapshotstreaming

import cats.{Applicative, Monad}
import cats.effect.std.Console
import cats.effect.{Resource, Temporal}
import cats.syntax.all._
import fs2.io.net.Network
import org.typelevel.otel4s.trace.Tracer
import skunk.{Command, PreparedCommand, Session}

package object db {

  def session[F[_]: Temporal: Tracer: Network: Console](dbConfig: DbConfig): Resource[F, Resource[F, Session[F]]] = {
    val DbConfig(host, port, user, password, database, maxConnections) = dbConfig

    Session.pooled(
      host = host,
      port = port,
      user = user,
      database = database,
      password = password,
      max = maxConnections
    )
  }

  def executeMany[T, F[_]: Monad](s: Session[F], entities: List[T])(insertMany: Command[entities.type]): F[Unit] =
    s.prepare(insertMany).flatMap(_.execute(entities)).void.whenA(entities.nonEmpty)

  private val dbChunkSize = 5000

  def executeMany[T, F[_]: Monad](s: Session[F], entities: List[T], insertMany: Int => Command[List[T]]): F[Unit] =
    entities.grouped(dbChunkSize).toList.traverse(es => s.prepare(insertMany(es.size)).flatMap(_.execute(es))).void

}
