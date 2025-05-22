package org.constellation.snapshotstreaming

import cats.{Applicative, Monad, Parallel}
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

  def executeCmd[T, F[_]: Applicative: Parallel](cmd: PreparedCommand[F, T])(entities: Seq[T]): F[Unit] =
    entities.parTraverse(e => cmd.execute(e)).whenA(entities.nonEmpty)

  private val dbChunkSize = 5000

  def executeMany[T, F[_]: Monad: Parallel](s: Session[F], entities: List[T], insertMany: Int => Command[List[T]]): F[Unit] =
    entities.grouped(dbChunkSize).toList.parTraverse(es => s.prepare(insertMany(es.size)).flatMap(_.execute(es))).void

}
