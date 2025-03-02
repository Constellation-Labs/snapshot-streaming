package org.constellation.snapshotstreaming

import cats.Applicative
import cats.effect.std.Console
import cats.effect.{Resource, Temporal}
import cats.syntax.all._
import fs2.io.net.Network
import org.typelevel.otel4s.trace.Tracer
import skunk.{PreparedCommand, Session}

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

  def executeCmd[T, F[_]: Applicative](cmd: PreparedCommand[F, T])(entities: Seq[T]): F[Unit] =
    entities.traverse(e => cmd.execute(e)).whenA(entities.nonEmpty)

}
