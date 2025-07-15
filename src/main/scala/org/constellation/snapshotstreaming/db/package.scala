package org.constellation.snapshotstreaming

import cats.{Applicative, Monad}
import cats.effect.std.Console
import cats.effect.{Resource, Temporal}
import cats.syntax.all._
import fs2.io.net.Network
import org.typelevel.log4cats.Logger
import org.typelevel.otel4s.trace.Tracer
import skunk.data.Completion
import skunk.data.Completion.Insert
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

  def executeCmd[T, F[_]: Monad: Logger](cmd: PreparedCommand[F, T])(entities: Seq[T]): F[Unit] =
    for {
      results <- entities.traverse(e => cmd.execute(e))
      _ <- logAffectedCount(results)
    } yield ()

  private val dbChunkSize = 5000

  def executeMany[T, F[_]: Monad: Logger](
    s: Session[F],
    entities: List[T],
    insertMany: Int => Command[List[T]]
  ): F[Unit] = for {
    results <- entities
      .grouped(dbChunkSize)
      .toList
      .traverse(es => s.prepare(insertMany(es.size)).flatMap(_.execute(es)))
    _ <- logAffectedCount(results)
  } yield ()

  def logAffectedCount[F[_]](results: Seq[Completion])(implicit log: Logger[F]): F[Unit] =
    log.info(s"Affected rows ${affectedCount(results)}")

  def affectedCount(results: Seq[Completion]): Int = results.mapFilter {
    case Insert(n) => Some(n)
    case _         => None
  }.sum

}
