package org.constellation.snapshotstreaming.db

import cats.Parallel
import cats.effect.{Async, Resource}
import fs2.Stream
import skunk._
import skunk.codec.all._
import skunk.implicits._

import java.time.LocalDateTime

trait SnapshotDBStream[F[_]] {

  def hashes( ordinal: Long): Stream[F,(Long, String, LocalDateTime)]

}

object SnapshotDBStream {

  private val selectHashes: Query[Long, (Long, String, LocalDateTime)] =
    sql"""
    SELECT ordinal, hash, created_at
    FROM global_snapshots
    WHERE ordinal > $int8
    order by ordinal
  """.query(int8 ~ varchar  ~ timestamp).map {
      case ((a, b), c) => (a, b, c)
    }

  def make[F[_]: Async: Parallel](pool: Resource[F, Session[F]]): SnapshotDBStream[F] = new SnapshotDBStream[F] {

    def hashes( ordinal: Long): Stream[F,(Long, String, LocalDateTime)] = Stream.resource(pool).flatMap{
      session => Stream.eval(session.prepare(selectHashes)).flatMap(_.stream(ordinal, chunkSize = 512))
    }

  }

}
