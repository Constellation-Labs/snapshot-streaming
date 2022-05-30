package org.constellation.snapshotstreaming.opensearch

import java.util.Date

import cats.effect.{Async, Clock}
import cats.syntax.functor._

import org.tessellation.dag.snapshot.GlobalSnapshot
import org.tessellation.security.signature.Signed

import com.sksamuel.elastic4s.ElasticDsl.bulk
import fs2.Stream
import org.constellation.snapshotstreaming.Configuration
import org.typelevel.log4cats.slf4j.Slf4jLogger

trait SnapshotDAO[F[_]] {
  def sendSnapshotToOpensearch(snapshot: Signed[GlobalSnapshot]): Stream[F, Long]
}

object SnapshotDAO {

  def make[F[_]: Async](updateRequestBuilder: UpdateRequestBuilder[F], config: Configuration): SnapshotDAO[F] =
    make(updateRequestBuilder, OpensearchDAO.make(config.opensearchUrl))

  def make[F[_]: Async](
    updateRequestBuilder: UpdateRequestBuilder[F],
    opensearchDAO: OpensearchDAO[F]
  ): SnapshotDAO[F] =
    new SnapshotDAO[F] {

      private val logger = Slf4jLogger.getLoggerFromClass[F](SnapshotDAO.getClass)

      def sendSnapshotToOpensearch(globalSnapshot: Signed[GlobalSnapshot]) = {
        val sendInBulks = for {
          timestamp <- Stream.eval(Clock[F].realTime.map(d => new Date(d.toMillis)))
          bulks <- Stream.eval(updateRequestBuilder.bulkUpdateRequests(globalSnapshot, timestamp))
          bulkRequests <- Stream.emits(bulks)
          _ <- opensearchDAO.sendToOpensearch(bulk(bulkRequests).refreshImmediately)
        } yield ()

        sendInBulks
          .fold[Long](globalSnapshot.ordinal.value.value) { case (_, _) => globalSnapshot.ordinal.value.value }
          .evalTap(ordinal => logger.info(s"Snapshot $ordinal sent to opensearch."))
      }

    }

}
