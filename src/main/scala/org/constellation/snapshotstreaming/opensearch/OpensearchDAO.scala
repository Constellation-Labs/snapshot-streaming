package org.constellation.snapshotstreaming.opensearch

import cats.effect.{Async, Resource}
import cats.syntax.flatMap._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.requests.bulk.{BulkRequest, BulkResponse}
import fs2.Stream
import org.http4s.Uri
import org.typelevel.log4cats.slf4j.Slf4jLogger

trait OpensearchDAO[F[_]] {
  def sendToOpensearch(bulkRequest: BulkRequest): Stream[F, Unit]
}

object OpensearchDAO {

  def make[F[_]: Async](openSearchUrl: Uri) =
    new OpensearchDAO[F] {

      private val logger = Slf4jLogger.getLoggerFromClass[F](OpensearchDAO.getClass)

      def getEsClient(): Resource[F, ElasticClient] = {
        val client = ElasticClient(
          JavaClient(ElasticProperties(s"${openSearchUrl.renderString}"))
        )
        Resource.fromAutoCloseable(logger.info("Initiating es client.") >> Async[F].delay(client))
      }

      def sendToOpensearch(bulkRequest: BulkRequest): Stream[F, Unit] = for {
        esClient <- Stream.resource(getEsClient())
        _ <- Stream.eval {
          Async[F].async_[Response[BulkResponse]] { cb =>
            esClient.execute(bulkRequest).onComplete {
              case Success(a) =>
                a match {
                  case RequestSuccess(_, _, _, result) if result.errors =>
                    cb(Left(new Throwable(s"Bulk request failed: ${result.failures}")))
                  case RequestSuccess(_, _, _, result) if !result.errors => cb(Right(a))
                  case RequestFailure(_, _, _, error)                    => cb(Left(error.asException))
                  case _                                                 => cb(Left(new Throwable("Unexpected error")))
                }
              case Failure(e) => cb(Left(e))
            }
          }
        }
      } yield (())

    }

}