package org.constellation.snapshotstreaming.opensearch

import cats.effect.{Async, Resource}
import cats.syntax.flatMap._

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

  def make[F[_]: Async](openSearchUrl: Uri): Resource[F, OpensearchDAO[F]] = {
    val logger = Slf4jLogger.getLogger
    def getEsClient(): Resource[F, ElasticClient] = {
      def client = ElasticClient(
        JavaClient(ElasticProperties(s"${openSearchUrl.renderString}"))
      )
      Resource.fromAutoCloseable(logger.info("Initiating es client.") >> Async[F].delay(client))
    }

    getEsClient().map(make(_))
  }

  def make[F[_]: Async](esClient: ElasticClient): OpensearchDAO[F] =
    new OpensearchDAO[F] {

      def sendToOpensearch(bulkRequest: BulkRequest): Stream[F, Unit] = for {
        _ <- Stream.eval {
          Async[F].delay(esClient.execute(bulkRequest)).flatMap { fut =>
            Async[F].executionContext.flatMap { implicit ec =>
              Async[F].async_[Response[BulkResponse]] { cb =>
                fut.onComplete {
                  case Success(a) =>
                    a match {
                      case RequestSuccess(_, _, _, result) if result.errors =>
                        cb(Left(new Throwable(s"Bulk request failed: ${result.failures}")))
                      case RequestSuccess(_, _, _, result) if !result.errors => cb(Right(a))
                      case RequestFailure(_, _, _, error)                    => cb(Left(error.asException))
                      case _ => cb(Left(new Throwable("Unexpected error")))
                    }
                  case Failure(e) => cb(Left(e))
                }
              }
            }
          }
        }
      } yield (())

    }

}
