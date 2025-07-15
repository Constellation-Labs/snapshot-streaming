package org.constellation.snapshotstreaming.opensearch

import cats.effect.{Async, Resource}
import cats.syntax.flatMap._
import cats.syntax.functor._
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.requests.searches.{SearchHit, SearchRequest}
import fs2.Stream
import org.constellation.snapshotstreaming.OpenSearchConfig
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.reflect.ClassTag

trait OpensearchDAO[F[_]] {

  def bulkStream[T: ClassTag, C: ClassTag](
    search: SearchRequest,
    transformHit: SearchHit => Option[T],
    cursorExtractor: SearchHit => Option[C],
    initialCursor: Option[C]
  ): Stream[F, T]

  def singleQuery[T: ClassTag, C: ClassTag]( search: SearchRequest,
                                             transformHit: SearchHit => Option[T]): F[Option[T]]
}

object OpensearchDAO {

  def make[F[_]: Async](osCfg: OpenSearchConfig): Resource[F, OpensearchDAO[F]] = {
    val logger = Slf4jLogger.getLogger
    val getEsClient: Resource[F, ElasticClient] = {
      def client = ElasticClient(
        JavaClient(ElasticProperties(s"${osCfg.uri.renderString}"))
      )
      Resource.fromAutoCloseable(logger.info("Initiating es client.") >> Async[F].delay(client))
    }

    getEsClient.map(make(_, osCfg))
  }

  def make[F[_]: Async](esClient: ElasticClient, osCfg: OpenSearchConfig): OpensearchDAO[F] = new OpensearchDAO[F] {

    def bulkStream[T: ClassTag, C: ClassTag](
      search: SearchRequest,
      transformHit: SearchHit => Option[T],
      cursorExtractor: SearchHit => Option[C],
      initialCursor: Option[C]
    ): Stream[F, T] = {

      def fetchBatch(lastOrdinalOpt: Option[C]): F[(Array[T], Option[C])] = {
        val requestWithLast = lastOrdinalOpt.fold(search)(last => search.searchAfter(Seq(last))).size(osCfg.bulkSize)
        Async[F].fromFuture(Async[F].delay(esClient.execute(requestWithLast))).map { response =>
          val hits = response.result.hits.hits
          val cursorOpt = hits.lastOption.flatMap(cursorExtractor)
          val results = hits.flatMap(transformHit)
          (results, cursorOpt)
        }
      }

      Stream
        .unfoldEval[F, Option[C], Seq[T]](initialCursor) { cursorOpt =>
          fetchBatch(cursorOpt).map {
            case (results, cursorO) if results.nonEmpty =>
              Some((results.toSeq, cursorO))
            case _ =>
              None
          }
        }
        .flatMap(Stream.emits)
    }

    def singleQuery[T: ClassTag, C: ClassTag]( search: SearchRequest,
                     transformHit: SearchHit => Option[T]): F[Option[T]] = {

      Async[F].fromFuture(Async[F].delay(esClient.execute(search))).map { response =>
        response.result.hits.hits.headOption.flatMap(transformHit)
      }
    }

  }

}
