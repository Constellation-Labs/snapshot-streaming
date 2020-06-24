package org.constellation.snapshotstreaming

import cats.Parallel
import cats.effect.{
  Concurrent,
  ConcurrentEffect,
  ExitCode,
  IO,
  IOApp,
  LiftIO,
  Timer
}
import cats.implicits._
import com.amazonaws.services.s3.AmazonS3ClientBuilder

import scala.concurrent.duration._
import fs2.{RaiseThrowable, Stream}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.snapshotstreaming.mapper.{
  SnapshotInfoMapper,
  StoredSnapshotMapper
}
import org.constellation.snapshotstreaming.es.ElasticSearchDAO
import org.constellation.snapshotstreaming.s3.{S3DAO, S3DeserializedResult}
import org.constellation.snapshotstreaming.serializer.KryoSerializer
import org.http4s.client.blaze.BlazeClientBuilder

import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration.FiniteDuration

object App extends IOApp {
  private val logger = Slf4jLogger.getLogger[IO]
  private val serializer = new KryoSerializer
  private val configuration = new Configuration
  private val storedSnapshotMapper = new StoredSnapshotMapper
  private val snapshotInfoMapper = new SnapshotInfoMapper

  def run(args: List[String]): IO[ExitCode] = {
    main(args).compile.drain
      .flatTap(_ => logger.debug("Done!"))
      .map(_ => ExitCode.Success)
      .handleError(_ => ExitCode.Error)
  }

  def main(args: List[String]): Stream[IO, Unit] =
    for {
      esClient <- BlazeClientBuilder[IO](global).stream
      s3Client <- Stream.bracket(
        IO(
          AmazonS3ClientBuilder
            .standard()
            .withRegion(configuration.bucketRegion)
            .build()
        )
      )(c => IO(c.shutdown()))

      elasticSearchDAO = ElasticSearchDAO[IO](esClient)(
        storedSnapshotMapper,
        snapshotInfoMapper,
        configuration
      )

      s3DAO = S3DAO[IO](s3Client)(configuration.bucketName, serializer)

      _ <- Stream.eval(
        logger.info(
          s"Streaming from bucket (${configuration.bucketName}, ${configuration.bucketRegion}) to elasticsearch (${configuration.elasticsearchUrl})"
        )
      )

      s3Object <- getFromS3(s3DAO)
      _ <- s3Object.map(putToES(elasticSearchDAO)).getOrElse(Stream.emit(()))
    } yield ()

  private def getFromS3[F[_]: Concurrent: Timer: RaiseThrowable](
    s3DAO: S3DAO[F]
  ): Stream[F, Option[S3DeserializedResult]] = {
    for {
      height <- getHeights[F](
        startingHeight = configuration.startingHeight,
        endingHeight = configuration.endingHeight
      )
      result <- s3DAO
        .get(height)
        .flatMap(
          o =>
            Stream
              .eval(LiftIO[F].liftIO(logger.info(s"[S3 ->] $height OK"))) >> Stream
              .emit(Some(o))
        )
        .handleErrorWith(
          e =>
            Stream.eval[F, Unit](
              LiftIO[F].liftIO(
                logger
                  .error(e)(s"[S3 ->] $height ERROR")
              )
            ) >> Stream.emit(None)
        )

    } yield result
  }

  private def putToES[F[_]: ConcurrentEffect: Timer: RaiseThrowable: Parallel](
    elasticSearchDAO: ElasticSearchDAO[F]
  )(result: S3DeserializedResult): Stream[F, Unit] =
    elasticSearchDAO
      .mapAndSendToElasticSearch(result.snapshot, result.snapshotInfo)
      .flatMap(
        _ =>
          Stream.eval(
            LiftIO[F]
              .liftIO(logger.info(s"[-> ES] ${result.height} OK"))
        )
      )
      .handleErrorWith(
        e =>
          Stream.eval[F, Unit](
            LiftIO[F].liftIO(
              logger
                .error(e)(s"[-> ES] ${result.height} ERROR")
            )
        )
      )

  private def retryInfinitely[F[_]: Concurrent: Timer, A](
    delay: FiniteDuration
  )(stream: Stream[F, A]): Stream[F, A] = {
    stream
      .attempts {
        Stream
          .unfold(delay)(d => Some(d -> delay))
          .covary[F]
      }
      .takeThrough(_.fold(scala.util.control.NonFatal.apply, _ => false))
      .last
      .map(_.get)
      .rethrow
  }

  private def getHeights[F[_]: Concurrent](
    startingHeight: Long,
    snapshotInterval: Long = 2L,
    endingHeight: Option[Long] = None
  ): Stream[F, Long] = {
    val heightsIterator = Stream.iterate(startingHeight)(_ + snapshotInterval)
    endingHeight.fold(heightsIterator)(
      endingHeight => heightsIterator.takeWhile(_ <= endingHeight)
    )
  }
}
