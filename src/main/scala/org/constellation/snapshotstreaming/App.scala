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
      esClient <- BlazeClientBuilder[IO](global)
        .withMaxWaitQueueLimit(configuration.maxWaitQueueLimit)
        .stream
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

      s3DAOs = configuration.bucketNames.map(S3DAO[IO](s3Client)(_, serializer))

      s3Object <- getFromS3(s3DAOs)
      _ <- s3Object.map(putToES(elasticSearchDAO)).getOrElse(Stream.emit(()))
    } yield ()

  private def getFromS3[F[_]: Concurrent: Timer: RaiseThrowable](
    s3DAOs: List[S3DAO[F]]
  ): Stream[F, Option[S3DeserializedResult]] = {

    def getWithFallback(height: Long,
                        daos: List[S3DAO[F]],
    ): Stream[F, S3DeserializedResult] =
      daos match {
        case List(bucket) =>
          bucket.get(height).handleErrorWith { e =>
            Stream.eval(
              LiftIO[F].liftIO(logger.error(e)(s"[S3 ->] $height ERROR."))
            ) >> Stream
              .raiseError(e)
          }
        case bucket :: otherBuckets =>
          bucket
            .get(height)
            .handleErrorWith { e =>
              for {
                _ <- Stream.eval(
                  LiftIO[F].liftIO(
                    logger.error(e)(
                      s"[S3 ->] $height ERROR. Trying another bucket (${bucket.getBucketName} -> ${otherBuckets.head.getBucketName})."
                    )
                  )
                )
                resultB <- getWithFallback(height, otherBuckets)
              } yield resultB
            }
        case Nil =>
          Stream.raiseError(
            new Throwable("[S3 ->] ERROR. No buckets available.")
          )
      }

    for {
      height <- getHeights[F](
        startingHeight = configuration.startingHeight,
        endingHeight = configuration.endingHeight
      )

      result <- getWithFallback(height, s3DAOs)
        .through(
          s =>
            if (configuration.skipHeightOnFailure) s
            else
              retryInfinitely(configuration.retryIntervalInSeconds.seconds)(s)
        )
        .flatMap(
          o =>
            Stream
              .eval(
                LiftIO[F].liftIO(
                  logger
                    .info(s"[S3 ->] $height (${o.snapshot.snapshot.hash}) OK")
                )
              ) >> Stream
              .emit(Some(o))
        )
        .handleErrorWith(
          e =>
            if (configuration.skipHeightOnFailure) {
              Stream.eval[F, Unit](
                LiftIO[F].liftIO(
                  logger
                    .error(e)(
                      s"[S3 ->] $height ERROR. Skipping and going to next height."
                    )
                )
              ) >> Stream.emit(None)
            } else Stream.raiseError(e)
        )

    } yield result
  }

  private def putToES[F[_]: ConcurrentEffect: Timer: RaiseThrowable: Parallel](
    elasticSearchDAO: ElasticSearchDAO[F]
  )(result: S3DeserializedResult): Stream[F, Unit] =
    elasticSearchDAO
      .mapAndSendToElasticSearch(result)
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
