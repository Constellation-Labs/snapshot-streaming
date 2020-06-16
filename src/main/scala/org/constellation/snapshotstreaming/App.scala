package org.constellation.snapshotstreaming

import cats.Parallel
import cats.effect.{Concurrent, ConcurrentEffect, ExitCode, IO, IOApp, Timer}
import cats.implicits._

import scala.concurrent.duration._
import fs2.{RaiseThrowable, Stream}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.snapshotstreaming.mapper.{
  SnapshotInfoMapper,
  StoredSnapshotMapper
}
import org.constellation.snapshotstreaming.es.ElasticSearchClient
import org.constellation.snapshotstreaming.s3.{
  S3DeserializedResult,
  S3StreamClient
}
import org.constellation.snapshotstreaming.serializer.KryoSerializer

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
      config <- getConfig[IO](args)

      _ <- Stream.eval(
        logger.info(
          s"Streaming from bucket (${config.bucketName}, ${config.bucketRegion}) to elasticsearch (${config.elasticsearchUrl})"
        )
      )
      _ <- transfer[IO](configuration)
    } yield ()

  def transfer[F[_]: Concurrent: ConcurrentEffect: Timer: Parallel](
    configuration: Configuration
  ): Stream[F, Unit] =
    for {
      s3Result <- getFromS3(configuration)
      _ <- putToES(configuration)(s3Result)
    } yield ()

  private def getFromS3[F[_]: Concurrent: Timer: RaiseThrowable](
    configuration: Configuration
  ): Stream[F, S3DeserializedResult] =
    for {
      client <- getS3Client[F](
        configuration.bucketName,
        configuration.bucketRegion
      )
      logger = Slf4jLogger.getLogger[F]
      height <- getHeights[F](
        startingHeight = configuration.startingHeight,
        endingHeight = configuration.endingHeight
      )
      result <- client
        .get(height)
        .handleErrorWith(
          e =>
            Stream.eval[F, Unit](
              logger
                .error(s"[S3 ->] Height $height failed: ${e.getMessage}")
            ) >> Stream.raiseError(e)
        )
        .through(retryInfinitely(5.seconds))

      _ <- Stream.eval(logger.info(s"[S3 ->] Height $height succeeded"))
    } yield result

  private def putToES[F[_]: Concurrent: ConcurrentEffect: Timer: RaiseThrowable: Parallel](
    configuration: Configuration
  )(result: S3DeserializedResult): Stream[F, Unit] =
    for {
      client <- getESClient(configuration)
      logger = Slf4jLogger.getLogger[F]
      _ <- client
        .mapAndSendToElasticSearch(result.snapshot, result.snapshotInfo)
        .handleErrorWith(
          e =>
            Stream.eval[F, Unit](
              logger.error(s"[-> ES] ${result.height} failed: ${e.getMessage}")
            ) >> Stream.raiseError(e)
        )
        .through(retryInfinitely(5.seconds))

      _ <- Stream.eval(
        logger.info(s"[-> ES] Height ${result.height} succeeded")
      )

    } yield ()

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
    startingHeight: Long = 2L,
    snapshotInterval: Long = 2L,
    endingHeight: Option[Long] = None
  ): Stream[F, Long] = {
    val heightsIterator = Stream.iterate(startingHeight)(_ + snapshotInterval)
    endingHeight.fold(heightsIterator)(
      endingHeight => heightsIterator.takeWhile(_ <= endingHeight)
    )
  }

  private def getS3Client[F[_]: Concurrent](
    bucket: String,
    region: String
  ): Stream[F, S3StreamClient[F]] =
    Stream.emit(S3StreamClient[F](region, bucket, serializer))

  private def getESClient[F[_]: Concurrent: ConcurrentEffect: Parallel](
    config: Configuration
  ): Stream[F, ElasticSearchClient[F]] = Stream.emit(
    new ElasticSearchClient[F](storedSnapshotMapper, snapshotInfoMapper, config)
  )

  private def getConfig[F[_]: Concurrent](
    args: List[String]
  ): Stream[F, Configuration] = Stream.emit(new Configuration)

}
