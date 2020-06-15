package org.constellation.snapshotstreaming.mapper

import cats.effect.{Concurrent, ExitCode, IO, IOApp, Timer}
import cats.implicits._

import scala.concurrent.duration._
import fs2.Stream
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.snapshotstreaming.Configuration
import org.constellation.snapshotstreaming.mapper.MapperApp.startingHeight
import org.constellation.snapshotstreaming.output.ElasticSearchSender
import org.constellation.snapshotstreaming.s3.S3StreamClient
import org.constellation.snapshotstreaming.serializer.KryoSerializer

import scala.concurrent.duration.FiniteDuration

object MapperApp extends IOApp {
  private val startingHeight = 2L
  private val serializer = new KryoSerializer
  private val configLoader = new Configuration
  private val storedSnapshotMapper = new StoredSnapshotMapper
  private val es =
    new ElasticSearchSender[IO](storedSnapshotMapper, configLoader)
  private val logger = Slf4jLogger.getLogger[IO]

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
          s"Streaming snapshots from bucket: ${config.bucketName} (${config.bucketRegion})"
        )
      )
      run = emit(config) _
      _ <- run(config.startingHeight, config.endingHeight)
      // TODO: Parallel
//      _ <- Stream(run(2L, 40L, run(42L, 80L, run(82L, 122L)
//        .parJoin(3)
    } yield ()

  def emit(
    config: Configuration
  )(startingHeight: Long, endingHeight: Option[Long] = None): Stream[IO, Unit] =
    for {
      height <- getHeights[IO](
        startingHeight = startingHeight,
        endingHeight = endingHeight
      )
      client <- getStreamClient[IO](config.bucketName, config.bucketRegion)
      snapshot <- client
        .getSnapshot(height)
        .handleErrorWith(
          e =>
            Stream
              .eval(
                logger
                  .error(s"(S3) $height failed. Retrying...: ${e.getMessage}")
              ) >> Stream
              .raiseError[IO](e)
        )
        .through(retryInfinitely(5.seconds))
      _ <- Stream.eval(
        logger.info(s"(S3) ${snapshot.height} - ${snapshot.snapshot.hash}")
      )
      _ <- es
        .mapAndSendToElasticSearch(snapshot)
        .handleErrorWith(
          e =>
            Stream.eval(
              logger.error(s"(ES) $height failed. Retrying...: ${e.getMessage}")
            ) >> Stream.raiseError[IO](e)
        )
        .through(retryInfinitely(5.seconds))
      _ <- Stream.eval(
        logger.info(s"(ES) ${snapshot.height} - ${snapshot.snapshot.hash}\n")
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

  private def getStreamClient[F[_]: Concurrent](
    bucket: String,
    region: String
  ): Stream[F, S3StreamClient[F]] =
    Stream.emit(S3StreamClient[F](region, bucket, serializer))

  private def getConfig[F[_]: Concurrent](
    args: List[String]
  ): Stream[F, Configuration] =
    for {
      cliConfig <- Stream.emit(new Configuration)
    } yield cliConfig

}
