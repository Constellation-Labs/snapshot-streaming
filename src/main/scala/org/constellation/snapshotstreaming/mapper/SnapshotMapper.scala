package org.constellation.snapshotstreaming.mapper

import cats.effect.{Concurrent, ExitCode, IO, IOApp, Timer}
import cats.implicits._

import scala.concurrent.duration._
import fs2.Stream
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.snapshotstreaming.Config
import org.constellation.snapshotstreaming.Config.CliConfig
import org.constellation.snapshotstreaming.mapper.SnapshotMapper.startingHeight
import org.constellation.snapshotstreaming.s3.S3StreamClient
import org.constellation.snapshotstreaming.serializer.KryoSerializer

import scala.concurrent.duration.FiniteDuration

object SnapshotMapper extends IOApp {
  private val startingHeight = 2L
  private val serializer = new KryoSerializer
  private val logger = Slf4jLogger.getLogger[IO]

  def run(args: List[String]): IO[ExitCode] = {
    main(args).compile.drain
      .map(_ => ExitCode.Success)
      .handleError(_ => ExitCode.Error)
  }

  def main(args: List[String]): Stream[IO, Unit] =
    for {
      config <- getConfig[IO](args)
      _ <- Stream.eval(
        logger.info(
          s"Streaming snapshots from bucket: ${config.bucket} (${config.region})"
        )
      )
      run = emit(config) _
      _ <- run(710L)
//      _ <- Stream(run(2L, 40L, 1), run(42L, 80L, 2), run(82L, 122L, 3))
//        .parJoin(3)
    } yield ()

  def emit(config: CliConfig)(startingHeight: Long): Stream[IO, Unit] =
    for {
      height <- getHeights[IO](startingHeight)
      client <- getStreamClient[IO](config.bucket, config.region)
      snapshot <- client
        .getSnapshot(height)
        .handleErrorWith(
          e =>
            Stream
              .eval(logger.error(s"❌ $height failed. Retrying...: $e")) >> Stream
              .raiseError[IO](e)
        )
        .through(retryInfinitely(5.seconds))
      _ <- Stream.eval(
        logger.info(s"✅ ${snapshot.height} - ${snapshot.snapshot.hash}")
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
  ): Stream[F, CliConfig] =
    for {
      cliConfig <- Stream.eval(Config.loadCliParams(args))
    } yield cliConfig

}
