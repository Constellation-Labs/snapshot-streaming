import Config.CliConfig
import cats.effect._
import cats.implicits._
import s3.S3StreamClient
import fs2._
import org.constellation.consensus.StoredSnapshot
import serializer.KryoSerializer

import scala.concurrent.duration._

object SnapshotMapper extends IOApp {
  val startingHeight = 2L
  val serializer = new KryoSerializer

  def run(args: List[String]): IO[ExitCode] =
    main[IO](args).compile.drain
      .map(_ => ExitCode.Success)
      .handleError(_ => ExitCode.Error)

  def main[F[_]: Concurrent: Timer](args: List[String]): Stream[F, Unit] = {
    for {
      config <- getConfig(args)
      height <- getHeights(startingHeight)
      client <- getStreamClient(config.bucket, config.region)
      snapshot <- client.getSnapshot(height)
        .through(retryInfinitely(5.seconds))
      _ <- Stream.emit(snapshot.toString).covary[F].showLinesStdOut
    } yield ()
  }

  private def retryInfinitely[F[_]: Concurrent: Timer, A](
    delay: FiniteDuration
  )(stream: Stream[F, A]): Stream[F, A] = {
    stream
      .attempts { Stream.unfold(delay)(d => Some(d -> delay)).covary[F] }
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

  private def getStreamClient[F[_]: Concurrent](bucket: String, region: String): Stream[F, S3StreamClient[F]] =
    Stream.emit(S3StreamClient[F](region, bucket, serializer))

  private def getConfig[F[_]: Concurrent](
    args: List[String]
  ): Stream[F, CliConfig] =
    for {
      cliConfig <- Stream.eval(Config.loadCliParams(args))
    } yield cliConfig

}
