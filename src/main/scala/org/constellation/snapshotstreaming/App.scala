package org.constellation.snapshotstreaming

import java.nio.file.Paths
import cats.Parallel
import cats.effect.{Blocker, Concurrent, ConcurrentEffect, ContextShift, ExitCode, IO, IOApp, LiftIO, Timer}
import cats.implicits._
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import fs2.{RaiseThrowable, Stream, _}
import org.constellation.snapshotstreaming.es.ElasticSearchDAO
import org.constellation.snapshotstreaming.mapper.{SnapshotInfoMapper, StoredSnapshotMapper}
import org.constellation.snapshotstreaming.s3.{S3DAO, S3DeserializedResult, S3GenesisDeserializedResult}
import org.constellation.snapshotstreaming.serializer.KryoSerializer
import org.constellation.snapshotstreaming.validation.BalanceValidator

import scala.concurrent.duration.{FiniteDuration, _}

object App extends IOApp {
  private val logger = Slf4jLogger.getLogger[IO]
  private val configuration = new Configuration
  private val storedSnapshotMapper = new StoredSnapshotMapper
  private val snapshotInfoMapper = new SnapshotInfoMapper
  private val balanceValidator = new BalanceValidator[IO](Map.empty)

  def run(args: List[String]): IO[ExitCode] =
    main(args).compile.drain
      .flatTap(_ => logger.debug("Done!"))
      .map(_ => ExitCode.Success)
      .handleErrorWith(e => logger.error(e)(e.getMessage).map(_ => ExitCode.Error))

  def main(args: List[String]): Stream[IO, Unit] =
    for {
      _ <- Stream.eval(KryoSerializer.init[IO])
      s3Client <- Stream.bracket(
        IO(
          AmazonS3ClientBuilder
            .standard()
            .withRegion(configuration.bucketRegion)
            .build()
        )
      )(c => IO(c.shutdown()))

      elasticSearchDAO = ElasticSearchDAO[IO](
        storedSnapshotMapper,
        snapshotInfoMapper,
        configuration
      )

      s3DAOs = configuration.bucketNames.map(S3DAO[IO](s3Client)(_))

      _ <- if (configuration.getGenesis)
        getAndSendGenesisToES(s3DAOs, elasticSearchDAO)
      else
        Stream.eval(LiftIO[IO].liftIO(().pure[IO]))

      _ <- if (configuration.getSnapshots)
        getAndSendSnapshotsToES(s3DAOs, elasticSearchDAO)
      else
        Stream.eval(LiftIO[IO].liftIO(().pure[IO]))
    } yield ()

  private def getAndSendGenesisToES[F[_]: Timer: RaiseThrowable: ConcurrentEffect: Parallel](
    s3DAOs: List[S3DAO[F]],
    elasticSearchDAO: ElasticSearchDAO[F]
  ): Stream[F, Unit] =
    for {
      genesis <- getGenesisFromS3(s3DAOs)
      _ <- putGenesisToES(elasticSearchDAO)(genesis)
    } yield ()

  private def getAndSendSnapshotsToES[F[_]: Timer: RaiseThrowable: ConcurrentEffect: Parallel: ContextShift](
    s3DAOs: List[S3DAO[F]],
    elasticSearchDAO: ElasticSearchDAO[F]
  ): Stream[F, Unit] =
    for {
      s3Object <- getFromS3(s3DAOs)

      _ <- s3Object
        .map(putToES(elasticSearchDAO))
        .getOrElse(Stream.emit(()))

      _ <- s3Object
        .map(r => saveLastStreamedHeightToFile(r.height))
        .getOrElse(Stream.emit(()))

      _ <- s3Object
        .map(r => Stream.eval(LiftIO[F].liftIO(balanceValidator.validate(r))))
        .getOrElse(Stream.emit(()))
    } yield ()

  private def getGenesisFromS3[F[_]: Concurrent: Timer: RaiseThrowable](
    s3DAOs: List[S3DAO[F]]
  ): Stream[F, S3GenesisDeserializedResult] = {
    s3DAOs match {
      case List(bucket) =>
        bucket.getGenesis().handleErrorWith { e =>
          Stream.eval(
            LiftIO[F].liftIO(
              logger.error(e)(s"[S3-Genesis ->] ERROR. While getting Genesis from bucket=${bucket.getBucketName}.")
            )
          ) >> Stream.raiseError(e)
        }
      case bucket :: otherBuckets =>
        bucket.getGenesis().handleErrorWith { e =>
          for {
            _ <- Stream.eval(
              LiftIO[F].liftIO(
                logger.error(e)(
                  s"[S3-Genesis ->] ERROR. Trying another bucket (${bucket.getBucketName} -> ${otherBuckets.head.getBucketName})."
                )
              )
            )
            resultB <- getGenesisFromS3(otherBuckets)
          } yield resultB
        }
      case Nil =>
        val e = new Throwable("[S3-Genesis ->] ERROR. No buckets available.")
        Stream.eval(
          LiftIO[F].liftIO(logger.error(e)(e.getMessage))
        ) >> Stream.raiseError(e)
    }
  }.flatMap { o =>
    Stream.eval(
      LiftIO[F].liftIO(logger.info("[S3-Genesis ->] OK."))
    ) >>
      Stream.emit(o)
  }

  private def getWithFallback[F[_]: Concurrent: RaiseThrowable](
    height: Long,
    daos: List[S3DAO[F]]
  ): Stream[F, S3DeserializedResult] =
    daos match {
      case List(bucket) =>
        bucket
          .get(height)
          .flatMap(
            r =>
              Stream.eval(
                LiftIO[F].liftIO(logger.info(s"${height} OK from ${bucket.getBucketName}"))
              ) >> Stream.emit(r)
          )
          .handleErrorWith { e =>
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
                  logger.error(
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

  private def getFromS3[F[_]: Concurrent: Timer: RaiseThrowable: ContextShift](
    s3DAOs: List[S3DAO[F]]
  ): Stream[F, Option[S3DeserializedResult]] =
    for {
      startingHeight <- getStartingHeight

      height <- configuration.fileWithHeights.fold(
        getHeights[F](
          startingHeight = startingHeight,
          endingHeight = configuration.endingHeight
        )
      )(getHeightsFromFile[F](_))

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

  private def putGenesisToES[F[_]: ConcurrentEffect: Timer: RaiseThrowable: Parallel](
    elasticSearchDAO: ElasticSearchDAO[F]
  )(result: S3GenesisDeserializedResult): Stream[F, Unit] =
    elasticSearchDAO
      .mapGenesisAndSendToElasticSearch(result)
      .flatMap(
        _ =>
          Stream.eval(
            LiftIO[F]
              .liftIO(logger.info(s"[Genesis -> ES] OK"))
          )
      )
      .handleErrorWith(
        e =>
          Stream.eval[F, Unit](
            LiftIO[F].liftIO(
              logger
                .error(e)(s"[Genesis -> ES] ERROR")
            )
          )
      )

  private def getStartingHeight[F[_]: Concurrent: ContextShift]: Stream[F, Long] =
    configuration.startingHeight
      .map(Stream.emit)
      .getOrElse(getLastStreamedHeightFromFile)
      .handleErrorWith(_ => Stream.raiseError[F](new Throwable("Couldn't get starting height")))

  private def saveLastStreamedHeightToFile[F[_]: Concurrent: ContextShift](height: Long): Stream[F, Unit] =
    Stream.resource(Blocker[F]).flatMap { blocker: Blocker =>
      Stream
        .emit(height.toString)
        .through(fs2.text.utf8Encode)
        .through(fs2.io.file.writeAll(Paths.get(configuration.lastSentHeightPath), blocker))
    }

  private def getLastStreamedHeightFromFile[F[_]: Concurrent: ContextShift]: Stream[F, Long] =
    Stream.resource(Blocker[F]).flatMap { blocker: Blocker =>
      fs2.io.file
        .readAll(Paths.get(configuration.lastSentHeightPath), blocker, 4096)
        .through(text.utf8Decode)
        .through(text.lines)
        .map(_.toLong)
        .evalTap(
          height =>
            LiftIO[F]
              .liftIO(
                logger
                  .info(s"Found last streamed height in file: $height. Returning starting height: ${height + 2L}.")
              )
        )
        .map(_ + 2L)
        .take(1)
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
  )(stream: Stream[F, A]): Stream[F, A] =
    stream.attempts {
      Stream
        .unfold(delay)(d => Some(d -> delay))
        .covary[F]
    }.takeThrough(_.fold(scala.util.control.NonFatal.apply, _ => false))
      .last
      .map(_.get)
      .rethrow

  private def getHeightsFromFile[F[_]: Concurrent: ContextShift](
    path: String
  ): Stream[F, Long] =
    Stream.resource(Blocker[F]).flatMap { blocker: Blocker =>
      fs2.io.file
        .readAll(Paths.get(path), blocker, 4 * 4096)
        .through(text.utf8Decode)
        .through(text.lines)
        .filter(_.nonEmpty)
        .map(_.toLong)
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
