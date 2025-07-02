package org.constellation.snapshotstreaming

import cats.Parallel
import cats.effect._
import cats.effect.implicits.clockOps
import cats.effect.std.{Console, Random}
import cats.syntax.all._
import fs2.Stream
import fs2.io.file.Files
import fs2.io.net.Network
import io.circe.Decoder
import org.constellation.snapshotstreaming.db.{SnapshotDAO, SnapshotDBStream}
import org.constellation.snapshotstreaming.mapper.{CurrencySnapshotMapper, GlobalSnapshotMapper}
import org.constellation.snapshotstreaming.s3.S3DAO
import org.constellation.snapshotstreaming.schema.schema.{GlobalData, MetagraphData}
import org.tessellation.currency.schema.currency.CurrencyIncrementalSnapshot
import org.tessellation.json.{JsonBrotliBinarySerializer, JsonSerializer}
import org.tessellation.kryo.KryoSerializer
import org.tessellation.node.shared.config.types.SharedConfigReader
import org.tessellation.schema.{GlobalIncrementalSnapshot, SnapshotOrdinal}
import org.tessellation.schema.address.Address
import org.tessellation.security._
import org.tessellation.security.hash.Hash
import org.tessellation.security.signature.Signed
import org.tessellation.statechannel.StateChannelSnapshotBinary
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.otel4s.trace.Tracer

import java.time.{Instant, LocalDateTime, ZoneId}


trait SnapshotProcessorS3[F[_]] {
  val runtime: Stream[F, Unit]
}

object SnapshotProcessorS3 {

  def make[F[
    _
  ]: Async: Parallel: KryoSerializer: JsonSerializer: SecurityProvider: Random: HasherSelector: Network: Files: Tracer: Console](
    configuration: SnapshotStreamingConfig,
    txHasher: Hasher[F]
  ): Resource[F, SnapshotProcessorS3[F]] =
    for {
      s3DAO <- S3DAO.make[F](configuration.s3)
      sessionPool <- db.session[F](configuration.db)
      snapshotDAO = SnapshotDAO.make[F](sessionPool)
      snapshotDBStream = SnapshotDBStream.make[F](sessionPool)
      jsonBrotliBinarySerializer <- Resource.eval(JsonBrotliBinarySerializer.forSync[F])
    } yield make(
      configuration,
      s3DAO,
      snapshotDAO,
      snapshotDBStream,
      GlobalSnapshotMapper.make(),
      CurrencySnapshotMapper.make(),
      txHasher,
      jsonBrotliBinarySerializer
    )


  def make[F[_]: Async: Parallel: HasherSelector](
    configuration: SnapshotStreamingConfig,
    s3DAO: S3DAO[F],
    snapshotDAO: SnapshotDAO[F],
    snapshotDBStream: SnapshotDBStream[F],
    globalMapper: GlobalSnapshotMapper[F],
    currencyMapper: CurrencySnapshotMapper[F],
    txHasher: Hasher[F],
    jsonBrotliBinarySerializer: JsonBrotliBinarySerializer[F]
  ): SnapshotProcessorS3[F] = new SnapshotProcessorS3[F] {
    private implicit val logger = Slf4jLogger.getLogger[F]

    private def storeInPostgres(global: GlobalData, metagraph: MetagraphData) =
      (snapshotDAO.insertGlobalData(global, metagraph.snapshots.size) >> snapshotDAO
        .insertMetagraphData(global.snapshot.hash, metagraph)
        .whenA(metagraph.snapshots.nonEmpty)).timed.flatMap { t =>
        logger
          .info(
            s"Snapshot ${global.snapshot.ordinal} (hash: ${global.snapshot.hash.show}) sent to postgres in ${t._1.toSeconds} s."
          ) >>
          logger
            .info(s"Metagraph Snapshots for currencies ${metagraph.snapshots.map(_.identifier)}  sent to postgres.")
            .handleErrorWith(s => logger.error(s)("Error in database layer") >> s.raiseError[F, Unit])
      }

    def deserialize[A: Decoder](binary: Signed[StateChannelSnapshotBinary]): F[Option[A]] =
      jsonBrotliBinarySerializer.deserialize[A](binary.value.content).map(_.toOption)

    private def mapSnapshots(
      snapshot: Hashed[GlobalIncrementalSnapshot],
      ccys: List[(Address, Hashed[CurrencyIncrementalSnapshot], Signed[StateChannelSnapshotBinary])],
      d: LocalDateTime,
      hasher: Hasher[F]
    ) = (
      globalMapper.mapGlobalSnapshot(snapshot, d, txHasher, hasher),
      currencyMapper.mapCurrencySnapshots(ccys, d, txHasher, hasher)
    ).tupled

    def store(
      snapshot: Hashed[GlobalIncrementalSnapshot],
      ccys: List[(Address, Hashed[CurrencyIncrementalSnapshot], Signed[StateChannelSnapshotBinary])],
      d: LocalDateTime,
      hasher: Hasher[F]
    ): F[Unit] =
      mapSnapshots(snapshot, ccys, d, hasher)
        .flatMap { case (globalData, metagraphData) =>
          Async[F].delay {
            if (metagraphData.snapshots.isEmpty && snapshot.stateChannelSnapshots.nonEmpty)
              throw new Exception(s"No MG snapshots for ${snapshot.stateChannelSnapshots}")
            else ()
          } >>
            storeInPostgres(globalData, metagraphData)
        }
        .void

    val reindexerConf = configuration.reindexer.get

    val runtime: Stream[F, Unit] = {

      val startAfterOrdinal = reindexerConf.startAfterOrdinal

      snapshotDBStream
        .hashes(startAfterOrdinal)
        .parEvalMap(reindexerConf.s3Parallelism) { case (ordinal, h, ts) =>
          val snapshotOrdinal = SnapshotOrdinal(ordinal).get
          implicit val hasher = HasherSelector[F].getForOrdinal(snapshotOrdinal)
          logger.info(s"Downloading hash ${h} from S3") >>
            s3DAO
              .downloadSnapshot(Hash(h), hasher.getLogic(snapshotOrdinal))
              .flatMap { snapshot =>
                logger.info(s" ordinal ${snapshot.ordinal} for hash ${h}")
                snapshot.toHashed[F].map(_.copy(hash = Hash(h)))
              }
              .map((_, ts))
        }
        .prefetchN(reindexerConf.s3Parallelism * 2)
        .evalMap { case (hashedSnapshot, ts) =>
          val reversedStateChannelSnapshots = hashedSnapshot.signed.value.stateChannelSnapshots.map {
            case (address, snapshots) =>
              address -> snapshots.reverse
          }
          val currencySnapshots = reversedStateChannelSnapshots.toList.traverse { case (address, ccys) =>
            ccys.toList
              .traverse(bin => deserialize[Signed[CurrencyIncrementalSnapshot]](bin).map(_.map((_, bin))))
              .flatMap {
                _.flatten.traverse { case (s, bin) =>
                  HasherSelector[F]
                    .forOrdinal(hashedSnapshot.ordinal) { implicit hasher =>
                      s.toHashed.map((address, _, bin))
                    }
                }
              }
          }.map(_.flatten)
          currencySnapshots.map(cs => (hashedSnapshot, cs, ts))
        }
    }
      .parEvalMap(reindexerConf.dbParallelism) { case (gsSnapshot, ccySnapshots, ts) =>
        val hasher = HasherSelector[F].getForOrdinal(gsSnapshot.ordinal)
        logger.info(s"Consumer: Processing snapshot ${gsSnapshot.ordinal}") >>
          retryF(
            store(gsSnapshot, ccySnapshots, ts, hasher).timedLog(
              s"Consumer: processed snapshot ${gsSnapshot.ordinal.value}"
            )
          ).handleErrorWith { e =>
            logger.error(e)(
              s"Consumer: unrecoverable error processing snapshot ${gsSnapshot.ordinal}"
            ) *> e.raiseError[F, Unit]
          }.as(gsSnapshot)
      }
      .void

  }

}
