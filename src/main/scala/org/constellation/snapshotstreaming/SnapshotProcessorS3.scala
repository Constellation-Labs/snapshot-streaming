package org.constellation.snapshotstreaming

import cats.Parallel
import cats.data.EitherT
import cats.effect._
import cats.effect.implicits.clockOps
import cats.effect.std.{Console, Random}
import cats.syntax.all._
import com.aayushatharva.brotli4j.decoder.{Decoder => BrotliDecoder}
import com.sksamuel.elastic4s.ElasticApi.{boolQuery, fieldSort, matchAllQuery, search, termQuery}
import com.sksamuel.elastic4s.requests.searches.{SearchHit, SearchRequest}
import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive
import fs2.Stream
import fs2.io.file.Files
import fs2.io.net.Network
import io.circe.Decoder
import io.circe.jawn.JawnParser
import org.constellation.snapshotstreaming.db.{SnapshotDAO, SnapshotDBStream}
import org.constellation.snapshotstreaming.mapper.{CurrencySnapshotMapper, GlobalSnapshotMapper}
import org.constellation.snapshotstreaming.opensearch.OpensearchDAO
import org.constellation.snapshotstreaming.s3.S3DAO
import org.constellation.snapshotstreaming.schema.schema.{GlobalData, MetagraphData}
import org.tessellation.currency.schema.currency._
import org.tessellation.json.JsonSerializer
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.address.Address
import org.tessellation.schema.epoch.EpochProgress
import org.tessellation.schema.height.{Height, SubHeight}
import org.tessellation.schema.semver.SnapshotVersion
import org.tessellation.schema.transaction.RewardTransaction
import org.tessellation.schema.{BlockAsActiveTip, GlobalIncrementalSnapshot, SnapshotOrdinal, SnapshotTips}
import org.tessellation.security._
import org.tessellation.security.hash.Hash
import org.tessellation.security.signature.Signed
import org.tessellation.statechannel.StateChannelSnapshotBinary
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.otel4s.trace.Tracer

import java.time.{Instant, LocalDateTime, ZoneId}
import scala.collection.immutable.{SortedMap, SortedSet}
import scala.reflect.ClassTag

trait SnapshotProcessorS3[F[_]] {
  val runtime: Stream[F, Unit]
}

object SnapshotProcessorS3 {

  def make[F[
    _
  ] : Async : Parallel : KryoSerializer : JsonSerializer : SecurityProvider : Random : HasherSelector : Network : Files : Tracer : Console](
                                                                                                                                             configuration: SnapshotStreamingConfig,
                                                                                                                                             txHasher: Hasher[F]
                                                                                                                                           ): Resource[F, SnapshotProcessorS3[F]] =
    for {
      s3DAO <- S3DAO.make[F](configuration.s3)
      opensearchDAO <- OpensearchDAO.make[F](configuration.opensearch)
      sessionPool <- db.session[F](configuration.db)
      snapshotDAO = SnapshotDAO.make[F](sessionPool)
      snapshotDBStream = SnapshotDBStream.make[F](sessionPool)
    } yield make(
      configuration,
      s3DAO,
      snapshotDAO,
      opensearchDAO,
      snapshotDBStream,
      GlobalSnapshotMapper.make(),
      CurrencySnapshotMapper.make(),
      txHasher
    )

  def make[F[_] : Async : Parallel : HasherSelector : KryoSerializer : JsonSerializer](
                                                                                        configuration: SnapshotStreamingConfig,
                                                                                        s3DAO: S3DAO[F],
                                                                                        snapshotDAO: SnapshotDAO[F],
                                                                                        opensearchDAO: OpensearchDAO[F],
                                                                                        snapshotDBStream: SnapshotDBStream[F],
                                                                                        globalMapper: GlobalSnapshotMapper[F],
                                                                                        currencyMapper: CurrencySnapshotMapper[F],
                                                                                        txHasher: Hasher[F]
                                                                                      ): SnapshotProcessorS3[F] = new SnapshotProcessorS3[F] {
    private implicit val logger = Slf4jLogger.getLogger[F]

    private def storeInPostgres(global: GlobalData, metagraph: MetagraphData) =
      // (
      // snapshotDAO.insertGlobalData(global, metagraph.snapshots.size) >>
      snapshotDAO
        .insertMetagraphData(global.snapshot.hash, metagraph)
        .whenA(metagraph.snapshots.nonEmpty)
        // )
        .timed
        .flatMap { t =>
          logger
            .info(
              s"Snapshot ${global.snapshot.ordinal} (hash: ${global.snapshot.hash.show}) sent to postgres in ${t._1.toSeconds} s."
            ) >>
            logger
              .info(s"Metagraph Snapshots for currencies ${metagraph.snapshots.map(_.identifier)}  sent to postgres.")
              .handleErrorWith(s => logger.error(s)("Error in database layer") >> s.raiseError[F, Unit])
        }

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
      mapSnapshots(snapshot, ccys, d, hasher).flatMap { case (globalData, metagraphData) =>
        Async[F].delay {
          if (metagraphData.snapshots.size != snapshot.stateChannelSnapshots.map(_._2.size).sum)
            throw new Exception(s"Missing MG snapshots for ${snapshot.stateChannelSnapshots}")
          else ()
        } >>
          storeInPostgres(globalData, metagraphData)
      }.void

    val reindexerConf = configuration.reindexer.get

    def deserialize[A: Decoder](content: Array[Byte]): F[Either[io.circe.Error, A]] = {
      val brotliData = BrotliDecoder.decompress(content).getDecompressedData
      val byteData = if (brotliData == null) content else brotliData
      JawnParser(false).decodeByteArray[A](byteData).pure
    }

    def currencySnapshotSearchRequest(currencyId: String, ordinal: Long) = {
      val q = search(configuration.opensearch.indexes.currency.snapshots)
        .query(
          boolQuery().must(
            termQuery("identifier", currencyId),
            termQuery("data.ordinal", ordinal)
          )
        )
      opensearchDAO
        .singleQuery(q, extractHash)
        .map(_.get)

    }
//
//    def hitMapper(hit: SearchHit) =
//      hit.sourceAsMap.get("data.hash").map(_.toString)

    def extractHash(hit: SearchHit): Option[String] = {
      import io.circe.parser._
      import io.circe.Json
      parse(hit.sourceAsString).toOption.flatMap { json =>
        json.hcursor.downField("data").get[String]("hash").toOption
      }
    }

    val runtime: Stream[F, Unit] = {

      val startAfterOrdinal = reindexerConf.startAfterOrdinal

      snapshotDBStream
        .hashes(startAfterOrdinal)
        // .parEvalMap(reindexerConf.s3Parallelism) { case (ordinal, h, ts) =>
        .evalMap { case (ordinal, h, ts) =>
          val snapshotOrdinal = SnapshotOrdinal(ordinal).get
          implicit val hasher = HasherSelector[F].getForOrdinal(snapshotOrdinal)
          logger.info(s"Downloading hash ${h} from S3") >>
            s3DAO
              .downloadSnapshot(Hash(h), hasher.getLogic(snapshotOrdinal))
              .map(s => (ordinal, s, h, ts).some)
              .handleErrorWith { e =>
                logger.warn(s"Can't download/deserialize snapshot $h message: $e").map(_ => None)
              }
        }
        .unNone
        .evalMap { case (ordinal, snapshot, h, ts) =>
          val snapshotOrdinal = SnapshotOrdinal(ordinal).get
          implicit val hasher = HasherSelector[F].getForOrdinal(snapshotOrdinal)
          logger.info(s" ordinal ${snapshot.ordinal} for hash ${h}") >>
            snapshot.toHashed[F].map(s => (s.copy(hash = Hash(h)), ts))
        }
        // .prefetchN(reindexerConf.s3Parallelism * 2)
        .evalMap { case (hashedSnapshot, ts) =>
          val reversedStateChannelSnapshots = hashedSnapshot.signed.value.stateChannelSnapshots.map {
            case (address, snapshots) =>
              address -> snapshots.reverse
          }
          reversedStateChannelSnapshots.toList.flatTraverse { case (address, ccys) =>
              val snapshotOrdinal = hashedSnapshot.ordinal
              implicit val hasher = HasherSelector[F].getForOrdinal(snapshotOrdinal)
              ccys.toList.traverse { snapshotBinary =>
                val bin = snapshotBinary.value.content
                val eitherCcy = EitherT(deserialize[Signed[CurrencySnapshot]](bin))
                  .flatMapF(signedSnapshot =>
                    signedSnapshot.toHashed.flatMap { hs =>
                      println("CSV")
                      CurrencyIncrementalSnapshot
                        .fromCurrencySnapshot(hs.signed.value)
                        .map(cis => hs.copy(signed = hs.signed.copy(value = cis)).asRight[Throwable])
                    }
                  )
                  .orElse(
                    EitherT(deserialize[Signed[CurrencySnapshotV1]](bin)).flatMapF(signedSnapshot =>
                      signedSnapshot.toHashed.flatMap { hs =>
                        println("CSV1")
                        CurrencyIncrementalSnapshot
                          .fromCurrencySnapshot(hs.signed.value.toCurrencySnapshot)
                          .map(cis => hs.copy(signed = hs.signed.copy(value = cis)).asRight[Throwable])
                      }
                    )
                  )
                  .orElse(
                    EitherT(deserialize[Signed[CurrencyIncrementalSnapshotV1]](bin)).flatMapF(signedSnapshot =>
                      signedSnapshot.toHashed.map { hs =>
                        println("CISV1")
                        hs.copy(signed = hs.signed.copy(value = hs.signed.value.toCurrencyIncrementalSnapshot))
                          .asRight[Throwable]
                      }
                    )
                  )
                  .orElse(
                    EitherT(deserialize[Signed[CurrencyIncrementalSnapshot]](bin)).flatMapF(signedSnapshot =>
                      signedSnapshot.toHashed.map(_.asRight[Throwable])
                    )
                  )
                eitherCcy.value.flatMap { case Right(ccySnapshot) =>
                  currencySnapshotSearchRequest(address.value.value, ccySnapshot.ordinal.value.value).map { hash =>
                    val newSnapshot = ccySnapshot.copy(hash = Hash(hash))
                    (address, newSnapshot, snapshotBinary)
                  }
                }
              }
              // .collect { case Right(ccySnapshot) => ccySnapshot }.map(x => (address, x, snapshotBinary))
            } // .traverseCollect {case Right(ccySnapshot) => currencySnapshotSearchRequest(address.value.value, ccySnapshot.ordinal.value.value).map { hash =>  ccySnapshot.copy(hash = Hash(hash))}}
            // currencySnapshots
            .map(cs => (hashedSnapshot, cs, ts))
        }
        // .parEvalMap(reindexerConf.dbParallelism) { case (gsSnapshot, ccySnapshots, ts) =>
        .evalMap { case (gsSnapshot, ccySnapshots, ts) =>
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

}

@derive(eqv, show, encoder, decoder)
case class CurrencySnapshotV1(
  ordinal: SnapshotOrdinal,
  height: Height,
  subHeight: SubHeight,
  lastSnapshotHash: Hash,
  blocks: SortedSet[BlockAsActiveTip],
  rewards: SortedSet[RewardTransaction],
  tips: SnapshotTips,
  info: Option[CurrencySnapshotInfoV1],
  epochProgress: EpochProgress,
  data: List[Int],
  version: SnapshotVersion
) {

  def toCurrencySnapshot: CurrencySnapshot =
    CurrencySnapshot(
      ordinal,
      height,
      subHeight,
      lastSnapshotHash,
      blocks,
      rewards,
      tips,
      info.getOrElse(CurrencySnapshotInfoV1(SortedMap.empty, SortedMap.empty)),
      epochProgress,
      None,
      version
    )

}
