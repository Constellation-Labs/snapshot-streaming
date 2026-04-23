package org.constellation.snapshotstreaming.s3

import cats.Applicative
import cats.effect.{Async, Resource}
import cats.syntax.all._
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.model.{ListObjectsV2Request, ObjectMetadata}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import io.circe.syntax._
import io.constellationnetwork.ext.kryo._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security._
import io.constellationnetwork.security.Hashed
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import fs2.{Stream, io}
import org.constellation.snapshotstreaming.storage.SnapshotWithState
import org.constellation.snapshotstreaming.{S3Config, retryF}
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import scala.jdk.CollectionConverters._

trait S3DAO[F[_]] {
  def uploadSnapshot(snapshot: Hashed[GlobalIncrementalSnapshot], hashLogic: HashLogic): F[Unit]
  def uploadState(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit]
  def uploadCombined(snapshotWithState: SnapshotWithState): F[Unit]
  def downloadSnapshot(hash: Hash): F[Signed[GlobalIncrementalSnapshot]]
  def downloadState(ordinal: SnapshotOrdinal, hash: Hash): F[GlobalSnapshotInfo]
  def downloadCombined(ordinal: SnapshotOrdinal, hash: Hash): F[SnapshotWithState]
  def pruneStatesAtOrdinal(ordinal: SnapshotOrdinal): F[Unit]
  def pruneCombinedAtOrdinal(ordinal: SnapshotOrdinal): F[Unit]
  def metadata(hash: Hash): F[ObjectMetadata]
}

object S3DAO {

  def make[F[_]: Async: KryoSerializer: JsonSerializer](config: S3Config): Resource[F, S3DAO[F]] =
    Resource.make {
      Applicative[F].pure {
        val emptyBuilder = AmazonS3ClientBuilder
          .standard()

        (config.api.endpoint, config.api.region).mapN { case (endpoint, region) =>
          emptyBuilder.withEndpointConfiguration(new EndpointConfiguration(endpoint, region))
        }
          .getOrElse(emptyBuilder.withRegion(config.bucketRegion))
          .withPathStyleAccessEnabled(config.api.pathStyleEnabled.getOrElse(false).booleanValue())
      }.flatMap { builder =>
        Async[F].delay(builder.build())
      }
    }(c => Async[F].delay(c.shutdown()))
      .map(make(config, _))

  def make[F[_]: Async: KryoSerializer](config: S3Config, s3Client: AmazonS3)(implicit
    jsonSerializer: JsonSerializer[F]
  ): S3DAO[F] = new S3DAO[F] {

    private implicit val logger = Slf4jLogger.getLogger[F]

    private def statesPrefix = s"${config.bucketDir}/states/"
    private def combinedPrefix = s"${config.bucketDir}/combined/"
    private def stateKey(ordinal: SnapshotOrdinal, hash: Hash) = s"$statesPrefix${ordinal.value.value}-$hash"
    private def combinedKey(ordinal: SnapshotOrdinal, hash: Hash) = s"$combinedPrefix${ordinal.value.value}-$hash"

    private def gzip(bytes: Array[Byte]): Array[Byte] = {
      val baos = new ByteArrayOutputStream()
      val gzos = new GZIPOutputStream(baos)
      try gzos.write(bytes)
      finally gzos.close()
      baos.toByteArray
    }

    private def gunzip(bytes: Array[Byte]): Array[Byte] = {
      val in = new GZIPInputStream(new ByteArrayInputStream(bytes))
      try in.readAllBytes()
      finally in.close()
    }

    private def buildMetadata(snapshot: Hashed[GlobalIncrementalSnapshot], bytes: Array[Byte]): ObjectMetadata = {
      val md = new ObjectMetadata()
      md.setContentLength(bytes.length.toLong)
      md.setContentEncoding("gzip")
      md.setContentType("application/json")
      md.addUserMetadata("ordinal", snapshot.ordinal.value.value.toString)
      md.addUserMetadata("hash", snapshot.hash.value)
      md
    }

    private def putBytes(key: String, payload: Array[Byte], metadata: ObjectMetadata): F[Unit] =
      Async[F].delay {
        val is = new ByteArrayInputStream(payload)
        s3Client.putObject(config.bucketName, key, is, metadata)
      }.void

    private def getBytes(key: String): F[Array[Byte]] = {
      val resource = for {
        s3Object <- Resource.eval(Async[F].delay(s3Client.getObject(config.bucketName, key)))
        dataStreamR <- Resource.fromAutoCloseable(Async[F].delay(s3Object.getObjectContent))
      } yield dataStreamR
      Stream
        .resource(resource)
        .flatMap(inputStream => io.readInputStream(Async[F].delay(inputStream.getDelegateStream), chunkSize = 4096))
        .compile
        .to(Array)
    }

    private def listKeysWithPrefix(prefix: String): F[List[String]] =
      Async[F].delay {
        val req = new ListObjectsV2Request().withBucketName(config.bucketName).withPrefix(prefix)
        val result = s3Client.listObjectsV2(req)
        result.getObjectSummaries.asScala.toList.map(_.getKey)
      }

    private def deleteKeys(keys: List[String]): F[Unit] =
      keys.traverse_(k => Async[F].delay(s3Client.deleteObject(config.bucketName, k)))

    def uploadSnapshot(snapshot: Hashed[GlobalIncrementalSnapshot], hashLogic: HashLogic): F[Unit] =
      retryF(for {
        arr <- hashLogic match {
          case JsonHash => jsonSerializer.serialize(snapshot.signed)
          case KryoHash => snapshot.signed.toBinaryF
        }
        is = new ByteArrayInputStream(arr)
        keyName = s"${config.bucketDir}/${snapshot.hash}"
        _ <- Async[F].delay(s3Client.putObject(config.bucketName, keyName, is, new ObjectMetadata()))
        _ <- logger.info(
          s"Snapshot ${snapshot.ordinal.value.value} (hash: ${snapshot.hash.show.take(8)}) uploaded to s3."
        )
      } yield ())

    def uploadState(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] =
      retryF(for {
        json <- jsonSerializer.serialize(state)
        compressed <- Async[F].delay(gzip(json))
        key = stateKey(snapshot.ordinal, snapshot.hash)
        md = buildMetadata(snapshot, compressed)
        _ <- putBytes(key, compressed, md)
        _ <- logger.info(
          s"State for snapshot ${snapshot.ordinal.value.value} (hash: ${snapshot.hash.show.take(8)}) uploaded to s3."
        )
      } yield ())

    def uploadCombined(snapshotWithState: SnapshotWithState): F[Unit] = {
      val snapshot = snapshotWithState.snapshot
      retryF(for {
        json <- Async[F].delay(snapshotWithState.asJson.noSpaces.getBytes("UTF-8"))
        compressed <- Async[F].delay(gzip(json))
        key = combinedKey(snapshot.ordinal, snapshot.hash)
        md = buildMetadata(snapshot, compressed)
        _ <- putBytes(key, compressed, md)
        _ <- logger.info(
          s"Combined (snapshot+state) ${snapshot.ordinal.value.value} (hash: ${snapshot.hash.show.take(8)}) uploaded to s3."
        )
      } yield ())
    }

    def downloadSnapshot(hash: Hash): F[Signed[GlobalIncrementalSnapshot]] = {
      val keyName = s"${config.bucketDir}/${hash}"
      val resource = for {
        s3Object <- Resource.eval(Async[F].delay(s3Client.getObject(config.bucketName, keyName)))
        dataStreamR <- Resource.fromAutoCloseable(Async[F].delay(s3Object.getObjectContent))
      } yield dataStreamR
      Stream
        .resource(resource)
        .flatMap(inputStream => io.readInputStream(Async[F].delay(inputStream.getDelegateStream), chunkSize = 4096))
        .compile
        .to(Array)
        .flatMap(d => d.fromBinaryF[Signed[GlobalIncrementalSnapshot]])

    }

    def downloadState(ordinal: SnapshotOrdinal, hash: Hash): F[GlobalSnapshotInfo] =
      for {
        compressed <- getBytes(stateKey(ordinal, hash))
        json <- Async[F].delay(gunzip(compressed))
        either <- jsonSerializer.deserialize[GlobalSnapshotInfo](json)
        state <- either.liftTo[F]
      } yield state

    def downloadCombined(ordinal: SnapshotOrdinal, hash: Hash): F[SnapshotWithState] =
      for {
        compressed <- getBytes(combinedKey(ordinal, hash))
        json <- Async[F].delay(gunzip(compressed))
        parsed <- Async[F].fromEither(_root_.io.circe.parser.decode[SnapshotWithState](new String(json, "UTF-8")))
      } yield parsed

    def pruneStatesAtOrdinal(ordinal: SnapshotOrdinal): F[Unit] =
      listKeysWithPrefix(s"$statesPrefix${ordinal.value.value}-").flatMap { keys =>
        deleteKeys(keys) >>
          logger.info(s"Pruned ${keys.size} state object(s) at ordinal ${ordinal.value.value} from s3.").whenA(keys.nonEmpty)
      }

    def pruneCombinedAtOrdinal(ordinal: SnapshotOrdinal): F[Unit] =
      listKeysWithPrefix(s"$combinedPrefix${ordinal.value.value}-").flatMap { keys =>
        deleteKeys(keys) >>
          logger.info(s"Pruned ${keys.size} combined object(s) at ordinal ${ordinal.value.value} from s3.").whenA(keys.nonEmpty)
      }

    def metadata(hash: Hash) =
      Async[F].delay(s3Client.getObjectMetadata(config.bucketName, s"${config.bucketDir}/${hash}"))

  }

}
