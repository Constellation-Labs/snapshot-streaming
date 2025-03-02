package org.constellation.snapshotstreaming.s3

import cats.Applicative
import cats.effect.{Async, Resource}
import io.constellationnetwork.ext.kryo._
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.schema.GlobalIncrementalSnapshot
import io.constellationnetwork.security.Hashed
import cats.syntax.all._
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.constellation.snapshotstreaming.S3Config
import org.typelevel.log4cats.slf4j.Slf4jLogger
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.hash.Hash
import fs2.{Stream, io}

import java.io.ByteArrayInputStream

trait S3DAO[F[_]] {
  def uploadSnapshot(snapshot: Hashed[GlobalIncrementalSnapshot]): F[Unit]
  def downloadSnapshot(hash: Hash): F[Signed[GlobalIncrementalSnapshot]]
  def metadata(hash: Hash): F[ObjectMetadata]
}

object S3DAO {

  def make[F[_]: Async : KryoSerializer](config: S3Config): Resource[F, S3DAO[F]] =
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

  def make[F[_]: Async: KryoSerializer](config: S3Config, s3Client: AmazonS3): S3DAO[F] = new S3DAO[F] {

    private val logger = Slf4jLogger.getLogger[F]

    def uploadSnapshot(snapshot: Hashed[GlobalIncrementalSnapshot]): F[Unit] =
      for {
        arr <- snapshot.signed.toBinaryF
        is = new ByteArrayInputStream(arr)
        keyName = s"${config.bucketDir}/${snapshot.hash}"
        _ <- Async[F].delay(s3Client.putObject(config.bucketName, keyName, is, new ObjectMetadata()))
        _ <- logger.info(
          s"Snapshot ${snapshot.ordinal.value.value} (hash: ${snapshot.hash.show.take(8)}) uploaded to s3."
        )
      } yield ()

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
        .flatMap(_.fromBinaryF[Signed[GlobalIncrementalSnapshot]])
    }

    def metadata(hash: Hash) =
      Async[F].delay(s3Client.getObjectMetadata(config.bucketName, s"${config.bucketDir}/${hash}"))

  }

}
