package org.constellation.snapshotstreaming.s3

import java.io.ByteArrayInputStream

import cats.Applicative
import cats.effect.{Async, Resource}
import cats.syntax.contravariantSemigroupal._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.show._

import org.tessellation.ext.kryo._
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.GlobalIncrementalSnapshot
import org.tessellation.security.Hashed

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.constellation.snapshotstreaming.Configuration
import org.typelevel.log4cats.slf4j.Slf4jLogger

trait S3DAO[F[_]] {
  def uploadSnapshot(snapshot: Hashed[GlobalIncrementalSnapshot]): F[Unit]
}

object S3DAO {

  def make[F[_]: Async: KryoSerializer](config: Configuration): Resource[F, S3DAO[F]] =
    Resource.make {
      Applicative[F].pure {
        val emptyBuilder = AmazonS3ClientBuilder
          .standard()

        (config.s3ApiEndpoint, config.s3ApiRegion).mapN { case (endpoint, region) =>
          emptyBuilder.withEndpointConfiguration(new EndpointConfiguration(endpoint, region))
        }
          .getOrElse(emptyBuilder.withRegion(config.bucketRegion))
          .withPathStyleAccessEnabled(config.s3ApiPathStyleEnabled.getOrElse(false).booleanValue())
      }.flatMap { builder =>
        Async[F].delay(builder.build())
      }
    }(c => Async[F].delay(c.shutdown()))
      .map(make(config, _))

  def make[F[_]: Async: KryoSerializer](config: Configuration, s3Client: AmazonS3): S3DAO[F] = new S3DAO[F] {

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

  }

}
