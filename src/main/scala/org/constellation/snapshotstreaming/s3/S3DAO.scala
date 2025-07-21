package org.constellation.snapshotstreaming.s3

import cats.{Applicative, Parallel}
import cats.effect.{Async, Resource}
import cats.syntax.all._
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import fs2.{Stream, io}
import org.constellation.snapshotstreaming.{S3Config, retryF}
import org.tessellation.json.JsonSerializer
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.{GlobalIncrementalSnapshot, GlobalSnapshot}
import org.tessellation.security.hash.Hash
import org.tessellation.security.signature.Signed
import org.tessellation.security.{HashLogic, Hashed, Hasher, HasherSelector, JsonHash, KryoHash}
import org.tessellation.ext.kryo._
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.io.ByteArrayInputStream

trait S3DAO[F[_]] {
  def downloadSnapshot(hash: Hash, hashLogic: HashLogic): F[Signed[GlobalIncrementalSnapshot]]
  def metadata(hash: Hash): F[ObjectMetadata]
}

object S3DAO {

  def make[F[_]: Async: KryoSerializer: JsonSerializer: HasherSelector: Parallel](
    config: S3Config
  ): Resource[F, S3DAO[F]] =
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

  def make[F[_]: Async: KryoSerializer: Parallel](config: S3Config, s3Client: AmazonS3)(implicit
    jsonSerializer: JsonSerializer[F],
    hs: HasherSelector[F]
  ): S3DAO[F] = new S3DAO[F] {

    private implicit val logger = Slf4jLogger.getLogger[F]

    def downloadSnapshot(hash: Hash, hashLogic: HashLogic): F[Signed[GlobalIncrementalSnapshot]] = {
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
        .flatMap(d =>
          hashLogic match {
            case JsonHash => jsonSerializer.deserialize[Signed[GlobalIncrementalSnapshot]](d).flatMap(_.liftTo[F])
            case KryoHash =>
              implicit val hasher = Hasher.forKryo[F]
              d.fromBinaryF[Signed[GlobalIncrementalSnapshot]]
                .orElse(d.fromBinaryF[Signed[GlobalSnapshot]].flatMap { case signed @ Signed(value, _) =>
                  GlobalIncrementalSnapshot.fromGlobalSnapshot(value).map(gis => signed.copy(value = gis))
                })
          }
        )

    }

    def metadata(hash: Hash) =
      Async[F].delay(s3Client.getObjectMetadata(config.bucketName, s"${config.bucketDir}/${hash}"))

  }

}
