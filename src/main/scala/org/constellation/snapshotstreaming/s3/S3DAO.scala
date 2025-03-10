package org.constellation.snapshotstreaming.s3

import cats.Applicative
import cats.data.NonEmptySet
import cats.effect.{Async, Resource}
import org.tessellation.ext.kryo._
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.GlobalIncrementalSnapshot
import org.tessellation.security.Hashed
import org.tessellation.security.signature.signature.SignatureProof
import cats.syntax.all._
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.constellation.snapshotstreaming.S3Config
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.tessellation.security.signature.Signed
import org.tessellation.security.hash.Hash
import fs2.{Stream, io}

import java.io.ByteArrayInputStream
import scala.collection.immutable.SortedSet

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
      Async[F].unit
      /*for {
        arr <- snapshot.signed.toBinaryF
        is = new ByteArrayInputStream(arr)
        keyName = s"${config.bucketDir}/${snapshot.hash}"
        _ <- Async[F].delay(s3Client.putObject(config.bucketName, keyName, is, new ObjectMetadata()))
        _ <- logger.info(
          s"Snapshot ${snapshot.ordinal.value.value} (hash: ${snapshot.hash.show.take(8)}) uploaded to s3."
        )
      } yield ()*/

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
        .flatMap( _.fromBinaryF[Signed[GlobalIncrementalSnapshot]])

    }

    private def safeProofSet(proofs : NonEmptySet[SignatureProof]): NonEmptySet[SignatureProof] ={
      NonEmptySet.fromSetUnsafe(SortedSet.from(proofs.toSortedSet)(SignatureProof.OrderingInstance))
    }

    def metadata(hash: Hash) =
      Async[F].delay(s3Client.getObjectMetadata(config.bucketName, s"${config.bucketDir}/${hash}"))

  }

}
