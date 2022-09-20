package org.constellation.snapshotstreaming.s3


import java.io.ByteArrayInputStream

import cats.effect.{Async, Resource}
import cats.syntax.functor._
import cats.syntax.show._

import org.tessellation.dag.snapshot.GlobalSnapshot
import org.tessellation.ext.kryo._
import org.tessellation.kryo.KryoSerializer
import org.tessellation.security.signature.Signed

import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import fs2.Stream
import org.constellation.snapshotstreaming.Configuration
import org.typelevel.log4cats.slf4j.Slf4jLogger

trait S3DAO[F[_]] {
  def uploadSnapshot(snapshot: Signed[GlobalSnapshot]): Stream[F, Unit]
}

object S3DAO {

    def make[F[_]: Async: KryoSerializer](config: Configuration): Resource[F, S3DAO[F]] = {
        Resource.make(Async[F].delay(AmazonS3ClientBuilder
            .standard()
            .withRegion(config.bucketRegion)
            .build()))(c => Async[F].delay(c.shutdown()))
            .map(make(config, _))
    }

  def make[F[_]: Async: KryoSerializer](config: Configuration, s3Client: AmazonS3): S3DAO[F] = new S3DAO[F] {

    private val logger = Slf4jLogger.getLoggerFromClass[F](S3DAO.getClass)

    def uploadSnapshot(snapshot: Signed[GlobalSnapshot]): Stream[F, Unit] = {
        for {
            arr <- Stream.eval(snapshot.toBinaryF)
            is <- Stream.fromAutoCloseable(Async[F].delay(new ByteArrayInputStream(arr)))
            hash <- Stream.eval(snapshot.toHashed.map(_.hash))
            keyName = s"${config.bucketDir}/${hash.value}"
            _ <- Stream.eval(Async[F].delay(s3Client.putObject(config.bucketName, keyName, is, new ObjectMetadata())))
            .evalTap(_ => logger.info(s"Snapshot ${snapshot.ordinal.value.value} (hash: ${hash.show.take(8)}) uploaded to s3."))
        } yield ()
  }}

}
