package org.constellation.snapshotstreaming.node

import cats.effect.Concurrent

import org.tessellation.dag.snapshot.GlobalSnapshot
import org.tessellation.kryo.KryoSerializer
import org.tessellation.security.signature.Signed

import io.circe.generic.auto._
import org.http4s.client.Client
import org.http4s.{Method, Request, Uri}

trait NodeClient[F[_]] {
  def uri: Uri
  def getLatestOrdinal: F[SnapshotOrdinal]
  def getSnapshot(ordinal: SnapshotOrdinal): F[Signed[GlobalSnapshot]]
}

object NodeClient {

  def make[F[_]: Concurrent: KryoSerializer](
    client: Client[F],
    nodeUri: Uri
  ) = new NodeClient[F] {

    val uri: Uri = nodeUri.addPath("global-snapshots/")

    def getLatestOrdinal = {
      import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
      val req = Request[F](Method.GET, uri.addPath("latest/ordinal"))
      client.expect[SnapshotOrdinal](req)
    }

    def getSnapshot(ordinal: SnapshotOrdinal) = {
      import org.tessellation.ext.codecs.BinaryCodec.decoder
      val req = Request[F](Method.GET, uri / ordinal.value)
      client.expect[Signed[GlobalSnapshot]](req)
    }

  }

}
