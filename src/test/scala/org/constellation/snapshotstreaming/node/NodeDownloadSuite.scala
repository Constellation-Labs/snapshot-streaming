package org.constellation.snapshotstreaming.node

import cats.effect.IO

import org.tessellation.dag.snapshot.GlobalSnapshot
import org.tessellation.security.signature.Signed

import org.constellation.snapshotstreaming.Configuration
import org.constellation.snapshotstreaming.data._
import org.constellation.snapshotstreaming.node.{NodeClient, SnapshotOrdinal}
import org.http4s.Uri
import org.http4s.syntax.literals._
import weaver.SimpleIOSuite

object NodeDownloadSuite extends SimpleIOSuite {

  def mkNodeClient(isSnapshotFail: Boolean = false) = new NodeClient[IO] {
    override def uri: Uri = uri"http://1.1.1.1:80"
    override def getLatestOrdinal: IO[SnapshotOrdinal] = ???

    override def getSnapshot(ordinal: SnapshotOrdinal): IO[Signed[GlobalSnapshot]] =
      if (isSnapshotFail) IO.raiseError(new Exception("Error")) else IO.delay(globalSnapshot(ordinal.value))

  }

  def mkNodeDownload(nodeClient: NodeClient[IO]) = NodeDownload.make[IO](nodeClient, new Configuration())

  test("return downloaded snapshot") {
    val expected = List(Right(globalSnapshot(1)))
    val actual = mkNodeDownload(mkNodeClient()).downloadSnapshot(SnapshotOrdinal(1))
    actual
      .take(1)
      .compile
      .toList
      .map(s => expect.same(s, expected))
  }

  test("return snapshot ordinal when snapshot download fails") {
    val expected = List(Left(SnapshotOrdinal(1)))
    val actual = mkNodeDownload(mkNodeClient(true)).downloadSnapshot(SnapshotOrdinal(1))
    actual
      .take(1)
      .compile
      .toList
      .map(s => expect.same(s, expected))
  }

}
