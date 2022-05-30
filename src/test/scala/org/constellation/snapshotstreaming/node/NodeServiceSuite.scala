package org.constellation.snapshotstreaming

import cats.Show
import cats.effect.IO
import cats.kernel.Order
import cats.syntax.contravariant._

import scala.collection.mutable.Map

import org.tessellation.dag.snapshot.{GlobalSnapshot, SnapshotOrdinal => OriginalSnapshotOrdinal}
import org.tessellation.security.signature.Signed

import eu.timepit.refined.auto._
import fs2.Stream
import org.constellation.snapshotstreaming.data._
import org.constellation.snapshotstreaming.node.{NodeDownload, NodeService, SnapshotOrdinal}
import org.scalacheck.Gen
import weaver.SimpleIOSuite
import weaver.scalacheck.Checkers

object NodeServiceSuite extends SimpleIOSuite with Checkers {

  case class TestSnapshot(ordinal: SnapshotOrdinal)

  def mkNodeDownload(ordinals: List[Long], ordinalsFromOtherNodes: List[SnapshotOrdinal] = Nil) =
    new NodeDownload[IO] {

      val callsPerKnownOrdinal: Map[SnapshotOrdinal, Int] = Map.from(ordinalsFromOtherNodes.map(s => (s, 0)))
      val limitToCallsForOrdinal = 1
      def downloadLatestOrdinal(): Stream[IO, SnapshotOrdinal] = Stream.emits(ordinals.map(SnapshotOrdinal(_)))

      def downloadSnapshot(ordinal: SnapshotOrdinal): Stream[IO, Either[SnapshotOrdinal, Signed[GlobalSnapshot]]] =
        if (ordinals.contains(ordinal.value))
          Stream(Right(globalSnapshot(ordinal.value)))
        else if (callsPerKnownOrdinal.updateWith(ordinal)(_.map(_ + 1)).exists(_ > limitToCallsForOrdinal))
          Stream.raiseError(new Exception("TEST ERROR"))
        else
          Stream(Left(ordinal))

    }

  def mkNodeService(nodeDownloadPool: List[NodeDownload[IO]]) =
    NodeService.make[IO](nodeDownloadPool)

  test("process all ordinals") {
    val expected = List[Long](0, 1, 2, 3)
    val actual = mkNodeService(List(mkNodeDownload(List(0, 1, 2, 3)))).getSnapshots(0, Nil)
    actual.assert(expected)
  }

  test("process all ordinals when many times returned the same latest ordinal") {
    val expected = List[Long](0, 1, 2, 3)
    val actual = mkNodeService(List(mkNodeDownload(List(0, 1, 1, 1, 2, 2, 3, 3, 3)))).getSnapshots(0, Nil)
    actual.assert(expected)
  }

  test("process all ordinals from many nodes") {
    val expected = List[Long](0, 1, 2, 3)
    val actual =
      mkNodeService(List(List(4L), List(3L), List(0L, 1, 2)).map(mkNodeDownload(_, Nil))).getSnapshots(0, Nil)
    actual.assert(expected)
  }

  test("process all ordinals starting from from initial") {
    val expected = List[Long](2, 3, 4)
    val actual = mkNodeService(List(mkNodeDownload(List(1, 2, 3, 4)))).getSnapshots(2, Nil)
    actual.assert(expected)
  }

  test("process all ordinals starting from from initial and include gaps") {
    val expected = List[Long](2, 5, 7, 8, 9)
    val actual = mkNodeService(List(mkNodeDownload(List(1, 2, 3, 4, 5, 6, 7, 8, 9)))).getSnapshots(7, List(2, 5))
    actual.assert(expected)
  }

  test("should not process further when cannot download some snapshot") {
    val actual = mkNodeService(List(mkNodeDownload(List(0, 1, 2, 4, 9)))).getSnapshots(0, Nil)
    actual.assertError("Couldn't download snapshot no. 3 from any node.")
  }

  test("all snapshots downloaded from different nodes") {
    forall(ordinalsPerNodeGen) { ordinalsPerNode: List[List[Long]] =>
      val expected: List[Long] =
        ordinalsPerNode.flatten.distinct.sorted

      val nodeDownloads = ordinalsPerNode.map(ordinals => mkNodeDownload(ordinals))
      val actual = mkNodeService(nodeDownloads).getSnapshots(0, Nil)

      actual.assert(expected)
    }
  }

  implicit class AssertGlobalSnapshotStream(s: Stream[IO, Signed[GlobalSnapshot]]) {

    def assert(expected: List[Long]) =
      s
        .take(expected.size)
        .compile
        .toList
        .map(_.map(_.ordinal.value.value))
        .map(actual => expect.same(actual, expected))

    def assertError(msg: String) =
      s.compile.drain.attempt
        .map(_.left.map(_.getMessage))
        .map(s => expect.same(s, Left(msg)))

  }

  implicit val snapshotOrdinalShow: Show[SnapshotOrdinal] = Show[Long].contramap(_.value)
  implicit val order: Order[Signed[GlobalSnapshot]] = Order[OriginalSnapshotOrdinal].contramap(_.ordinal)
  implicit val ordering: Ordering[Signed[GlobalSnapshot]] = order.toOrdering

  val snapshotOrdinalsGen = for {
    numberOfOrdinals <- Gen.chooseNum(0L, 10)
  } yield (0L to numberOfOrdinals).toList

  val ordinalsPerNodeGen: Gen[List[List[Long]]] = for {
    numberOfNodes <- Gen.choose(1, 3)
    nodesOrdinals <- Gen.listOfN(numberOfNodes, snapshotOrdinalsGen)
  } yield nodesOrdinals

}
