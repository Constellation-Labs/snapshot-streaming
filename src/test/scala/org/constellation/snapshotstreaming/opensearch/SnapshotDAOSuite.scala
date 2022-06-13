package org.constellation.snapshotstreaming.opensearch

import java.util.Date

import cats.effect.IO

import org.tessellation.dag.snapshot.GlobalSnapshot
import org.tessellation.security.signature.Signed

import com.sksamuel.elastic4s.requests.bulk.BulkRequest
import com.sksamuel.elastic4s.requests.update.UpdateRequest
import eu.timepit.refined.auto._
import fs2.Stream
import org.constellation.snapshotstreaming.data._
import weaver.SimpleIOSuite

object SnapshotDAOSuite extends SimpleIOSuite {

  def mkUpdateBuilder() = new UpdateRequestBuilder[IO] {

    override def bulkUpdateRequests(
      globalSnapshot: Signed[GlobalSnapshot],
      timestamp: Date
    ): IO[Seq[Seq[UpdateRequest]]] =
      IO.delay(Seq(Seq(UpdateRequest("testIndex1", "1")), Seq(UpdateRequest("testIndex2", "2"))))

  }

  def mkOpensearchDAO(opensearchResult: => Stream[IO, Unit]) = new OpensearchDAO[IO] {

    def sendToOpensearch(bulkRequest: BulkRequest): Stream[IO, Unit] = bulkRequest.requests.toList.head match {
      case head: UpdateRequest if head.id == "1" => Stream.emit(())
      case _                                     => opensearchResult
    }

  }

  def mkSnapshotDAO(opensearchDAO: OpensearchDAO[IO]) = SnapshotDAO.make(mkUpdateBuilder())(opensearchDAO)

  test("return ordinal of fully sent snapshot") {
    val actual = mkSnapshotDAO(mkOpensearchDAO(Stream.emit(()))).sendSnapshotToOpensearch(globalSnapshot(345L))

    actual
      .take(1)
      .compile
      .toList
      .map(l => expect.same(l, List(345L)))
  }

  test("pass an error when happen") {
    val actual = mkSnapshotDAO(mkOpensearchDAO(Stream.raiseError(new Exception("TEST ERROR!"))))
      .sendSnapshotToOpensearch(globalSnapshot(345L))

    actual
      .take(1)
      .compile
      .toList
      .attempt
      .map(_.left.map(_.getMessage))
      .map(s => expect.same(s, Left("TEST ERROR!")))
  }

}
