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

  def mkElastiDAO(elasticResult: => Stream[IO, Unit]) = new ElasticSearchDAO[IO] {

    def sendToOpenSearch(bulkRequest: BulkRequest): Stream[IO, Unit] = bulkRequest.requests.toList.head match {
      case head: UpdateRequest if head.id == "1" => Stream.emit(())
      case _                                     => elasticResult
    }

  }

  def mkSnapshotDAO(elastiDAO: ElasticSearchDAO[IO]) = SnapshotDAO.make(mkUpdateBuilder(), elastiDAO)

  test("return ordinal of fully sent snapshot") {
    val actual = mkSnapshotDAO(mkElastiDAO(Stream.emit(()))).sendSnapshotToOpenSearch(globalSnapshot(345L))

    actual
      .take(1)
      .compile
      .toList
      .map(l => expect.same(l, List(345L)))
  }

  test("pass an error when happen") {
    val actual = mkSnapshotDAO(mkElastiDAO(Stream.raiseError(new Exception("TEST ERROR!"))))
      .sendSnapshotToOpenSearch(globalSnapshot(345L))

    actual
      .take(1)
      .compile
      .toList
      .attempt
      .map(_.left.map(_.getMessage))
      .map(s => expect.same(s, Left("TEST ERROR!")))
  }

}
