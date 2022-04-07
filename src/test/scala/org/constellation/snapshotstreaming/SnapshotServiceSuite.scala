package org.constellation.snapshotstreaming

import java.util.concurrent.atomic.AtomicLong

import cats.effect.{IO, Ref}

import org.tessellation.dag.snapshot.GlobalSnapshot
import org.tessellation.security.signature.Signed

import eu.timepit.refined.auto._
import fs2.Stream
import org.constellation.snapshotstreaming.data._
import org.constellation.snapshotstreaming.node.NodeService
import org.constellation.snapshotstreaming.opensearch.SnapshotDAO
import weaver.SimpleIOSuite

object SnapshotServiceSuite extends SimpleIOSuite {

  def mkProcessedSnapshotsService(ref: Ref[IO, ProcessedSnapshots]) = new ProcessedSnapshotsService[IO] {

    override def initialState(): Stream[IO, ProcessedSnapshots] = Stream.eval(ref.get).head

    override def saveState(processedSnapshots: ProcessedSnapshots): Stream[IO, Unit] =
      Stream.eval(ref.set(processedSnapshots))

  }

  def mkNodeService(ordinals: List[Long], ordinalToFail: Option[Long] = None) = new NodeService[IO] {

    override def getSnapshots(
      startingOrdinal: Long,
      additionalOrdinals: Seq[Long]
    ): fs2.Stream[IO, Signed[GlobalSnapshot]] =
      Stream
        .emits(
          ordinals
            .filter(ord => startingOrdinal <= ord || additionalOrdinals.contains(ord))
            .map(globalSnapshot(_))
        )
        .flatMap(snap =>
          if (ordinalToFail.contains(snap.ordinal.value.value)) Stream.raiseError(new Throwable("NODE ERROR!"))
          else Stream.emit(snap)
        )

  }

  def mkSnapshotDAO(ordinalToFail: Option[Long] = None, failOnlyOnce: Boolean = false) = new SnapshotDAO[IO] {
    val counter = new AtomicLong(0)

    override def sendSnapshotToOpenSearch(snapshot: Signed[GlobalSnapshot]): fs2.Stream[IO, Long] =
      if (
        ordinalToFail.contains(
          snapshot.ordinal.value.value
        ) && (!failOnlyOnce || (failOnlyOnce && counter.getAndIncrement < 1))
      ) {
        Stream.raiseError(new Exception("DAO ERROR!"))
      } else Stream.emit(snapshot.ordinal.value.value)

  }

  def mkSnapshotService(
    nodeService: NodeService[IO],
    dao: SnapshotDAO[IO],
    processedService: ProcessedSnapshotsService[IO]
  ) =
    SnapshotService.make[IO, Signed[GlobalSnapshot]](nodeService, dao, processedService)

  test("process properly and save next ordinal") {
    {
      for {
        ref <- Ref[IO].of(ProcessedSnapshots(0, Nil))
        _ <- mkSnapshotService(mkNodeService(List(0, 1, 2, 3, 5, 6)), mkSnapshotDAO(), mkProcessedSnapshotsService(ref))
          .processSnapshot()
          .take(5)
          .compile
          .drain
      } yield ref
    }
      .flatMap(ref => ref.get)
      .map(res => expect.same(res, ProcessedSnapshots(4, Nil)))
  }

  test("process properly and save next ordinal to process") {
    {
      for {
        ref <- Ref[IO].of(ProcessedSnapshots(0, Nil))
        _ <- mkSnapshotService(mkNodeService(List(0, 1, 2, 3)), mkSnapshotDAO(), mkProcessedSnapshotsService(ref))
          .processSnapshot()
          .take(40)
          .compile
          .drain
      } yield ref
    }
      .flatMap(ref => ref.get)
      .map(res => expect.same(res, ProcessedSnapshots(4, Nil)))
  }

  test("process properly with gaps and save next ordinal to process") {
    {
      for {
        ref <- Ref[IO].of(ProcessedSnapshots(3, List(1)))
        _ <- mkSnapshotService(mkNodeService(List(1, 3, 4, 5)), mkSnapshotDAO(), mkProcessedSnapshotsService(ref))
          .processSnapshot()
          .take(40)
          .compile
          .drain
      } yield ref
    }
      .flatMap(ref => ref.get)
      .map(res => expect.same(res, ProcessedSnapshots(6, Nil)))
  }

  test("process properly and return gaps") {
    {
      for {
        ref <- Ref[IO].of(ProcessedSnapshots(0, Nil))
        _ <- mkSnapshotService(mkNodeService(List(2, 3)), mkSnapshotDAO(), mkProcessedSnapshotsService(ref))
          .processSnapshot()
          .take(3)
          .compile
          .drain
      } yield ref
    }
      .flatMap(ref => ref.get)
      .map(res => expect.same(res, ProcessedSnapshots(4, List(0, 1))))
  }

  test("should not fail when sending to opensearch fails") {
    {
      for {
        ref <- Ref[IO].of(ProcessedSnapshots(0, Nil))
        _ <- mkSnapshotService(
          mkNodeService(List(0, 1, 2, 3, 4, 5)),
          mkSnapshotDAO(Some(5)),
          mkProcessedSnapshotsService(ref)
        )
          .processSnapshot()
          .take(6)
          .compile
          .drain
      } yield ref
    }
      .flatMap(ref => ref.get)
      .map(res => expect.same(res, ProcessedSnapshots(5, Nil)))
  }

  test("should not fail when sending to opensearch fails, but stop processing further ordinals") {
    {
      for {
        ref <- Ref[IO].of(ProcessedSnapshots(0, Nil))
        _ <- mkSnapshotService(
          mkNodeService(List(0, 1, 2, 3, 4, 5)),
          mkSnapshotDAO(Some(3)),
          mkProcessedSnapshotsService(ref)
        ).processSnapshot().take(40).compile.drain
      } yield ref
    }
      .flatMap(ref => ref.get)
      .map(res => expect.same(res, ProcessedSnapshots(3, Nil)))
  }

  test(
    "should not fail when sending to opensearch fails and resume processing further ordinals when sending restored"
  ) {
    {
      for {
        ref <- Ref[IO].of(ProcessedSnapshots(0, Nil))
        _ <- mkSnapshotService(
          mkNodeService(List(0, 1, 2, 3, 4, 5)),
          mkSnapshotDAO(Some(3), true),
          mkProcessedSnapshotsService(ref)
        ).processSnapshot().take(8).compile.drain
      } yield ref
    }
      .flatMap(ref => ref.get)
      .map(res => expect.same(res, ProcessedSnapshots(6, Nil)))
  }

  test("should not fail when downloading from nodes fail, but stop processing further ordinals") {
    {
      for {
        ref <- Ref[IO].of(ProcessedSnapshots(0, Nil))
        _ <- mkSnapshotService(
          mkNodeService(List(0, 1, 2, 3, 4, 5), Some(3)),
          mkSnapshotDAO(),
          mkProcessedSnapshotsService(ref)
        ).processSnapshot().take(5).compile.drain
      } yield ref
    }
      .flatMap(ref => ref.get)
      .map(res => expect.same(res, ProcessedSnapshots(3, Nil)))
  }

}
