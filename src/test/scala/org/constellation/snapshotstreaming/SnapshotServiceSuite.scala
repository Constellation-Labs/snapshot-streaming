package org.constellation.snapshotstreaming

import java.util.concurrent.atomic.AtomicLong

import cats.effect.{IO, Ref}
import cats.syntax.option._

import org.tessellation.dag.snapshot.GlobalSnapshot
import org.tessellation.security.signature.Signed

import eu.timepit.refined.auto._
import fs2.Stream
import org.constellation.snapshotstreaming.data._
import org.constellation.snapshotstreaming.node.NodeService
import org.constellation.snapshotstreaming.opensearch.SnapshotDAO
import weaver.SimpleIOSuite
import org.constellation.snapshotstreaming.s3.S3DAO

object SnapshotServiceSuite extends SimpleIOSuite {

  def mkProcessedSnapshotsService(ref: Ref[IO, ProcessedSnapshots]) = new ProcessedSnapshotsService[IO] {

    override def initialState(): Stream[IO, ProcessedSnapshots] = Stream.eval(ref.get).head

    override def saveState(processedSnapshots: ProcessedSnapshots): Stream[IO, Unit] =
      Stream.eval(ref.set(processedSnapshots))

  }

  def mkBrokenProcessedSnapshotsService() = new ProcessedSnapshotsService[IO] {

    override def initialState(): Stream[IO, ProcessedSnapshots] = Stream.raiseError(new Throwable("Init error"))

    override def saveState(processedSnapshots: ProcessedSnapshots): Stream[IO, Unit] =
      Stream.raiseError(new Throwable("Save error"))

  }

  def mkNodeService(ordinals: List[Long], ordinalToFail: Option[Long] = None) = new NodeService[IO] {

    override def getSnapshots(
      startingOrdinal: Option[Long],
      gapsOrdinals: Seq[Long]
    ): fs2.Stream[IO, Signed[GlobalSnapshot]] =
      Stream
        .emits(
          ordinals
            .filter(ord => startingOrdinal.exists(_ <= ord) || gapsOrdinals.contains(ord))
            .map(globalSnapshot(_))
        )
        .flatMap(snap =>
          if (ordinalToFail.contains(snap.ordinal.value.value)) Stream.raiseError(new Throwable("Node error"))
          else Stream.emit(snap)
        )

  }

  def mkSnapshotDAO(ordinalToFail: Option[Long] = None, failOnlyOnce: Boolean = false) = new SnapshotDAO[IO] {
    val counter = new AtomicLong(0)

    override def sendSnapshotToOpensearch(snapshot: Signed[GlobalSnapshot]): fs2.Stream[IO, Long] =
      if (
        ordinalToFail.contains(
          snapshot.ordinal.value.value
        ) && (!failOnlyOnce || (failOnlyOnce && counter.getAndIncrement < 1))
      ) {
        Stream.raiseError(new Exception("DAO error"))
      } else Stream.emit(snapshot.ordinal.value.value)

  }

  def mkS3DAO(ordinalToFail: Option[Long] = None) = new S3DAO[IO] {

    override def uploadSnapshot(snapshot: Signed[GlobalSnapshot]): Stream[IO,Unit] = 
      if(ordinalToFail.contains(
          snapshot.ordinal.value.value
        )) Stream.raiseError(new Exception("S3DAO error"))
      else Stream.emit(())


  }

  def mkSnapshotService(
    nodeService: NodeService[IO],
    dao: SnapshotDAO[IO],
    processedService: ProcessedSnapshotsService[IO],
    s3DAO: S3DAO[IO] = mkS3DAO()
  ) = {
    SnapshotService.make[IO, Signed[GlobalSnapshot]](nodeService, processedService, dao, s3DAO)
  }

  test("save next ordinal to process when succeed") {
    {
      for {
        ref <- Ref[IO].of(ProcessedSnapshots(0L.some, Nil))
        _ <- mkSnapshotService(mkNodeService(List(0, 1, 2, 3)), mkSnapshotDAO(), mkProcessedSnapshotsService(ref))
          .processSnapshot()
          .take(5)
          .compile
          .drain
      } yield ref
    }
      .flatMap(ref => ref.get)
      .map(res => expect.same(res, ProcessedSnapshots(4L.some, Nil)))
  }

  test("save next ordinal to process and filter out processed gaps") {
    {
      for {
        ref <- Ref[IO].of(ProcessedSnapshots(3L.some, List(1)))
        _ <- mkSnapshotService(mkNodeService(List(1, 3, 4, 5)), mkSnapshotDAO(), mkProcessedSnapshotsService(ref))
          .processSnapshot()
          .take(5)
          .compile
          .drain
      } yield ref
    }
      .flatMap(ref => ref.get)
      .map(res => expect.same(res, ProcessedSnapshots(6L.some, Nil)))
  }

  test("filter out only processed gaps when no starting ordinal") {
    {
      for {
        ref <- Ref[IO].of(ProcessedSnapshots(None, List(1, 2, 5)))
        _ <- mkSnapshotService(mkNodeService(List(0, 1, 2, 3)), mkSnapshotDAO(), mkProcessedSnapshotsService(ref))
          .processSnapshot()
          .take(5)
          .compile
          .drain
      } yield ref
    }
      .flatMap(ref => ref.get)
      .map(res => expect.same(res, ProcessedSnapshots(None, List(5))))
  }

  test("save next ordinal and add gaps for missing snapshots") {
    {
      for {
        ref <- Ref[IO].of(ProcessedSnapshots(0L.some, Nil))
        _ <- mkSnapshotService(mkNodeService(List(2, 3)), mkSnapshotDAO(), mkProcessedSnapshotsService(ref))
          .processSnapshot()
          .take(3)
          .compile
          .drain
      } yield ref
    }
      .flatMap(ref => ref.get)
      .map(res => expect.same(res, ProcessedSnapshots(4L.some, List(0, 1))))
  }

  test("should keep trying when sending to opensearch fails") {
    {
      for {
        ref <- Ref[IO].of(ProcessedSnapshots(0L.some, Nil))
        _ <- mkSnapshotService(
          mkNodeService(List(0, 1, 2, 3, 4, 5, 6)),
          mkSnapshotDAO(Some(5)),
          mkProcessedSnapshotsService(ref)
        )
          .processSnapshot()
          .take(7)
          .compile
          .drain
      } yield ref
    }
      .flatMap(ref => ref.get)
      .map(res => expect.same(res, ProcessedSnapshots(5L.some, Nil)))
  }

  test("should keep trying when uploading to s3 fails") {
    {
      for {
        ref <- Ref[IO].of(ProcessedSnapshots(0L.some, Nil))
        _ <- mkSnapshotService(
          mkNodeService(List(0, 1, 2, 3, 4, 5, 6)),
          mkSnapshotDAO(),
          mkProcessedSnapshotsService(ref),
          mkS3DAO(ordinalToFail = Some(4))
        )
          .processSnapshot()
          .take(7)
          .compile
          .drain
      } yield ref
    }
      .flatMap(ref => ref.get)
      .map(res => expect.same(res, ProcessedSnapshots(4L.some, Nil)))
  }

  test(
    "should keep trying when sending to opensearch fails and resume processing further ordinals when sending restored"
  ) {
    {
      for {
        ref <- Ref[IO].of(ProcessedSnapshots(0L.some, Nil))
        _ <- mkSnapshotService(
          mkNodeService(List(0, 1, 2, 3, 4, 5)),
          mkSnapshotDAO(Some(3), true),
          mkProcessedSnapshotsService(ref)
        ).processSnapshot().take(8).compile.drain
      } yield ref
    }
      .flatMap(ref => ref.get)
      .map(res => expect.same(res, ProcessedSnapshots(6L.some, Nil)))
  }

  test("should keep trying when downloading from nodes fail") {
    {
      for {
        ref <- Ref[IO].of(ProcessedSnapshots(0L.some, Nil))
        _ <- mkSnapshotService(
          mkNodeService(List(0, 1, 2, 3, 4, 5), Some(3)),
          mkSnapshotDAO(),
          mkProcessedSnapshotsService(ref)
        ).processSnapshot().take(6).compile.drain
      } yield ref
    }
      .flatMap(ref => ref.get)
      .map(res => expect.same(res, ProcessedSnapshots(3L.some, Nil)))
  }

  test("fail when cannot read initial state") {
    mkSnapshotService(mkNodeService(List(0), Some(3)), mkSnapshotDAO(), mkBrokenProcessedSnapshotsService())
      .processSnapshot()
      .take(6)
      .compile
      .toList
      .attempt
      .map(_.left.map(_.getMessage))
      .map(s => expect.same(s, Left("Init error")))

  }

}
