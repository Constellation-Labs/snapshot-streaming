package org.constellation.snapshotstreaming.node

import cats.effect.testkit.TestControl
import cats.effect.{IO, Outcome, Ref}

import scala.concurrent.duration._

import org.tessellation.dag.snapshot.GlobalSnapshot
import org.tessellation.security.signature.Signed

import org.constellation.snapshotstreaming.Configuration
import org.constellation.snapshotstreaming.data._
import org.constellation.snapshotstreaming.node.{NodeClient, SnapshotOrdinal}
import org.http4s.Uri
import org.http4s.syntax.literals._
import weaver.SimpleIOSuite

object NodeDownloadSuite extends SimpleIOSuite {

  def mkNodeClient(
    snapshotOrdinalsState: Ref[IO, List[IO[SnapshotOrdinal]]] = Ref.unsafe[IO, List[IO[SnapshotOrdinal]]](Nil),
    isSnapshotFail: Boolean = false
  ) =
    new NodeClient[IO] {
      override def uri: Uri = uri"http://1.1.1.1:80"

      override def getLatestOrdinal: IO[SnapshotOrdinal] = for {
        snapshotsOrdinals <- snapshotOrdinalsState.get
        _ <- snapshotOrdinalsState.set(snapshotsOrdinals.tail)
        snapshotOrdinal <- snapshotsOrdinals.head
      } yield snapshotOrdinal

      override def getSnapshot(ordinal: SnapshotOrdinal): IO[Signed[GlobalSnapshot]] =
        if (isSnapshotFail) IO.raiseError(new Exception("Error")) else IO.delay(globalSnapshot(ordinal.value))

    }

  def mkNodeDownload(nodeClient: NodeClient[IO], conf: Configuration = new Configuration) =
    NodeDownload.make[IO](nodeClient, conf)

  test("return snapshot ordinals") {
    val nodeInterval = 3
    val conf = new Configuration {
      override val nodeIntervalInSeconds = nodeInterval
    }
    val program = for {
      ref <- Ref[IO].of(List(IO(SnapshotOrdinal(1)), IO(SnapshotOrdinal(2))))
      res <- mkNodeDownload(mkNodeClient(ref), conf)
        .downloadLatestOrdinal()
        .take(2)
        .compile
        .toList
    } yield res

    TestControl.execute(program).flatMap { control =>
      for {
        _ <- control.tick
        _ <- control.advance(nodeInterval.seconds)
        _ <- control.tick
        _ <- control.advance(nodeInterval.seconds)
        _ <- control.tick
        results <- control.results
      } yield expect.same(results, Some(Outcome.succeeded(List(SnapshotOrdinal(1), SnapshotOrdinal(2)))))
    }
  }

  test("return snapshot ordinals when long execution, but shorter than interval") {
    val nodeInterval = 5
    val executionDuration = 3
    val conf = new Configuration {
      override val nodeIntervalInSeconds = nodeInterval
    }
    val program = for {
      ref <- Ref[IO].of(
        List(IO(SnapshotOrdinal(1)), IO(SnapshotOrdinal(2)))
      )
      res <- mkNodeDownload(mkNodeClient(ref), conf)
        .downloadLatestOrdinal()
        .evalTap(_ => IO.sleep(executionDuration.seconds))
        .take(2)
        .compile
        .toList
    } yield res

    TestControl.execute(program).flatMap { control =>
      for {
        _ <- control.tick
        _ <- control.advance(nodeInterval.seconds)
        _ <- control.tick
        _ <- control.advance(executionDuration.seconds)
        _ <- control.tick
        _ <- control.advance((nodeInterval - executionDuration).seconds)
        _ <- control.tick
        intermediateResults <- control.results
        _ <- expect.same(intermediateResults, None).failFast
        _ <- control.advance(executionDuration.seconds)
        _ <- control.tick
        results <- control.results
      } yield expect.same(results, Some(Outcome.succeeded(List(SnapshotOrdinal(1), SnapshotOrdinal(2)))))
    }
  }

  test("return snapshot ordinals when long execution after, longer than interval") {
    val nodeInterval = 5
    val executionDuration = 7
    val conf = new Configuration {
      override val nodeIntervalInSeconds = nodeInterval
    }
    val program = for {
      ref <- Ref[IO].of(
        List(IO(SnapshotOrdinal(1)), IO(SnapshotOrdinal(2)))
      )
      res <- mkNodeDownload(mkNodeClient(ref), conf)
        .downloadLatestOrdinal()
        .evalTap(_ => IO.sleep(executionDuration.seconds))
        .take(2)
        .compile
        .toList
    } yield res

    TestControl.execute(program).flatMap { control =>
      for {
        _ <- control.tick
        _ <- control.advance(nodeInterval.seconds)
        _ <- control.tick
        _ <- control.advance(executionDuration.seconds)
        _ <- control.tick
        _ <- control.advance(executionDuration.seconds)
        _ <- control.tick
        results <- control.results
      } yield expect.same(results, Some(Outcome.succeeded(List(SnapshotOrdinal(1), SnapshotOrdinal(2)))))
    }
  }

  test("return snapshot ordinal after failed attempt") {
    val nodeInterval = 3
    val conf = new Configuration {
      override val nodeIntervalInSeconds = nodeInterval
    }
    val program = for {
      ref <- Ref[IO].of(List(IO.raiseError(new Throwable("ERROR")), IO(SnapshotOrdinal(2))))
      res <- mkNodeDownload(mkNodeClient(ref), conf)
        .downloadLatestOrdinal()
        .take(1)
        .compile
        .toList
    } yield res

    TestControl.execute(program).flatMap { control =>
      for {
        _ <- control.tick
        _ <- control.advance(nodeInterval.seconds)
        _ <- control.tick
        _ <- control.advance(nodeInterval.seconds)
        _ <- control.tick
        results <- control.results
      } yield expect.same(results, Some(Outcome.succeeded(List(SnapshotOrdinal(2)))))
    }
  }

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
    val actual = mkNodeDownload(mkNodeClient(isSnapshotFail = true)).downloadSnapshot(SnapshotOrdinal(1))
    actual
      .take(1)
      .compile
      .toList
      .map(s => expect.same(s, expected))
  }

}
