package org.constellation.snapshotstreaming

import cats.effect.std.Random
import cats.effect.{IO, Resource}
import cats.syntax.option._
import eu.timepit.refined.auto._
import fs2.io.file.{Files, Path}
import org.constellation.snapshotstreaming.data.globalSnapshot
import org.tessellation.schema.SnapshotOrdinal
import org.tessellation.ext.cats.effect.ResourceIO
import org.tessellation.schema.height.Height
import org.tessellation.sdk.domain.snapshot.storage.LastGlobalSnapshotStorage
import org.tessellation.security.hash.Hash
import weaver.SimpleIOSuite

object FileBasedLastGlobalSnapshotStorageSuite extends SimpleIOSuite {

  def fileBasedStorage: Resource[IO, LastGlobalSnapshotStorage[IO]] =
    Random.scalaUtilRandom.asResource.flatMap { rnd =>
      rnd.nextLong.asResource.map(l => Path(l.toString)).flatMap { path =>
        Resource.make(
          FileBasedLastGlobalSnapshotStorage.make(path)
        )(_ => Files[IO].deleteIfExists(path).as(()))
      }
    }

  val initial = globalSnapshot(100L, 10L, 20L, Hash("abc"), Hash("def"))

  test("get should return None before initial snapshot is set") {
    fileBasedStorage.use { storage =>
      storage.get
        .map(expect.same(None, _))
    }
  }

  test("get should return last snapshot after it's set") {
    fileBasedStorage.use { storage =>
      storage.setInitial(initial) >>
        storage.get
          .map(expect.same(Some(initial), _))
    }
  }

  test("getOrdinal should return None before initial snapshot is set") {
    fileBasedStorage.use { storage =>
      storage.getOrdinal
        .map(expect.same(None, _))
    }
  }

  test("getOrdinal should return last snapshot's ordinal after it's set") {
    fileBasedStorage.use { storage =>
      storage.setInitial(initial) >>
        storage.getOrdinal
          .map(expect.same(SnapshotOrdinal(100L).some, _))
    }
  }

  test("getHeight should return None before initial snapshot is set") {
    fileBasedStorage.use { storage =>
      storage.getHeight
        .map(expect.same(None, _))
    }
  }

  test("getHeight should return last snapshot's height after it's set") {
    fileBasedStorage.use { storage =>
      storage.setInitial(initial) >>
        storage.getHeight
          .map(expect.same(Height(10L).some, _))
    }
  }

  test("set should fail when we try to set snapshot before the initial snapshot is set") {
    fileBasedStorage.use { storage =>
      storage.set(initial)
        .map(_ => none[Throwable])
        .handleError(_.some)
        .map(maybeError => verify(maybeError.isDefined, maybeError.fold("none")(_.getMessage)))
    }
  }

  test("set should fail when we try to set a snapshot that's not the next one") {
    fileBasedStorage.use { storage =>
      val nextWrong = globalSnapshot(102L, 10L, 22L, Hash("ghi"), Hash("jkl"))

      storage.setInitial(initial) >>
        storage.set(nextWrong)
          .map(_ => none[Throwable])
          .handleError(_.some)
          .map(maybeError => verify(maybeError.isDefined, maybeError.fold("none")(_.getMessage)))
    }
  }

  test("set should successfully set a snapshot if it is the next one") {
    fileBasedStorage.use { storage =>
      val nextCorrect = globalSnapshot(101L, 10L, 21L, Hash("def"), Hash("ghi"))

      storage.setInitial(initial) >>
        storage.set(nextCorrect)
          .map(expect.same((), _))
    }
  }

  test("setInitial should fail if the initial snapshot already exists") {
    fileBasedStorage.use { storage =>
      storage.setInitial(initial) >>
        storage.setInitial(initial)
          .map(_ => none[Throwable])
          .handleError(_.some)
          .map(maybeError => verify(maybeError.isDefined, maybeError.fold("none")(_.getMessage)))
    }
  }

  test("setInitial should successfully set initial snapshot if it not yet exists") {
    fileBasedStorage.use { storage =>
      storage.setInitial(initial)
        .map(expect.same((), _))
    }
  }
}
