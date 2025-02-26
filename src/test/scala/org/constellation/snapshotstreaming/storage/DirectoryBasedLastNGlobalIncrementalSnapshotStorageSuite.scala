package org.constellation.snapshotstreaming.storage

import cats.effect.std.Random
import cats.effect.{IO, Resource}
import cats.syntax.option._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.height.Height
import io.constellationnetwork.schema.transaction.TransactionReference
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.{Hasher, HasherSelector}
import io.constellationnetwork.shared.sharedKryoRegistrar

import eu.timepit.refined.auto._
import fs2.io.file.{Files, Path}
import org.constellation.snapshotstreaming.data.{hashSelect, incrementalGlobalSnapshot}
import weaver.MutableIOSuite

object DirectoryBasedLastNGlobalIncrementalSnapshotStorageSuite extends MutableIOSuite {

  type Res = HasherSelector[IO]

  override def sharedResource: Resource[IO, Res] =
    KryoSerializer.forAsync[IO](sharedKryoRegistrar).flatMap { implicit ks =>
      JsonSerializer.forSync[IO].asResource.map { implicit jsonSerializer =>
        (HasherSelector.forSync[IO](Hasher.forJson[IO], Hasher.forKryo[IO], hashSelect))
      }
    }

  def directoryBasedStorage(implicit
    h: HasherSelector[IO]
  ): Resource[IO, DirectoryBasedLastNGlobalIncrementalSnapshotStorage[IO]] =
    Random.scalaUtilRandom.asResource.flatMap { rnd =>
      rnd.nextLong.asResource.map(l => Path(s"test_snapshots_$l")).flatMap { dirPath =>
        Resource.make(
          Files[IO]
            .createDirectories(dirPath)
            .flatMap(_ => DirectoryBasedLastNGlobalIncrementalSnapshotStorage.make(dirPath, 10))
        )(_ => Files[IO].deleteRecursively(dirPath).as(()))
      }
    }

  private val address = Address("DAG2AUdecqFwEGcgAcH1ac2wrsg8acrgGwrQojzw")

  private val snapshotInfo = GlobalSnapshotInfo(
    SortedMap(address -> Hash.empty),
    SortedMap(address -> TransactionReference.empty),
    SortedMap(address -> Balance(0L)),
    SortedMap.empty,
    SortedMap.empty,
    None,
    None,
    None,
    None
  )

  private def mkInitialSnapshot()(implicit h: HasherSelector[IO]) =
    incrementalGlobalSnapshot(100L, 10L, 20L, Hash("abc"), Hash("def"), snapshotInfo)

  test("get should return None before initial snapshot is set") { res =>
    implicit val h: HasherSelector[IO] = res

    directoryBasedStorage.use { storage =>
      storage.get
        .map(expect.same(None, _))
    }
  }

  test("getCombined should return None before initial snapshot is set") { res =>
    implicit val h: HasherSelector[IO] = res

    directoryBasedStorage.use { storage =>
      storage.get
        .map(expect.same(None, _))
    }
  }

  test("get should return last snapshot after it's set") { res =>
    implicit val h: HasherSelector[IO] = res

    directoryBasedStorage.use { storage =>
      mkInitialSnapshot().flatMap { initial =>
        storage.setInitial(initial, snapshotInfo) >>
          storage.get
            .map(expect.same(Some(initial), _))
      }
    }
  }

  test("getCombined should return last snapshot after it's set") { res =>
    implicit val h: HasherSelector[IO] = res

    directoryBasedStorage.use { storage =>
      mkInitialSnapshot().flatMap { initial =>
        storage.setInitial(initial, snapshotInfo) >>
          storage.getCombined
            .map(expect.same(Some((initial, snapshotInfo)), _))
      }
    }
  }

  test("getOrdinal should return None before initial snapshot is set") { res =>
    implicit val h: HasherSelector[IO] = res

    directoryBasedStorage.use { storage =>
      storage.getOrdinal
        .map(expect.same(None, _))
    }
  }

  test("getOrdinal should return last snapshot's ordinal after it's set") { res =>
    implicit val h: HasherSelector[IO] = res

    directoryBasedStorage.use { storage =>
      mkInitialSnapshot().flatMap { initial =>
        storage.setInitial(initial, snapshotInfo) >>
          storage.getOrdinal
            .map(expect.same(SnapshotOrdinal(100L), _))
      }
    }
  }

  test("getHeight should return None before initial snapshot is set") { res =>
    implicit val h: HasherSelector[IO] = res

    directoryBasedStorage.use { storage =>
      storage.getHeight
        .map(expect.same(None, _))
    }
  }

  test("getHeight should return last snapshot's height after it's set") { res =>
    implicit val h: HasherSelector[IO] = res

    directoryBasedStorage.use { storage =>
      mkInitialSnapshot.flatMap { initial =>
        storage.setInitial(initial, snapshotInfo) >>
          storage.getHeight
            .map(expect.same(Height(10L).some, _))
      }
    }
  }

  test("set should fail when we try to set snapshot before the initial snapshot is set") { res =>
    implicit val h: HasherSelector[IO] = res

    directoryBasedStorage.use { storage =>
      mkInitialSnapshot.flatMap { initial =>
        storage
          .set(initial, snapshotInfo)
          .map(_ => none[Throwable])
          .handleError(_.some)
          .map(maybeError => verify(maybeError.isDefined, maybeError.fold("none")(_.getMessage)))
      }
    }
  }

  test("set should fail when we try to set a snapshot that's not the next one") { res =>
    implicit val h: HasherSelector[IO] = res

    directoryBasedStorage.use { storage =>
      mkInitialSnapshot.flatMap { initial =>
        incrementalGlobalSnapshot(102L, 10L, 22L, Hash("ghi"), Hash("jkl"), snapshotInfo).flatMap { nextWrong =>
          storage.setInitial(initial, snapshotInfo) >>
            storage
              .set(nextWrong, snapshotInfo)
              .map(_ => none[Throwable])
              .handleError(_.some)
              .map(maybeError => verify(maybeError.isDefined, maybeError.fold("none")(_.getMessage)))
        }
      }
    }
  }

  test("set should fail when we try to set a snapshot with not matching state") { res =>
    implicit val h: HasherSelector[IO] = res

    directoryBasedStorage.use { storage =>
      mkInitialSnapshot.flatMap { initial =>
        incrementalGlobalSnapshot(101L, 10L, 21L, Hash("def"), Hash("ghi"), snapshotInfo).flatMap { nextCorrect =>
          storage.setInitial(initial, snapshotInfo) >>
            storage
              .set(nextCorrect, GlobalSnapshotInfo.empty)
              .map(_ => none[Throwable])
              .handleError(_.some)
              .map(maybeError => verify(maybeError.isDefined, maybeError.fold("none")(_.getMessage)))
        }
      }
    }
  }

  test("set should successfully set a snapshot if it is the next one") { res =>
    implicit val h: HasherSelector[IO] = res

    directoryBasedStorage.use { storage =>
      mkInitialSnapshot.flatMap { initial =>
        incrementalGlobalSnapshot(101L, 10L, 21L, Hash("def"), Hash("ghi"), snapshotInfo).flatMap { nextCorrect =>
          storage.setInitial(initial, snapshotInfo) >>
            storage
              .set(nextCorrect, snapshotInfo)
              .map(expect.same((), _))
        }
      }
    }
  }

  test("setInitial should fail if the initial snapshot already exists") { res =>
    implicit val h: HasherSelector[IO] = res

    directoryBasedStorage.use { storage =>
      mkInitialSnapshot.flatMap { initial =>
        storage.setInitial(initial, snapshotInfo) >>
          storage
            .setInitial(initial, snapshotInfo)
            .map(_ => none[Throwable])
            .handleError(_.some)
            .map(maybeError => verify(maybeError.isDefined, maybeError.fold("none")(_.getMessage)))
      }
    }
  }

  test("setInitial should fail when we try to set initial snapshot with not matching state") { res =>
    implicit val h: HasherSelector[IO] = res

    directoryBasedStorage.use { storage =>
      mkInitialSnapshot.flatMap { initial =>
        storage
          .setInitial(initial, GlobalSnapshotInfo.empty)
          .map(_ => none[Throwable])
          .handleError(_.some)
          .map(maybeError => verify(maybeError.isDefined, maybeError.fold("none")(_.getMessage)))
      }
    }
  }

  test("setInitial should successfully set initial snapshot if it not yet exists") { res =>
    implicit val h: HasherSelector[IO] = res

    directoryBasedStorage.use { storage =>
      mkInitialSnapshot.flatMap { initial =>
        storage
          .setInitial(initial, snapshotInfo)
          .map(expect.same((), _))
      }
    }
  }

  test("set should successfully list all snapshots") { res =>
    implicit val h: HasherSelector[IO] = res

    directoryBasedStorage.use { storage =>
      mkInitialSnapshot.flatMap { initial =>
        for {
          incrementalNextSnapshot <- incrementalGlobalSnapshot(101L, 10L, 21L, Hash("def"), Hash("ghi"), snapshotInfo)
          _ <- storage.setInitial(initial, snapshotInfo)
          _ <- storage.set(incrementalNextSnapshot, snapshotInfo)
          result <- storage.getAll
        } yield expect.same(List(101, 100).some, result.map(_.map(_.ordinal.value.value)))
      }
    }
  }

}
