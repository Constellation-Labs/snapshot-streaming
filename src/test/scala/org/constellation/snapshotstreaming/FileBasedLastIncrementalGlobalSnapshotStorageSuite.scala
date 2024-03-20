package org.constellation.snapshotstreaming

import cats.effect.std.Random
import cats.effect.{IO, Resource}
import cats.syntax.option._

import scala.collection.immutable.SortedMap

import org.tessellation.ext.cats.effect.ResourceIO
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema._
import org.tessellation.schema.address.Address
import org.tessellation.schema.balance.Balance
import org.tessellation.schema.height.Height
import org.tessellation.schema.transaction.TransactionReference
import org.tessellation.node.shared.domain.snapshot.storage.LastSnapshotStorage
import org.tessellation.security.hash.Hash
import org.tessellation.shared.sharedKryoRegistrar

import eu.timepit.refined.auto._
import fs2.io.file.{Files, Path}
import org.constellation.snapshotstreaming.data.incrementalGlobalSnapshot
import weaver.MutableIOSuite
import org.tessellation.security.HashSelect
import org.tessellation.security.HashLogic
import org.tessellation.security.JsonHash
import org.tessellation.security.Hasher
import org.tessellation.json.JsonSerializer

object FileBasedLastIncrementalGlobalSnapshotStorageSuite extends MutableIOSuite {

  type Res = (KryoSerializer[IO], Hasher[IO])

  override def sharedResource: Resource[IO, Res] =
    KryoSerializer.forAsync[IO](sharedKryoRegistrar).flatMap { implicit ks =>
      JsonSerializer.forSync[IO].asResource.map { implicit jsonSerializer =>
        (ks, Hasher.forSync[IO](data.hashSelect))
      }
    }

  def fileBasedStorage(implicit
    ks: KryoSerializer[IO],
    h: Hasher[IO]
  ): Resource[IO, LastSnapshotStorage[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo]] =
    Random.scalaUtilRandom.asResource.flatMap { rnd =>
      rnd.nextLong.asResource.map(l => Path(l.toString)).flatMap { path =>
        Resource.make(
          FileBasedLastIncrementalGlobalSnapshotStorage.make(path, data.hashSelect)
        )(_ => Files[IO].deleteIfExists(path).as(()))
      }
    }

  private val address = Address("DAG2AUdecqFwEGcgAcH1ac2wrsg8acrgGwrQojzw")

  private val snapshotInfo = GlobalSnapshotInfo(
    SortedMap(address -> Hash.empty),
    SortedMap(address -> TransactionReference.empty),
    SortedMap(address -> Balance(0L)),
    SortedMap.empty,
    SortedMap.empty
  )

  private def mkInitialSnapshot()(implicit ks: KryoSerializer[IO], h: Hasher[IO]) =
    incrementalGlobalSnapshot(100L, 10L, 20L, Hash("abc"), Hash("def"), snapshotInfo)

  test("get should return None before initial snapshot is set") { res =>
    implicit val (ks, h) = res

    fileBasedStorage.use { storage =>
      storage.get
        .map(expect.same(None, _))
    }
  }

  test("getCombined should return None before initial snapshot is set") { res =>
    implicit val (ks, h) = res

    fileBasedStorage.use { storage =>
      storage.get
        .map(expect.same(None, _))
    }
  }

  test("get should return last snapshot after it's set") { res =>
    implicit val (ks, h) = res

    fileBasedStorage.use { storage =>
      mkInitialSnapshot().flatMap { initial =>
        storage.setInitial(initial, snapshotInfo) >>
          storage.get
            .map(expect.same(Some(initial), _))
      }
    }
  }

  test("getCombined should return last snapshot after it's set") { res =>
    implicit val (ks, h) = res

    fileBasedStorage.use { storage =>
      mkInitialSnapshot().flatMap { initial =>
        storage.setInitial(initial, snapshotInfo) >>
          storage.getCombined
            .map(expect.same(Some((initial, snapshotInfo)), _))
      }
    }
  }

  test("getOrdinal should return None before initial snapshot is set") { res =>
    implicit val (ks, h) = res

    fileBasedStorage.use { storage =>
      storage.getOrdinal
        .map(expect.same(None, _))
    }
  }

  test("getOrdinal should return last snapshot's ordinal after it's set") { res =>
    implicit val (ks, h) = res

    fileBasedStorage.use { storage =>
      mkInitialSnapshot().flatMap { initial =>
        storage.setInitial(initial, snapshotInfo) >>
          storage.getOrdinal
            .map(expect.same(SnapshotOrdinal(100L), _))
      }
    }
  }

  test("getHeight should return None before initial snapshot is set") { res =>
    implicit val (ks, h) = res

    fileBasedStorage.use { storage =>
      storage.getHeight
        .map(expect.same(None, _))
    }
  }

  test("getHeight should return last snapshot's height after it's set") { res =>
    implicit val (ks, h) = res

    fileBasedStorage.use { storage =>
      mkInitialSnapshot.flatMap { initial =>
        storage.setInitial(initial, snapshotInfo) >>
          storage.getHeight
            .map(expect.same(Height(10L).some, _))
      }
    }
  }

  test("set should fail when we try to set snapshot before the initial snapshot is set") { res =>
    implicit val (ks, h) = res

    fileBasedStorage.use { storage =>
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
    implicit val (ks, h) = res

    fileBasedStorage.use { storage =>
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
    implicit val (ks, h) = res

    fileBasedStorage.use { storage =>
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
    implicit val (ks, h) = res

    fileBasedStorage.use { storage =>
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
    implicit val (ks, h) = res

    fileBasedStorage.use { storage =>
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
    implicit val (ks, h) = res

    fileBasedStorage.use { storage =>
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
    implicit val (ks, h) = res

    fileBasedStorage.use { storage =>
      mkInitialSnapshot.flatMap { initial =>
        storage
          .setInitial(initial, snapshotInfo)
          .map(expect.same((), _))
      }
    }
  }

}
