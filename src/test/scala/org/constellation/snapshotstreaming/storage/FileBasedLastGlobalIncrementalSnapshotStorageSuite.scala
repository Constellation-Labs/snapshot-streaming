package org.constellation.snapshotstreaming.storage

import cats.effect.std.Random
import cats.effect.IO
import cats.effect.Resource
import cats.syntax.option._

import scala.collection.immutable.SortedMap
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.height.Height
import io.constellationnetwork.schema.transaction.TransactionReference
import io.constellationnetwork.node.shared.domain.snapshot.storage.LastSnapshotStorage
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.shared.sharedKryoRegistrar
import eu.timepit.refined.auto._
import fs2.io.file.Files
import fs2.io.file.Path
import org.constellation.snapshotstreaming.data.hashSelect
import org.constellation.snapshotstreaming.data.incrementalGlobalSnapshot
import weaver.MutableIOSuite
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax.GlobalSnapshotInfoMptOps
import io.constellationnetwork.security.HasherSelector
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import org.constellation.snapshotstreaming.schema.kryoRegistrar

object FileBasedLastGlobalIncrementalSnapshotStorageSuite extends MutableIOSuite {

  type Res = (KryoSerializer[IO], HasherSelector[IO], JsonSerializer[IO])
  type StorageWithMptStore = (LastSnapshotStorage[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo], MptStore[IO, GlobalStateKey])

  override def sharedResource: Resource[IO, Res] =
    KryoSerializer.forAsync[IO](sharedKryoRegistrar ++ kryoRegistrar).flatMap { implicit ks =>
      JsonSerializer.forAsync[IO].asResource.map { implicit jsonSerializer =>
        (ks, HasherSelector.forSync[IO](Hasher.forJson[IO], Hasher.forKryo[IO], hashSelect), jsonSerializer)
      }
    }

  def fileBasedStorage(implicit
                       ks: KryoSerializer[IO],
                       h: HasherSelector[IO]
  ): Resource[IO, StorageWithMptStore] = {
    Random.scalaUtilRandom.asResource.flatMap { rnd =>
      Resource.eval(rnd.nextLong).map(l => Path(l.toString)).flatMap { path =>
        implicit val gsps: GlobalStateProofSelector =
          GlobalStateProofSelector(SnapshotOrdinal.MinValue)
        implicit val hasher: Hasher[IO] = HasherSelector[IO].getCurrent
        JsonSerializer.forAsync[IO].asResource.flatMap { implicit js =>
          Resource.eval(InMemoryMerklePatriciaProducer.make[IO]()).flatMap { mptProducer =>
            Resource.eval(MptStore.make[IO, GlobalStateKey](
              mptProducer,
              GlobalStateKey.toHex[IO]
            )).flatMap { mptStore =>
            Resource.make(
              FileBasedLastGlobalIncrementalSnapshotStorage.make(path, mptStore).map(storage => (storage, mptStore))
            )(_ => Files[IO].deleteIfExists(path).as(()))
            }
          }
        }
      }
    }
  }

  /** Initialize the MPT store with state entries */
  private def initMptStore(
    mptStore: MptStore[IO, GlobalStateKey],
    state: GlobalSnapshotInfo,
    ordinal: SnapshotOrdinal
  )(implicit h: HasherSelector[IO], js: JsonSerializer[IO]): IO[Unit] = {
    implicit val gsps: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal.MinValue)
    h.withCurrent { implicit hasher =>
      state.allStateEntries[IO]
    }.flatMap { kvPairs =>
      mptStore.syncFull(kvPairs, ordinal)
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
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None
  )

  private def mkInitialSnapshot()(implicit ks: KryoSerializer[IO], h: HasherSelector[IO], jsonSerializer: JsonSerializer[IO]) =
    incrementalGlobalSnapshot[IO](100L, 10L, 20L, Hash("abc"), Hash("def"), snapshotInfo)

  test("get should return None before initial snapshot is set") { res =>
    implicit val (ks, h, js) = res

    fileBasedStorage.use { case (storage, _) =>
      storage.get
        .map(expect.same(None, _))
    }
  }

  test("getCombined should return None before initial snapshot is set") { res =>
    implicit val (ks, h, js) = res

    fileBasedStorage.use { case (storage, _) =>
      storage.get
        .map(expect.same(None, _))
    }
  }

  test("get should return last snapshot after it's set") { res =>
    implicit val (ks, h, js) = res

    fileBasedStorage.use { case (storage, mptStore) =>
      mkInitialSnapshot().flatMap { initial =>
        initMptStore(mptStore, snapshotInfo, initial.ordinal) >>
          storage.setInitial(initial, snapshotInfo) >>
          storage.get
            .map(expect.same(Some(initial), _))
      }
    }
  }

  test("getCombined should return last snapshot after it's set") { res =>
    implicit val (ks, h, js) = res

    fileBasedStorage.use { case (storage, mptStore) =>
      mkInitialSnapshot().flatMap { initial =>
        initMptStore(mptStore, snapshotInfo, initial.ordinal) >>
          storage.setInitial(initial, snapshotInfo) >>
          storage.getCombined
            .map(expect.same(Some((initial, snapshotInfo)), _))
      }
    }
  }

  test("getOrdinal should return None before initial snapshot is set") { res =>
    implicit val (ks, h, js) = res

    fileBasedStorage.use { case (storage, _) =>
      storage.getOrdinal
        .map(expect.same(None, _))
    }
  }

  test("getOrdinal should return last snapshot's ordinal after it's set") { res =>
    implicit val (ks, h, js) = res

    fileBasedStorage.use { case (storage, mptStore) =>
      mkInitialSnapshot().flatMap { initial =>
        initMptStore(mptStore, snapshotInfo, initial.ordinal) >>
          storage.setInitial(initial, snapshotInfo) >>
          storage.getOrdinal
            .map(expect.same(SnapshotOrdinal(100L), _))
      }
    }
  }

  test("getHeight should return None before initial snapshot is set") { res =>
    implicit val (ks, h, js) = res

    fileBasedStorage.use { case (storage, _) =>
      storage.getHeight
        .map(expect.same(None, _))
    }
  }

  test("getHeight should return last snapshot's height after it's set") { res =>
    implicit val (ks, h, js) = res

    fileBasedStorage.use { case (storage, mptStore) =>
      mkInitialSnapshot.flatMap { initial =>
        initMptStore(mptStore, snapshotInfo, initial.ordinal) >>
          storage.setInitial(initial, snapshotInfo) >>
          storage.getHeight
            .map(expect.same(Height(10L).some, _))
      }
    }
  }

  test("set should call setInitial when no initial snapshot exists") { res =>
    implicit val (ks, h, js) = res

    fileBasedStorage.use { case (storage, mptStore) =>
      mkInitialSnapshot.flatMap { initial =>
        initMptStore(mptStore, snapshotInfo, initial.ordinal) >>
          storage
            .set(initial, snapshotInfo)
            .map(expect.same((), _))
      }
    }
  }

  test("set should succeed even when ordinal is not sequential (no validation)") { res =>
    implicit val (ks, h, js) = res

    fileBasedStorage.use { case (storage, mptStore) =>
      mkInitialSnapshot.flatMap { initial =>
        incrementalGlobalSnapshot[IO](102L, 10L, 22L, Hash("ghi"), Hash("jkl"), snapshotInfo).flatMap { nextWrong =>
          initMptStore(mptStore, snapshotInfo, initial.ordinal) >>
            storage.setInitial(initial, snapshotInfo) >>
            storage
              .set(nextWrong, snapshotInfo)
              .map(expect.same((), _))
        }
      }
    }
  }

  test("set should succeed even with different state (no state validation in set)") { res =>
    implicit val (ks, h, js) = res

    fileBasedStorage.use { case (storage, mptStore) =>
      mkInitialSnapshot.flatMap { initial =>
        incrementalGlobalSnapshot[IO](101L, 10L, 21L, Hash("def"), Hash("ghi"), snapshotInfo).flatMap { nextCorrect =>
          initMptStore(mptStore, snapshotInfo, initial.ordinal) >>
            storage.setInitial(initial, snapshotInfo) >>
            storage
              .set(nextCorrect, GlobalSnapshotInfo.empty)
              .map(expect.same((), _))
        }
      }
    }
  }

  test("set should successfully set a snapshot if it is the next one") { res =>
    implicit val (ks, h, js) = res

    fileBasedStorage.use { case (storage, mptStore) =>
      mkInitialSnapshot.flatMap { initial =>
        incrementalGlobalSnapshot[IO](101L, 10L, 21L, Hash("def"), Hash("ghi"), snapshotInfo).flatMap { nextCorrect =>
          initMptStore(mptStore, snapshotInfo, initial.ordinal) >>
            storage.setInitial(initial, snapshotInfo) >>
            storage
              .set(nextCorrect, snapshotInfo)
              .map(expect.same((), _))
        }
      }
    }
  }

  test("setInitial should fail if the initial snapshot already exists") { res =>
    implicit val (ks, h, js) = res

    fileBasedStorage.use { case (storage, mptStore) =>
      mkInitialSnapshot.flatMap { initial =>
        initMptStore(mptStore, snapshotInfo, initial.ordinal) >>
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
    implicit val (ks, h, js) = res

    fileBasedStorage.use { case (storage, mptStore) =>
      mkInitialSnapshot.flatMap { initial =>
        initMptStore(mptStore, snapshotInfo, initial.ordinal) >>
          storage
            .setInitial(initial, GlobalSnapshotInfo.empty)
            .map(_ => none[Throwable])
            .handleError(_.some)
            .map(maybeError => verify(maybeError.isDefined, maybeError.fold("none")(_.getMessage)))
      }
    }
  }

  test("setInitial should successfully set initial snapshot if it not yet exists") { res =>
    implicit val (ks, h, js) = res

    fileBasedStorage.use { case (storage, mptStore) =>
      mkInitialSnapshot.flatMap { initial =>
        initMptStore(mptStore, snapshotInfo, initial.ordinal) >>
          storage
            .setInitial(initial, snapshotInfo)
            .map(expect.same((), _))
      }
    }
  }

}
