package org.constellation.snapshotstreaming.storage

import cats.effect.Async
import cats.syntax.all._
import cats.Applicative
import cats.effect.std.Mutex
import fs2.io.file.Files
import fs2.io.file.Path
import org.tessellation.currency.schema.currency.CurrencyIncrementalSnapshot
import org.tessellation.currency.schema.currency.CurrencySnapshot
import org.tessellation.currency.schema.currency.CurrencySnapshotInfo
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.address.Address
import org.tessellation.schema.balance.Balance
import org.tessellation.security.signature.Signed
import org.tessellation.security.Hashed
import org.tessellation.security.Hasher
import org.tessellation.statechannel.StateChannelSnapshotBinary

import scala.collection.immutable.SortedMap

trait LastCurrencySnapshotStorage[F[_]] {

  def set(
    identifier: Address,
    snapshot: Either[Hashed[
      CurrencySnapshot
    ], (Hashed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo, Signed[StateChannelSnapshotBinary])],
    hasher: Hasher[F]
  ): F[Unit]

  def getLastBalances(identifier: Address, hasher: Hasher[F]): F[Option[SortedMap[Address, Balance]]]

}

object LastCurrencySnapshotStorage {

  def make[F[_]: Async: KryoSerializer](basePath: Path): F[LastCurrencySnapshotStorage[F]] =
    Mutex[F].map(make(_, basePath))

  def make[F[_]: Async: KryoSerializer](
    mutex: Mutex[F],
    basePath: Path
  ): LastCurrencySnapshotStorage[F] = new LastCurrencySnapshotStorage[F] {

    private def metagraphPath(identifier: Address) = basePath / identifier.toString

    private def lastSnapshotPath(identifier: Address) = metagraphPath(identifier) / "lastSnapshot.json"

    private def lastIncrementalSnapshotPath(identifier: Address) =
      metagraphPath(identifier) / "lastIncrementalSnapshot.json"

    private def createDirectoryIfDoesntExist(identifier: Address): F[Unit] = Files[F]
      .exists(metagraphPath(identifier))
      .ifM(
        Applicative[F].unit,
        Files[F].createDirectories(metagraphPath(identifier))
      )

    def set(
      identifier: Address,
      snapshot: Either[Hashed[
        CurrencySnapshot
      ], (Hashed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo, Signed[StateChannelSnapshotBinary])],
      hasher: Hasher[F]
    ): F[Unit] =
      createDirectoryIfDoesntExist(identifier).flatMap { _ =>
        snapshot match {
          case Left(full) =>
            FileBasedLastFullSnapshotStorage.make[F, CurrencySnapshot](lastSnapshotPath(identifier)).set(full)
          case Right((incremental, info, _)) =>
            implicit val hs: Hasher[F] = hasher
            val storage =
              FileBasedLastIncrementalCurrencySnapshotStorage.make(mutex, lastIncrementalSnapshotPath(identifier))
            storage.get.flatMap {
              case Some(_) => storage.set(incremental, info)
              case None    => storage.setInitial(incremental, info)
            }
        }
      }

    def getLastBalances(identifier: Address, hasher: Hasher[F]): F[Option[SortedMap[Address, Balance]]] = {
      implicit val hs: Hasher[F] = hasher
      FileBasedLastIncrementalCurrencySnapshotStorage
        .make(lastIncrementalSnapshotPath(identifier))
        .flatMap(_.getCombined)
        .flatMap {
          case Some((_, info)) => info.balances.some.pure[F]
          case None =>
            FileBasedLastFullSnapshotStorage
              .make[F, CurrencySnapshot](lastSnapshotPath(identifier))
              .get
              .map(_.map(_.info.balances))
        }
    }

  }

}
