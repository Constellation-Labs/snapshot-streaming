package org.constellation.snapshotstreaming.storage

import cats.effect.Async
import cats.effect.std.Mutex
import cats.syntax.applicative._
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.functor._
import org.tessellation.security._
import fs2.io.file._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.tessellation.currency.schema.currency.CurrencyIncrementalSnapshot
import org.tessellation.currency.schema.currency.CurrencySnapshotInfo
import org.tessellation.currency.schema.currency.CurrencySnapshotStateProof
import org.tessellation.merkletree.StateProofValidator

class FileBasedLastIncrementalCurrencySnapshotStorage[F[_]: Async: Hasher](mutex: Mutex[F], path: Path)(implicit
  codec: Codec[SnapshotWithState[CurrencyIncrementalSnapshot, CurrencySnapshotInfo]]
) extends BaseLastIncrementalSnapshotStorage[F, CurrencySnapshotStateProof, CurrencyIncrementalSnapshot, CurrencySnapshotInfo](mutex, path) {

  def validateStateProof(snapshot: Hashed[CurrencyIncrementalSnapshot], state: CurrencySnapshotInfo): F[Unit] =
    StateProofValidator
      .validate(snapshot, state)
      .flatMap(Async[F].fromValidated)

}

object FileBasedLastIncrementalCurrencySnapshotStorage {

  def make[F[_]: Async: Hasher](mutex: Mutex[F], path: Path): FileBasedLastIncrementalCurrencySnapshotStorage[F] = {
    implicit val codec: Codec[Hashed[CurrencyIncrementalSnapshot]] = deriveCodec[Hashed[CurrencyIncrementalSnapshot]]
    implicit val snapshotWithInfoCodec: Codec[SnapshotWithState[CurrencyIncrementalSnapshot, CurrencySnapshotInfo]] =
      deriveCodec[SnapshotWithState[CurrencyIncrementalSnapshot, CurrencySnapshotInfo]]
    new FileBasedLastIncrementalCurrencySnapshotStorage[F](mutex, path)
  }


  def make[F[_]: Async: Hasher](path: Path): F[FileBasedLastIncrementalCurrencySnapshotStorage[F]] =
    Mutex[F].map(make(_, path))

}
