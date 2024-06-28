package org.constellation.snapshotstreaming.storage

import cats.effect.Async
import cats.effect.std.Mutex
import cats.syntax.applicative._
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.functor._
import org.tessellation.kryo.KryoSerializer
import org.tessellation.merkletree.StateProofValidator
import org.tessellation.node.shared.domain.snapshot.storage.LastSnapshotStorage
import org.tessellation.schema._
import org.tessellation.security._
import fs2.io.file._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec

class FileBasedLastIncrementalGlobalSnapshotStorage[F[_]: Async: HasherSelector](mutex: Mutex[F], path: Path)(
  implicit codec: Codec[SnapshotWithState[GlobalIncrementalSnapshot, GlobalSnapshotInfo]]
) extends BaseLastIncrementalSnapshotStorage[
      F,
      GlobalSnapshotStateProof,
      GlobalIncrementalSnapshot,
      GlobalSnapshotInfo
    ](mutex, path) {

  protected def validateStateProof(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo): F[Unit] =
    HasherSelector[F].forOrdinal(snapshot.ordinal) { implicit hasher =>
      (hasher.getLogic(snapshot.ordinal) match {
        case JsonHash => StateProofValidator.validate(snapshot, state)
        case KryoHash => StateProofValidator.validate(snapshot, GlobalSnapshotInfoV2.fromGlobalSnapshotInfo(state))
      }).flatMap(Async[F].fromValidated)
    }

}

object FileBasedLastIncrementalGlobalSnapshotStorage {

  def make[F[_]: Async: Files: KryoSerializer: HasherSelector](
    path: Path
  ): F[LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo]] =
    Mutex[F].map { mutex =>
      implicit val codec: Codec[Hashed[GlobalIncrementalSnapshot]] = deriveCodec[Hashed[GlobalIncrementalSnapshot]]
      implicit val snapshotWithInfoCodec: Codec[SnapshotWithState[GlobalIncrementalSnapshot, GlobalSnapshotInfo]] =
        deriveCodec[SnapshotWithState[GlobalIncrementalSnapshot, GlobalSnapshotInfo]]
      new FileBasedLastIncrementalGlobalSnapshotStorage[F](mutex, path)
    }

}
