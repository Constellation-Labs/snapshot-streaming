package org.constellation.snapshotstreaming

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo}
import io.constellationnetwork.security.Hashed

package object storage {

  case class SnapshotWithState(snapshot: Hashed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)

  object SnapshotWithState {
    implicit val codec: Codec[Hashed[GlobalIncrementalSnapshot]] = deriveCodec[Hashed[GlobalIncrementalSnapshot]]
    implicit val snapshotWithInfoCodec: Codec[SnapshotWithState] = deriveCodec[SnapshotWithState]
  }

}
