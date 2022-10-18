package org.constellation.snapshotstreaming

import cats.data.{NonEmptyList, NonEmptySet}

import scala.collection.immutable.{SortedMap, SortedSet}

import org.tessellation.dag.snapshot.epoch.EpochProgress
import org.tessellation.dag.snapshot.{GlobalSnapshot, GlobalSnapshotInfo, GlobalSnapshotTips, SnapshotOrdinal}
import org.tessellation.schema.ID.Id
import org.tessellation.schema.height.{Height, SubHeight}
import org.tessellation.schema.peer.PeerId
import org.tessellation.security.hash.{Hash, ProofsHash}
import org.tessellation.security.hex.Hex
import org.tessellation.security.signature.Signed
import org.tessellation.security.signature.signature.{Signature, SignatureProof}

import eu.timepit.refined.types.numeric.NonNegLong
import org.tessellation.security.Hashed

object data {

  def globalSnapshot(
    ordinal: NonNegLong,
    height: NonNegLong,
    subHeight: NonNegLong,
    lastSnapshot: Hash,
    hash: Hash
  ): Hashed[GlobalSnapshot] =
    Hashed(
      Signed(
        GlobalSnapshot(
          ordinal = SnapshotOrdinal(ordinal),
          height = Height(height),
          subHeight = SubHeight(subHeight),
          lastSnapshotHash = lastSnapshot,
          blocks = SortedSet.empty,
          stateChannelSnapshots = SortedMap.empty,
          rewards = SortedSet.empty,
          epochProgress = EpochProgress.MinValue,
          nextFacilitators = NonEmptyList.of(PeerId(Hex(""))),
          info = GlobalSnapshotInfo(SortedMap.empty, SortedMap.empty, SortedMap.empty),
          tips = GlobalSnapshotTips(SortedSet.empty, SortedSet.empty)
        ),
        NonEmptySet.one(SignatureProof(Id(Hex("")), Signature(Hex(""))))
      ),
      hash,
      ProofsHash(Hash.empty.value)
    )

}
