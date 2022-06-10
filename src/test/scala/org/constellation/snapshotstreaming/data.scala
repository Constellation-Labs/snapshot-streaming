package org.constellation.snapshotstreaming

import cats.data.{NonEmptyList, NonEmptySet}

import scala.collection.immutable.{SortedMap, SortedSet}

import org.tessellation.dag.snapshot.{GlobalSnapshot, SnapshotOrdinal}
import org.tessellation.schema.ID.Id
import org.tessellation.schema.height.{Height, SubHeight}
import org.tessellation.schema.peer.PeerId
import org.tessellation.security.hash.Hash
import org.tessellation.security.hex.Hex
import org.tessellation.security.signature.Signed
import org.tessellation.security.signature.signature.{Signature, SignatureProof}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong

object data {

  def globalSnapshot(ordinal: Long) = Signed(
    GlobalSnapshot(
      ordinal = SnapshotOrdinal(NonNegLong.unsafeFrom(ordinal)),
      height = Height(0),
      subHeight = SubHeight(1L),
      lastSnapshotHash = Hash(""),
      blocks = SortedSet.empty,
      stateChannelSnapshots = SortedMap.empty,
      rewards = SortedSet.empty,
      nextFacilitators = NonEmptyList.of(PeerId(Hex(""))),
      info = null,
      tips = null
    ),
    NonEmptySet.one(SignatureProof(Id(Hex("")), Signature(Hex(""))))
  )

}
