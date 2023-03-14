package org.constellation.snapshotstreaming

import cats.MonadThrow
import cats.data.{NonEmptyList, NonEmptySet}
import cats.syntax.functor._

import scala.collection.immutable.{SortedMap, SortedSet}

import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.ID.Id
import org.tessellation.schema._
import org.tessellation.schema.block.DAGBlock
import org.tessellation.schema.epoch.EpochProgress
import org.tessellation.schema.height.{Height, SubHeight}
import org.tessellation.schema.peer.PeerId
import org.tessellation.schema.transaction.RewardTransaction
import org.tessellation.security.Hashed
import org.tessellation.security.hash.{Hash, ProofsHash}
import org.tessellation.security.hex.Hex
import org.tessellation.security.signature.Signed
import org.tessellation.security.signature.signature.{Signature, SignatureProof}

import eu.timepit.refined.types.numeric.NonNegLong

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
          info = GlobalSnapshotInfoV1(SortedMap.empty, SortedMap.empty, SortedMap.empty),
          tips = SnapshotTips(SortedSet.empty, SortedSet.empty)
        ),
        NonEmptySet.one(SignatureProof(Id(Hex("")), Signature(Hex(""))))
      ),
      hash,
      ProofsHash(Hash.empty.value)
    )

  def incrementalGlobalSnapshot[F[_]: MonadThrow: KryoSerializer](
    ordinal: NonNegLong,
    height: NonNegLong,
    subHeight: NonNegLong,
    lastSnapshot: Hash,
    hash: Hash,
    globalSnapshotInfo: GlobalSnapshotInfo = GlobalSnapshotInfo.empty,
    blocks: SortedSet[BlockAsActiveTip[DAGBlock]] = SortedSet.empty,
    rewards: SortedSet[RewardTransaction] = SortedSet.empty
  ): F[Hashed[GlobalIncrementalSnapshot]] =
    globalSnapshotInfo.stateProof.map { sp =>
      Hashed(
        Signed(
          GlobalIncrementalSnapshot(
            ordinal = SnapshotOrdinal(ordinal),
            height = Height(height),
            subHeight = SubHeight(subHeight),
            lastSnapshotHash = lastSnapshot,
            blocks = blocks,
            stateChannelSnapshots = SortedMap.empty,
            rewards = rewards,
            epochProgress = EpochProgress.MinValue,
            nextFacilitators = NonEmptyList.of(PeerId(Hex(""))),
            tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
            stateProof = sp
          ),
          NonEmptySet.one(SignatureProof(Id(Hex("")), Signature(Hex(""))))
        ),
        hash,
        ProofsHash(Hash.empty.value)
      )
    }

}
