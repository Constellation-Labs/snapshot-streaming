package org.constellation.snapshotstreaming.schema

import io.estatico.newtype.macros.newtype

case class DelegatedStakingCreate(
  snapshotHash: String,
  hash: String,
  createdAtOrdinal: Long,
  sourceAddress: String,
  nodeId: String,
  amount: Long,
  fee: Long = 0L,
  rewards: Long,
  tokenLockHash: String,
  parentHash: String,
  isUpdate: Boolean
)

case class DelegatedStakingWithdraw(
  snapshotHash: String,
  hash: String,
  sourceAddress: String,
  stakeCreateHash: String,
  rewards: Long,
  createdAtEpoch: Long
)

sealed trait StakingEventRef
case class StakingEventCreate(hash: String) extends StakingEventRef
case class StakingEventWithdraw(hash: String) extends StakingEventRef

case class DelegatedStakingBalanceChanges(
  snapshotHash: String,
  snapshotOrdinal: Long,
  address: String,
  nodeId: String,
  balance: Long,
  rewards: Long,
  stakingCreateEvent: StakingEventRef
)

case class DelegatedStakingReward(snapshotHash: String, address: String, nodeId: String, amount: Long)
