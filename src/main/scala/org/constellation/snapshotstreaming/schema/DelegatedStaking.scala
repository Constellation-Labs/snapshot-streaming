package org.constellation.snapshotstreaming.schema

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
  transferFrom: Option[String],
)

case class DelegatedStakingWithdraw(
  snapshotHash: String,
  hash: String,
  sourceAddress: String,
  stakeCreateHash: String,
  rewards: Long,
  createdAtEpoch: Long,
  unlockEpoch: Long,
  completed: Boolean,
)

case class DelegatedStakingReward(snapshotHash: String, stakeHash: String, address: String, nodeId: String, amount: Long)
