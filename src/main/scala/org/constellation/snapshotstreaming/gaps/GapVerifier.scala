package org.constellation.snapshotstreaming.gaps

import org.constellation.schema.snapshot.SnapshotInfo

class GapVerifier {

  def hasNonDummyTransactions(snapshotInfoBeforeGap: SnapshotInfo, snapshotInfoAfterGap: SnapshotInfo): Boolean = {
    val nonRewardBalancesBeforeGap = snapshotInfoBeforeGap.addressCacheData
      .mapValues(a => a.balance - a.rewardsBalance)
    val nonRewardBalancesAfterGap = snapshotInfoAfterGap.addressCacheData
      .mapValues(a => a.balance - a.rewardsBalance)

    val difference: Set[(String, Long)] = nonRewardBalancesBeforeGap.toSet.diff(nonRewardBalancesAfterGap.toSet)
    difference.nonEmpty
  }
}
