package org.constellation.snapshotstreaming.mapper

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import org.constellation.domain.snapshot.SnapshotInfo

case class AddressBalance(balance: Long, rewardsBalance: Long)

object AddressBalance {
  implicit val addressBalanceEncoder: Encoder[AddressBalance] = deriveEncoder
}

class SnapshotInfoMapper {
  def mapAddressBalances(
    snapshotInfo: SnapshotInfo
  ): Map[String, AddressBalance] =
    snapshotInfo.addressCacheData.mapValues(
      data =>
        AddressBalance(
          balance = data.balance,
          rewardsBalance = data.rewardsBalance
      )
    )
}
