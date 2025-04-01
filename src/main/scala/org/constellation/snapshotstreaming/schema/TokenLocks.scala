package org.constellation.snapshotstreaming.schema

import java.util.UUID

object TokenLocks {
  case class TokenLock(
                        snapshotHash: String,
                        hash: String,
                        source: String,
                        amount: Long,
                        unlockEpoch: Option[Long],
                        ordinal: Long,
                        roundId: UUID,
                      )

  case class TokenUnlock(
                          snapshotHash: String,
                          hash: String,
                          lockReference: String,
                          amount: Long,
                          address: String,
                        )

}
