package org.constellation.snapshotstreaming

import cats.effect.Async

import org.tessellation.currency.schema.currency.{CurrencyBlock, CurrencyTransaction}
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.balance.Amount
import org.tessellation.schema.block.DAGBlock
import org.tessellation.schema.transaction.DAGTransaction
import org.tessellation.sdk.infrastructure.block.processing.BlockAcceptanceManager
import org.tessellation.sdk.infrastructure.snapshot._
import org.tessellation.sdk.modules.SdkValidators
import org.tessellation.security.SecurityProvider

import eu.timepit.refined.auto._

object TessellationServices {

  def make[F[_]: Async: KryoSerializer: SecurityProvider](configuration: Configuration): TessellationServices[F] = {

    val validators = SdkValidators.make[F](None)
    val currencySnapshotAcceptanceManager = CurrencySnapshotAcceptanceManager.make(
      BlockAcceptanceManager.make[F, CurrencyTransaction, CurrencyBlock](validators.currencyBlockValidator),
      Amount(0L)
    )
    val currencySnapshotContextFns = CurrencySnapshotContextFunctions.make(currencySnapshotAcceptanceManager)
    val globalSnapshotAcceptanceManager = GlobalSnapshotAcceptanceManager.make(
      BlockAcceptanceManager.make[F, DAGTransaction, DAGBlock](validators.blockValidator),
      GlobalSnapshotStateChannelEventsProcessor.make[F](validators.stateChannelValidator, currencySnapshotContextFns),
      configuration.collateral
    )
    val globalSnapshotContextFns = GlobalSnapshotContextFunctions.make[F](globalSnapshotAcceptanceManager)
    new TessellationServices[F](globalSnapshotContextFns) {}
  }

}

sealed abstract class TessellationServices[F[_]] private (
  val globalSnapshotContextFns: GlobalSnapshotContextFunctions[F]
)
