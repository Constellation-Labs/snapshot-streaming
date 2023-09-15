package org.constellation.snapshotstreaming

import cats.effect.Async
import cats.syntax.flatMap._
import cats.syntax.functor._
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.balance.Amount
import org.tessellation.sdk.infrastructure.block.processing.BlockAcceptanceManager
import org.tessellation.sdk.infrastructure.snapshot._
import org.tessellation.sdk.modules.SdkValidators
import org.tessellation.security.SecurityProvider
import eu.timepit.refined.auto._
import org.tessellation.security.signature.SignedValidator

object TessellationServices {

  def make[F[_]: Async: KryoSerializer: SecurityProvider](configuration: Configuration): F[TessellationServices[F]] =
    for {
      _ <- Async[F].unit
      validators = SdkValidators.make[F](None, None, None)
      currencySnapshotAcceptanceManager = CurrencySnapshotAcceptanceManager.make(
        BlockAcceptanceManager.make[F](validators.currencyBlockValidator),
        Amount(0L)
      )
      currencySnapshotCreator = CurrencySnapshotCreator.make[F](currencySnapshotAcceptanceManager, None)
      currencySnapshotValidator = CurrencySnapshotValidator.make[F](currencySnapshotCreator, SignedValidator.make[F], None, None)
      currencySnapshotContextFns = CurrencySnapshotContextFunctions.make(currencySnapshotValidator)
      stateChannelManager <- GlobalSnapshotStateChannelAcceptanceManager.make(None)
      globalSnapshotStateChannelEventsProcessor =
        GlobalSnapshotStateChannelEventsProcessor.make[F](validators.stateChannelValidator, stateChannelManager, currencySnapshotContextFns)
      globalSnapshotAcceptanceManager = GlobalSnapshotAcceptanceManager.make(
        BlockAcceptanceManager.make[F](validators.blockValidator),
        globalSnapshotStateChannelEventsProcessor,
        configuration.collateral
      )
      globalSnapshotContextFns = GlobalSnapshotContextFunctions.make[F](globalSnapshotAcceptanceManager)
      globalSnapshotContextService = GlobalSnapshotContextService.make(globalSnapshotStateChannelEventsProcessor, globalSnapshotContextFns)
  } yield new TessellationServices[F](globalSnapshotContextService) {}

}

sealed abstract class TessellationServices[F[_]] private (
  val globalSnapshotContextService: GlobalSnapshotContextService[F]
)
