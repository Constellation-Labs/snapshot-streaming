package org.constellation.snapshotstreaming

import cats.effect.Async
import cats.syntax.flatMap._
import cats.syntax.functor._

import org.tessellation.json.JsonBrotliBinarySerializer
import org.tessellation.kryo.KryoSerializer
import org.tessellation.node.shared.config.types.SnapshotSizeConfig
import org.tessellation.node.shared.infrastructure.block.processing.BlockAcceptanceManager
import org.tessellation.node.shared.infrastructure.snapshot._
import org.tessellation.node.shared.modules.SharedValidators
import org.tessellation.schema.balance.Amount
import org.tessellation.security.signature.SignedValidator
import org.tessellation.security.{HashSelect, Hasher, SecurityProvider}

import eu.timepit.refined.auto._

object TessellationServices {

  def make[F[_]: Async: KryoSerializer: SecurityProvider: Hasher](configuration: Configuration, hashSelect: HashSelect, snapshotSizeConfig: SnapshotSizeConfig): F[TessellationServices[F]] =
    for {
      _ <- Async[F].unit
      validators = SharedValidators.make[F](None, None, None, snapshotSizeConfig.maxStateChannelSnapshotBinarySizeInBytes)
      currencySnapshotAcceptanceManager = CurrencySnapshotAcceptanceManager.make(
        BlockAcceptanceManager.make[F](validators.currencyBlockValidator),
        Amount(0L),
        hashSelect
      )
      currencyEventsCutter = CurrencyEventsCutter.make[F]
      currencySnapshotCreator = CurrencySnapshotCreator.make[F](currencySnapshotAcceptanceManager, None, snapshotSizeConfig, currencyEventsCutter)
      currencySnapshotValidator = CurrencySnapshotValidator.make[F](currencySnapshotCreator, SignedValidator.make[F], None, None)
      currencySnapshotContextFns = CurrencySnapshotContextFunctions.make(currencySnapshotValidator, hashSelect)
      stateChannelManager <- GlobalSnapshotStateChannelAcceptanceManager.make(None)
      jsonBrotliBinarySerializer <- JsonBrotliBinarySerializer.forSync[F]
      globalSnapshotStateChannelEventsProcessor =
        GlobalSnapshotStateChannelEventsProcessor.make[F](validators.stateChannelValidator, stateChannelManager, currencySnapshotContextFns, jsonBrotliBinarySerializer)
      globalSnapshotAcceptanceManager = GlobalSnapshotAcceptanceManager.make(
        BlockAcceptanceManager.make[F](validators.blockValidator),
        globalSnapshotStateChannelEventsProcessor,
        configuration.collateral
      )
      globalSnapshotContextFns = GlobalSnapshotContextFunctions.make[F](globalSnapshotAcceptanceManager, hashSelect)
      globalSnapshotContextService = GlobalSnapshotContextService.make(globalSnapshotStateChannelEventsProcessor, globalSnapshotContextFns)
  } yield new TessellationServices[F](globalSnapshotContextService) {}

}

sealed abstract class TessellationServices[F[_]] private (
  val globalSnapshotContextService: GlobalSnapshotContextService[F]
)
