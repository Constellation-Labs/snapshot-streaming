package org.constellation.snapshotstreaming

import cats.effect.Async
import cats.syntax.flatMap._
import cats.syntax.functor._

import org.tessellation.json.JsonBrotliBinarySerializer
import org.tessellation.json.JsonSerializer
import org.tessellation.kryo.KryoSerializer
import org.tessellation.node.shared.config.types.SnapshotSizeConfig
import org.tessellation.node.shared.infrastructure.block.processing.BlockAcceptanceManager
import org.tessellation.node.shared.infrastructure.snapshot._
import org.tessellation.node.shared.modules.SharedValidators
import org.tessellation.schema.balance.Amount
import org.tessellation.security.signature.SignedValidator
import org.tessellation.security.{HashSelect, Hasher, SecurityProvider, HasherSelector}

import eu.timepit.refined.auto._

object TessellationServices {

  def make[F[_]: Async: JsonSerializer: KryoSerializer: SecurityProvider](configuration: Configuration, snapshotSizeConfig: SnapshotSizeConfig, hasherSelector: HasherSelector[F]): F[TessellationServices[F]] =
    for {
      _ <- Async[F].unit
      txHasher = Hasher.forKryo
      validators = SharedValidators.make[F](None, None, None, snapshotSizeConfig.maxStateChannelSnapshotBinarySizeInBytes, txHasher)
      currencySnapshotAcceptanceManager = CurrencySnapshotAcceptanceManager.make(
        BlockAcceptanceManager.make[F](validators.currencyBlockValidator, txHasher),
        Amount(0L)
      )
      currencyEventsCutter = CurrencyEventsCutter.make[F](None)
      alwaysCurrentHasherSelector = HasherSelector.forSyncAlwaysCurrent(Hasher.forJson)
      currencySnapshotCreator = {
        implicit val hs = alwaysCurrentHasherSelector
        CurrencySnapshotCreator.make[F](currencySnapshotAcceptanceManager, None, snapshotSizeConfig, currencyEventsCutter)
      }
      currencySnapshotValidator = {
        implicit val hs = alwaysCurrentHasherSelector
        CurrencySnapshotValidator.make[F](currencySnapshotCreator, SignedValidator.make[F], None, None)
      }
      currencySnapshotContextFns = {
        implicit val hs = alwaysCurrentHasherSelector
        CurrencySnapshotContextFunctions.make(currencySnapshotValidator)
      }
      stateChannelManager <- GlobalSnapshotStateChannelAcceptanceManager.make(None)
      jsonBrotliBinarySerializer <- JsonBrotliBinarySerializer.forSync[F]
      globalSnapshotStateChannelEventsProcessor =
        GlobalSnapshotStateChannelEventsProcessor.make[F](validators.stateChannelValidator, stateChannelManager, currencySnapshotContextFns, jsonBrotliBinarySerializer)
      globalSnapshotAcceptanceManager = {
        implicit val hs = hasherSelector
        GlobalSnapshotAcceptanceManager.make(
        BlockAcceptanceManager.make[F](validators.blockValidator, txHasher),
        globalSnapshotStateChannelEventsProcessor,
        configuration.collateral
      )
      }
      globalSnapshotContextFns = {
        implicit val hs = hasherSelector
        GlobalSnapshotContextFunctions.make[F](globalSnapshotAcceptanceManager)
      }
      globalSnapshotContextService = {
        implicit val hs = hasherSelector
        GlobalSnapshotContextService.make(globalSnapshotStateChannelEventsProcessor, globalSnapshotContextFns)
      }
  } yield new TessellationServices[F](globalSnapshotContextService) {}

}

sealed abstract class TessellationServices[F[_]] private (
  val globalSnapshotContextService: GlobalSnapshotContextService[F]
)
