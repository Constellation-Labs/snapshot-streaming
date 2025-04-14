package org.constellation.snapshotstreaming

import cats.Parallel
import cats.effect.Async
import cats.syntax.flatMap._
import cats.syntax.functor._
import org.tessellation.json.JsonBrotliBinarySerializer
import org.tessellation.json.JsonSerializer
import org.tessellation.kryo.KryoSerializer
import org.tessellation.node.shared.infrastructure.block.processing.BlockAcceptanceManager
import org.tessellation.node.shared.infrastructure.snapshot._
import org.tessellation.node.shared.modules.SharedValidators
import org.tessellation.schema.balance.Amount
import org.tessellation.security.signature.SignedValidator
import org.tessellation.security.Hasher
import org.tessellation.security.HasherSelector
import org.tessellation.security.SecurityProvider
import eu.timepit.refined.auto._
import org.tessellation.node.shared.domain.statechannel.FeeCalculator

object TessellationServices {

  def make[F[_]: Async: Parallel: JsonSerializer: KryoSerializer: SecurityProvider](
    configuration: Configuration,
  )(implicit hasherSelector: HasherSelector[F]): F[TessellationServices[F]] =
    for {
      _ <- Async[F].unit
      txHasher = Hasher.forKryo
      validators = SharedValidators.make[F](
        None,
        None,
        None,
        configuration.feeConfigs,
        configuration.snapshotSize.maxStateChannelSnapshotBinarySizeInBytes,
        txHasher
      )

      stateChannelManager <- GlobalSnapshotStateChannelAcceptanceManager.make(None)
      jsonBrotliBinarySerializer <- JsonBrotliBinarySerializer.forSync[F]
      feeCalculator = FeeCalculator.make(configuration.feeConfigs)

      currencySnapshotContextFns = {
        val currencySnapshotAcceptanceManager: CurrencySnapshotAcceptanceManager[F] =
          CurrencySnapshotAcceptanceManager.make(
            BlockAcceptanceManager.make[F](validators.currencyBlockValidator, txHasher),
            Amount(0L),
            validators.currencyMessageValidator
          )
        val currencyEventsCutter = CurrencyEventsCutter.make[F](None)
        val currencySnapshotCreator = CurrencySnapshotCreator
          .make[F](currencySnapshotAcceptanceManager, None, configuration.snapshotSize, currencyEventsCutter)
        val currencySnapshotValidator = CurrencySnapshotValidator
          .make[F](currencySnapshotCreator, SignedValidator.make[F], None, None)
        CurrencySnapshotContextFunctions.make(currencySnapshotValidator)
      }

      globalSnapshotContextService = {
        val globalSnapshotStateChannelEventsProcessor =
          GlobalSnapshotStateChannelEventsProcessor.make[F](
            validators.stateChannelValidator,
            stateChannelManager,
            currencySnapshotContextFns,
            jsonBrotliBinarySerializer,
            feeCalculator
          )
        val globalSnapshotAcceptanceManager: GlobalSnapshotAcceptanceManager[F] = GlobalSnapshotAcceptanceManager.make(
          BlockAcceptanceManager.make[F](validators.blockValidator, txHasher),
          globalSnapshotStateChannelEventsProcessor,
          configuration.collateral
        )
        val globalSnapshotContextFns = GlobalSnapshotContextFunctions.make[F](globalSnapshotAcceptanceManager)
        GlobalSnapshotContextService.make(globalSnapshotStateChannelEventsProcessor, globalSnapshotContextFns)
      }
    } yield new TessellationServices[F](globalSnapshotContextService) {}

}

sealed abstract class TessellationServices[F[_]] private (
  val globalSnapshotContextService: GlobalSnapshotContextService[F]
)
