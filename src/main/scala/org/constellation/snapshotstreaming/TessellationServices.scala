package org.constellation.snapshotstreaming

import cats.effect.Async
import cats.syntax.flatMap._
import cats.syntax.functor._
import io.constellationnetwork.json.JsonBrotliBinarySerializer
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.infrastructure.block.processing.BlockAcceptanceManager
import io.constellationnetwork.node.shared.infrastructure.snapshot._
import io.constellationnetwork.node.shared.modules.SharedValidators
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.security.signature.SignedValidator
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.HasherSelector
import io.constellationnetwork.security.SecurityProvider
import eu.timepit.refined.auto._
import io.constellationnetwork.node.shared.config.types.AddressesConfig
import io.constellationnetwork.node.shared.domain.block.processing.BlockAcceptanceManager
import io.constellationnetwork.node.shared.domain.statechannel.FeeCalculator
import io.constellationnetwork.node.shared.domain.swap.block.AllowSpendBlockAcceptanceManager
import io.constellationnetwork.node.shared.domain.tokenlock.block.TokenLockBlockAcceptanceManager
import io.constellationnetwork.node.shared.domain.transaction.FeeTransactionValidator

object TessellationServices {

  def make[F[_]: Async: JsonSerializer: KryoSerializer: SecurityProvider: Hasher](
    configuration: Configuration,
  )(implicit hasherSelector: HasherSelector[F]): F[TessellationServices[F]] =
    for {
      _ <- Async[F].unit
      txHasher = Hasher.forKryo
      validators = SharedValidators.make[F](
        configuration.addresses,
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
            TokenLockBlockAcceptanceManager.make[F](validators.tokenLockBlockValidator),
            AllowSpendBlockAcceptanceManager.make[F](validators.allowSpendBlockValidator),
            Amount(0L),
            validators.currencyMessageValidator,
            FeeTransactionValidator.make[F](validators.signedValidator),
            GlobalSnapshotSyncValidator.make[F](validators.signedValidator, None)
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
