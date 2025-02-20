package org.constellation.snapshotstreaming

import cats.effect.Async
import cats.syntax.flatMap._
import cats.syntax.functor._

import io.constellationnetwork.json.{JsonBrotliBinarySerializer, JsonSerializer}
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.config.types.{
  AddressesConfig,
  DelegatedStakingConfig,
  LastGlobalSnapshotsSyncConfig
}
import io.constellationnetwork.node.shared.domain.node.UpdateNodeParametersAcceptanceManager
import io.constellationnetwork.node.shared.domain.statechannel.FeeCalculator
import io.constellationnetwork.node.shared.domain.swap.block.AllowSpendBlockAcceptanceManager
import io.constellationnetwork.node.shared.domain.tokenlock.block.TokenLockBlockAcceptanceManager
import io.constellationnetwork.node.shared.infrastructure.block.processing.BlockAcceptanceManager
import io.constellationnetwork.node.shared.infrastructure.consensus.CurrencySnapshotEventValidationErrorStorage
import io.constellationnetwork.node.shared.infrastructure.snapshot._
import io.constellationnetwork.node.shared.modules.SharedValidators
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.node.RewardFraction
import io.constellationnetwork.security.signature.SignedValidator
import io.constellationnetwork.security.{Hasher, HasherSelector, SecurityProvider}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosInt}

object TessellationServices {

  def make[F[_]: Async: JsonSerializer: KryoSerializer: SecurityProvider](
    configuration: Configuration
  )(implicit hasherSelector: HasherSelector[F]): F[TessellationServices[F]] =
    for {
      _ <- Async[F].unit
      txHasher = Hasher.forKryo
      validators = hasherSelector.withCurrent { implicit hasher =>
        SharedValidators.make[F](
          AddressesConfig(Set.empty),
          None,
          None,
          None,
          configuration.feeConfigs,
          configuration.snapshotSize.maxStateChannelSnapshotBinarySizeInBytes,
          txHasher,
          DelegatedStakingConfig(RewardFraction.MinValue, RewardFraction.MinValue)
        )
      }

      stateChannelManager <- GlobalSnapshotStateChannelAcceptanceManager.make(None)
      jsonBrotliBinarySerializer <- JsonBrotliBinarySerializer.forSync[F]
      feeCalculator = FeeCalculator.make(configuration.feeConfigs)

      currencySnapshotContextFns <- {
        val currencySnapshotAcceptanceManager: CurrencySnapshotAcceptanceManager[F] =
          CurrencySnapshotAcceptanceManager.make(
            LastGlobalSnapshotsSyncConfig(NonNegLong(2L), PosInt(10)),
            BlockAcceptanceManager.make[F](validators.currencyBlockValidator, txHasher),
            TokenLockBlockAcceptanceManager.make(validators.tokenLockBlockValidator),
            AllowSpendBlockAcceptanceManager.make(validators.allowSpendBlockValidator),
            Amount(0L),
            validators.currencyMessageValidator,
            validators.feeTransactionValidator,
            validators.globalSnapshotSyncValidator
          )
        val currencyEventsCutter = CurrencyEventsCutter.make[F](None)
        hasherSelector.withCurrent { implicit hasher =>
          CurrencySnapshotEventValidationErrorStorage.make(PosInt(10)).map { validationErrorStorage =>
            val currencySnapshotCreator = CurrencySnapshotCreator
              .make[F](
                currencySnapshotAcceptanceManager,
                None,
                configuration.snapshotSize,
                currencyEventsCutter,
                validationErrorStorage
              )
            val currencySnapshotValidator = CurrencySnapshotValidator
              .make[F](currencySnapshotCreator, SignedValidator.make[F], None, None)
            CurrencySnapshotContextFunctions.make(currencySnapshotValidator)
          }
        }
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
          AllowSpendBlockAcceptanceManager.make[F](validators.allowSpendBlockValidator),
          globalSnapshotStateChannelEventsProcessor,
          UpdateNodeParametersAcceptanceManager.make[F](validators.updateNodeParametersValidator),
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
