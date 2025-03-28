package org.constellation.snapshotstreaming

import cats.effect.Async
import cats.syntax.flatMap._
import cats.syntax.functor._
import io.constellationnetwork.json.{JsonBrotliBinarySerializer, JsonSerializer}
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.config.types.{AddressesConfig, DelegatedStakingConfig, LastGlobalSnapshotsSyncConfig, SharedConfigReader}
import io.constellationnetwork.node.shared.domain.node.UpdateNodeParametersAcceptanceManager
import io.constellationnetwork.node.shared.domain.statechannel.FeeCalculator
import io.constellationnetwork.node.shared.domain.swap.SpendActionValidator
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
import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.node.shared.domain.delegatedStake.UpdateDelegatedStakeAcceptanceManager
import io.constellationnetwork.node.shared.domain.nodeCollateral.UpdateNodeCollateralAcceptanceManager
import io.constellationnetwork.schema.SnapshotOrdinal

object TessellationServices {

  def make[F[_] : Async : JsonSerializer : KryoSerializer : SecurityProvider](
                                                                               env: AppEnvironment,
                                                                               configuration: SharedConfigReader
  )(implicit hasherSelector: HasherSelector[F]): F[TessellationServices[F]] =
    for {
      _ <- Async[F].unit
      nodeConfig = Configuration.nodeSharedConfig(env, configuration)
      tokenLocksAddedToGl0Ordinal = configuration.fieldsAddedOrdinals.globalTokenLocks.getOrElse(env, SnapshotOrdinal.MinValue)
      txHasher = Hasher.forKryo
      validators = hasherSelector.withCurrent { implicit hasher =>
        SharedValidators.make[F](
          AddressesConfig(Set.empty),
          None,
          None,
          None,
          nodeConfig.feeConfigs,
          nodeConfig.snapshotSize.maxStateChannelSnapshotBinarySizeInBytes,
          txHasher,
          DelegatedStakingConfig(RewardFraction.MinValue, RewardFraction.MinValue, configuration.delegatedStaking.withdrawalTimeLimit)
        )
      }

      stateChannelManager <- GlobalSnapshotStateChannelAcceptanceManager.make(None)
      jsonBrotliBinarySerializer <- JsonBrotliBinarySerializer.forSync[F]
      feeCalculator = FeeCalculator.make(nodeConfig.feeConfigs)

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
                nodeConfig.snapshotSize,
                currencyEventsCutter,
                validationErrorStorage
              )
            val currencySnapshotValidator = CurrencySnapshotValidator
              .make[F](tokenLocksAddedToGl0Ordinal, currencySnapshotCreator, SignedValidator.make[F], None, None)
            CurrencySnapshotContextFunctions.make(currencySnapshotValidator)
          }
        }
      }

      globalSnapshotContextService = hasherSelector.withCurrent { implicit hasher => {
        val globalSnapshotStateChannelEventsProcessor =
          GlobalSnapshotStateChannelEventsProcessor.make[F](
            validators.stateChannelValidator,
            stateChannelManager,
            currencySnapshotContextFns,
            jsonBrotliBinarySerializer,
            feeCalculator
          )
        val globalSnapshotAcceptanceManager: GlobalSnapshotAcceptanceManager[F] = GlobalSnapshotAcceptanceManager.make(
          tokenLocksAddedToGl0Ordinal,
          nodeConfig.fieldsAddedOrdinals.globalTokenLocks(env),
          nodeConfig.fieldsAddedOrdinals.nodeCollateral(env),
          BlockAcceptanceManager.make[F](validators.blockValidator, txHasher),
          AllowSpendBlockAcceptanceManager.make[F](validators.allowSpendBlockValidator),
          TokenLockBlockAcceptanceManager.make[F](validators.tokenLockBlockValidator),
          globalSnapshotStateChannelEventsProcessor,
          UpdateNodeParametersAcceptanceManager.make[F](validators.updateNodeParametersValidator),
          UpdateDelegatedStakeAcceptanceManager.make[F](validators.updateDelegatedStakeValidator),
          UpdateNodeCollateralAcceptanceManager.make[F](validators.updateNodeCollateralValidator),
          SpendActionValidator.make[F],
          configuration.collateral.get.amount,
          configuration.delegatedStaking.withdrawalTimeLimit
        )
        val globalSnapshotContextFns = GlobalSnapshotContextFunctions.make[F](globalSnapshotAcceptanceManager)
        GlobalSnapshotContextService.make(globalSnapshotStateChannelEventsProcessor, globalSnapshotContextFns)
      }
      }
    } yield new TessellationServices[F](globalSnapshotContextService) {}

}

sealed abstract class TessellationServices[F[_]] private(
  val globalSnapshotContextService: GlobalSnapshotContextService[F]
)
