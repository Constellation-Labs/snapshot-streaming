package org.constellation.snapshotstreaming

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._
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
import io.constellationnetwork.security.signature.SignedValidator
import io.constellationnetwork.security.{Hasher, HasherSelector, SecurityProvider}
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosInt}
import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.node.shared.domain.delegatedStake.UpdateDelegatedStakeAcceptanceManager
import io.constellationnetwork.node.shared.domain.nodeCollateral.UpdateNodeCollateralAcceptanceManager
import io.constellationnetwork.node.shared.domain.snapshot.services.GlobalL0Service
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage.LastNGlobalSnapshotStorage
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.epoch.EpochProgress

object TessellationServices {

  def make[F[_]: Async: Parallel: JsonSerializer: KryoSerializer: SecurityProvider](
    configuration: Configuration,
    l0Service: GlobalL0Service[F]
  )(implicit hasherSelector: HasherSelector[F]): F[TessellationServices[F]] =
    for {
      _ <- Async[F].unit
      tessellation3Migration = configuration.tessellation3Migration
      txHasher = Hasher.forKryo
      validators = hasherSelector.withCurrent { implicit hasher =>
        SharedValidators.make[F](
          AddressesConfig(Set.empty),
        None,
        None,
        None,
        configuration.feeConfigs,
          configuration.sharedConfigReader.snapshot.size.maxStateChannelSnapshotBinarySizeInBytes,
          txHasher,
          DelegatedStakingConfig(
            configuration.sharedConfigReader.delegatedStaking.minRewardFraction,
            configuration.sharedConfigReader.delegatedStaking.maxRewardFraction,
            configuration.sharedConfigReader.delegatedStaking.maxMetadataFieldsChars,
            configuration.sharedConfigReader.delegatedStaking.maxTokenLocksPerAddress,
            configuration.sharedConfigReader.delegatedStaking.minTokenLockAmount,
            configuration.sharedConfigReader.delegatedStaking.withdrawalTimeLimit)
      )
      }

      stateChannelManager <- GlobalSnapshotStateChannelAcceptanceManager.make(None)
      jsonBrotliBinarySerializer <- JsonBrotliBinarySerializer.forSync[F]
      feeCalculator = FeeCalculator.make(configuration.feeConfigs)
      currencySnapshotAcceptanceManager <- CurrencySnapshotAcceptanceManager.make(
          configuration.sharedConfigReader.fieldsAddedOrdinals,
          configuration.environment,
          configuration.sharedConfigReader.lastGlobalSnapshotsSync,
          BlockAcceptanceManager.make[F](validators.currencyBlockValidator, txHasher),
          TokenLockBlockAcceptanceManager.make(validators.tokenLockBlockValidator),
          AllowSpendBlockAcceptanceManager.make(validators.allowSpendBlockValidator),
          Amount(0L),
          validators.currencyMessageValidator,
          validators.feeTransactionValidator,
          validators.globalSnapshotSyncValidator
        )
      currencySnapshotContextFns <- {
        val currencyEventsCutter = CurrencyEventsCutter.make[F](None)
        hasherSelector.withCurrent { implicit hasher =>
          CurrencySnapshotEventValidationErrorStorage.make(PosInt(10)).map { validationErrorStorage =>
        val currencySnapshotCreator = CurrencySnapshotCreator
              .make[F](
                tessellation3Migration,
                currencySnapshotAcceptanceManager,
                None,
                configuration.snapshotSize,
                currencyEventsCutter,
                validationErrorStorage
              )
        val currencySnapshotValidator = CurrencySnapshotValidator
              .make[F](tessellation3Migration, currencySnapshotCreator, SignedValidator.make[F], None, None)
        CurrencySnapshotContextFunctions.make(currencySnapshotValidator)
      }
        }
      }

      updateNodeParametersAcceptanceManager = UpdateNodeParametersAcceptanceManager.make[F](validators.updateNodeParametersValidator)
      updateDelegatedStakeAcceptanceManager = UpdateDelegatedStakeAcceptanceManager.make[F](validators.updateDelegatedStakeValidator)
      updateNodeCollateralAcceptanceManager = UpdateNodeCollateralAcceptanceManager.make[F](validators.updateNodeCollateralValidator)
      lastNGlobalSnapshotStorage <-  hasherSelector.withCurrent { implicit hasher =>
        LastNGlobalSnapshotStorage.make[F](
          configuration.sharedConfigReader.lastGlobalSnapshotsSync,
          l0Service.asLeft
        )
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
          tessellation3Migration,
          BlockAcceptanceManager.make[F](validators.blockValidator, txHasher),
          AllowSpendBlockAcceptanceManager.make[F](validators.allowSpendBlockValidator),
          TokenLockBlockAcceptanceManager.make[F](validators.tokenLockBlockValidator),
          globalSnapshotStateChannelEventsProcessor,
          updateNodeParametersAcceptanceManager,
          updateDelegatedStakeAcceptanceManager,
          updateNodeCollateralAcceptanceManager,
          SpendActionValidator.make[F],
          configuration.collateral,
          configuration.delegatedStakingWithdrawalTimeLimit
        )
        val globalSnapshotContextFns = GlobalSnapshotContextFunctions.make[F](
          globalSnapshotAcceptanceManager,
          updateDelegatedStakeAcceptanceManager,
          configuration.delegatedStakingWithdrawalTimeLimit,
          tessellation3Migration
        )

        GlobalSnapshotContextService.make(globalSnapshotStateChannelEventsProcessor, globalSnapshotContextFns, lastNGlobalSnapshotStorage)
      }
      }
    } yield new TessellationServices[F](globalSnapshotContextService) {}

}

sealed abstract class TessellationServices[F[_]] private (
  val globalSnapshotContextService: GlobalSnapshotContextService[F]
)
