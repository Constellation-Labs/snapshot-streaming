package org.constellation.snapshotstreaming

import cats.Parallel
import cats.effect.{Async, IO}
import cats.syntax.all._
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosInt
import io.circe.Printer
import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.json.{JsonBrotliBinarySerializer, JsonSerializer}
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.config.DefaultDelegatedRewardsConfigProvider
import io.constellationnetwork.node.shared.config.types.{AddressesConfig, DelegatedStakingConfig, PriceOracleConfig, SharedConfigReader}
import io.constellationnetwork.node.shared.domain.delegatedStake.UpdateDelegatedStakeAcceptanceManager
import io.constellationnetwork.node.shared.domain.node.UpdateNodeParametersAcceptanceManager
import io.constellationnetwork.node.shared.domain.nodeCollateral.UpdateNodeCollateralAcceptanceManager
import io.constellationnetwork.node.shared.domain.priceOracle.{PriceStateUpdater, PricingUpdateValidator}
import io.constellationnetwork.node.shared.domain.snapshot.services.GlobalL0Service
import io.constellationnetwork.node.shared.domain.snapshot.storage.LastNGlobalSnapshotStorage
import io.constellationnetwork.node.shared.domain.statechannel.FeeCalculator
import io.constellationnetwork.node.shared.domain.swap.SpendActionValidator
import io.constellationnetwork.node.shared.domain.swap.block.AllowSpendBlockAcceptanceManager
import io.constellationnetwork.node.shared.domain.tokenlock.block.TokenLockBlockAcceptanceManager
import io.constellationnetwork.node.shared.infrastructure.block.processing.BlockAcceptanceManager
import io.constellationnetwork.node.shared.infrastructure.consensus.CurrencySnapshotEventValidationErrorStorage
import io.constellationnetwork.node.shared.infrastructure.snapshot._
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.currency.CurrencySnapshotAcceptanceManager
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.{GlobalSnapshotAcceptanceManager, GlobalSnapshotStateChannelAcceptanceManager, GlobalSnapshotStateChannelEventsProcessor}
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage.{LastNGlobalSnapshotStorage, LastSnapshotStorage}
import io.constellationnetwork.node.shared.logger.NoDbLogger
import io.constellationnetwork.node.shared.modules.SharedValidators
import io.constellationnetwork.schema.{CurrencyStateProofSelector, GlobalIncrementalSnapshot, GlobalSnapshotInfo, GlobalStateProofSelector, SnapshotOrdinal}
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.security.signature.SignedValidator
import io.constellationnetwork.security.{Hasher, HasherSelector, SecurityProvider}

object TessellationServices {

  def make[F[_] : Async : Parallel : JsonSerializer : KryoSerializer : SecurityProvider](
    env          : AppEnvironment,
    configuration: SharedConfigReader,
    l0Service    : GlobalL0Service[F]
  )(
    implicit hasherSelector: HasherSelector[F],
    globalStateProofSelector: GlobalStateProofSelector,
    currencyStateProofSelector: CurrencyStateProofSelector
  ): F[TessellationServices[F]] =
    for {
      _ <- Async[F].unit
      nodeConfig = Configuration.nodeSharedConfig(env, configuration)
      tessellation3Migration = configuration.fieldsAddedOrdinals.tessellation3Migration.getOrElse(env, SnapshotOrdinal.MinValue)
      txHasher = Hasher.forKryo
      validators = hasherSelector.withCurrent { implicit hasher =>
        SharedValidators.make[F](
          env,
          AddressesConfig(Set.empty),
          None,
          None,
          None,
          nodeConfig.feeConfigs,
          nodeConfig.snapshotSize.maxStateChannelSnapshotBinarySizeInBytes,
          txHasher,
          DelegatedStakingConfig(configuration.delegatedStaking.minRewardFraction, configuration.delegatedStaking.maxRewardFraction, configuration.delegatedStaking.maxMetadataFieldsChars, configuration.delegatedStaking.maxTokenLocksPerAddress, configuration.delegatedStaking.minTokenLockAmount, configuration.delegatedStaking.withdrawalTimeLimit),
          nodeConfig.priceOracle
        )
      }

      stateChannelManager <- GlobalSnapshotStateChannelAcceptanceManager.make(None)
      printer = Printer(dropNullValues = false, indent = "")
      jsonBrotliBinarySerializer <- JsonBrotliBinarySerializer.forAsync[F](printer)
      feeCalculator = FeeCalculator.make(nodeConfig.feeConfigs)

      lastNGlobalSnapshotStorage <- hasherSelector.withCurrent { implicit hasher =>
        LastNGlobalSnapshotStorage.make[F](
          configuration.lastGlobalSnapshotsSync
        )
      }
      lastGlobalSnapshotStorage <- LastSnapshotStorage.make[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo]

      currencySnapshotAcceptanceManager <- CurrencySnapshotAcceptanceManager.make(
        configuration.fieldsAddedOrdinals,
        env,
        configuration.lastGlobalSnapshotsSync,
        BlockAcceptanceManager.make[F](validators.currencyBlockValidator, txHasher),
        TokenLockBlockAcceptanceManager.make(validators.tokenLockBlockValidator),
        AllowSpendBlockAcceptanceManager.make(validators.allowSpendBlockValidator),
        Amount(0L),
        validators.currencyMessageValidator,
        validators.feeTransactionValidator,
        validators.globalSnapshotSyncValidator,
        lastNGlobalSnapshotStorage,
        lastGlobalSnapshotStorage
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
                nodeConfig.snapshotSize,
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
      noDbLogger <- NoDbLogger.makeUnsafe
      globalSnapshotContextService = hasherSelector.withCurrent { implicit hasher => {
        val globalSnapshotStateChannelEventsProcessor =
          GlobalSnapshotStateChannelEventsProcessor.make[F](
            validators.stateChannelValidator,
            stateChannelManager,
            currencySnapshotContextFns,
            feeCalculator
          )
        val priceOracle = configuration.priceOracle.getOrElse(env, PriceOracleConfig.default)
        val globalSnapshotAcceptanceManager: GlobalSnapshotAcceptanceManager[F] = GlobalSnapshotAcceptanceManager.make(
          configuration.fieldsAddedOrdinals,
          configuration.metagraphsSync,
          env,
          BlockAcceptanceManager.make[F](validators.blockValidator, txHasher),
          AllowSpendBlockAcceptanceManager.make[F](validators.allowSpendBlockValidator),
          TokenLockBlockAcceptanceManager.make[F](validators.tokenLockBlockValidator),
          globalSnapshotStateChannelEventsProcessor,
          updateNodeParametersAcceptanceManager,
          updateDelegatedStakeAcceptanceManager,
          updateNodeCollateralAcceptanceManager,
          SpendActionValidator.make[F],
          PricingUpdateValidator.make[F](priceOracle.allowedMetagraphIds, priceOracle.minEpochsBetweenUpdates),
          PriceStateUpdater.make[F](env, DefaultDelegatedRewardsConfigProvider),
          configuration.collateral.get.amount,
          configuration.delegatedStaking.withdrawalTimeLimit.getOrElse(env, EpochProgress.MinValue),
          noDbLogger
        )

        val globalSnapshotContextFns = GlobalSnapshotContextFunctions.make[F](
          globalSnapshotAcceptanceManager,
          updateDelegatedStakeAcceptanceManager,
          configuration.delegatedStaking.withdrawalTimeLimit.getOrElse(env, EpochProgress.MinValue),
          tessellation3Migration,
          configuration.fieldsAddedOrdinals.setSumFix.getOrElse(env, SnapshotOrdinal.MinValue)
        )

        GlobalSnapshotContextService.make(globalSnapshotStateChannelEventsProcessor, globalSnapshotContextFns, lastNGlobalSnapshotStorage, lastGlobalSnapshotStorage)
      }
      }
    } yield new TessellationServices[F](globalSnapshotContextService) {}

}

sealed abstract class TessellationServices[F[_]] private(
  val globalSnapshotContextService: GlobalSnapshotContextService[F]
)
