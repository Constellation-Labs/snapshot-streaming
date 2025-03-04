package org.constellation.snapshotstreaming

import cats.effect.Async
import cats.syntax.flatMap._
import cats.syntax.functor._
import io.constellationnetwork.json.JsonBrotliBinarySerializer
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.infrastructure.block.processing.BlockAcceptanceManager
import io.constellationnetwork.node.shared.infrastructure.snapshot._
import io.constellationnetwork.node.shared.modules.{SharedStorages, SharedValidators}
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.security.signature.SignedValidator
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.HasherSelector
import io.constellationnetwork.security.SecurityProvider
import eu.timepit.refined.auto._
import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.node.shared.config.types.{AddressesConfig, SharedConfigReader}
import io.constellationnetwork.node.shared.domain.block.processing.BlockAcceptanceManager
import io.constellationnetwork.node.shared.domain.node.UpdateNodeParametersAcceptanceManager
import io.constellationnetwork.node.shared.domain.statechannel.FeeCalculator
import io.constellationnetwork.node.shared.domain.swap.block.AllowSpendBlockAcceptanceManager
import io.constellationnetwork.node.shared.domain.tokenlock.block.TokenLockBlockAcceptanceManager
import io.constellationnetwork.node.shared.domain.transaction.FeeTransactionValidator
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.cluster.ClusterId
import org.constellation.snapshotstreaming.Configuration

import java.util.UUID

object TessellationServices {

  def make[F[_]: Async: JsonSerializer: KryoSerializer: SecurityProvider](
    env: AppEnvironment,
    configuration: SharedConfigReader
  )(implicit hasherSelector: HasherSelector[F]): F[TessellationServices[F]] = {
    implicit val txHasher = Hasher.forKryo
    val nodeConfig = Configuration.nodeSharedConfig(env, configuration)
    val tokenLocksAddedToGl0Ordinal = configuration.fieldsAddedOrdinals.globalTokenLocks.getOrElse(env, SnapshotOrdinal.MinValue)
    val validators = SharedValidators.make[F](
      nodeConfig.addresses,
      None,
      None,
      None,
      nodeConfig.feeConfigs,
      nodeConfig.snapshotSize.maxStateChannelSnapshotBinarySizeInBytes,
      txHasher,
      configuration.delegatedStaking
    )



    for {
      stateChannelManager <- GlobalSnapshotStateChannelAcceptanceManager.make(None)
      jsonBrotliBinarySerializer <- JsonBrotliBinarySerializer.forSync[F]
      feeCalculator = FeeCalculator.make(nodeConfig.feeConfigs)
      sharedStorage <- SharedStorages.make(ClusterId(UUID.randomUUID()), nodeConfig)
      currencySnapshotContextFns = {
        val currencySnapshotAcceptanceManager: CurrencySnapshotAcceptanceManager[F] =
          CurrencySnapshotAcceptanceManager.make(
            configuration.lastGlobalSnapshotsSync,
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
          .make[F](currencySnapshotAcceptanceManager, None, nodeConfig.snapshotSize, currencyEventsCutter, sharedStorage.currencySnapshotEventValidationError)

        val currencySnapshotValidator = CurrencySnapshotValidator
          .make[F](tokenLocksAddedToGl0Ordinal, currencySnapshotCreator, SignedValidator.make[F], None, None)
        CurrencySnapshotContextFunctions.make(currencySnapshotValidator)
      }

    } yield {
      val globalSnapshotStateChannelEventsProcessor =
        GlobalSnapshotStateChannelEventsProcessor.make[F](
          validators.stateChannelValidator,
          stateChannelManager,
          currencySnapshotContextFns,
          jsonBrotliBinarySerializer,
          feeCalculator
        )

      val updateNodeParametersAcceptanceManager = UpdateNodeParametersAcceptanceManager.make(validators.updateNodeParametersValidator)
      val globalSnapshotAcceptanceManager: GlobalSnapshotAcceptanceManager[F] = GlobalSnapshotAcceptanceManager.make(
        tokenLocksAddedToGl0Ordinal,
        BlockAcceptanceManager.make[F](validators.blockValidator, txHasher),
        AllowSpendBlockAcceptanceManager.make[F](validators.allowSpendBlockValidator),
        TokenLockBlockAcceptanceManager.make[F](validators.tokenLockBlockValidator),
        globalSnapshotStateChannelEventsProcessor,
        updateNodeParametersAcceptanceManager,
        validators.spendActionValidator,
        nodeConfig.collateral.amount
      )



      val globalSnapshotContextFns = GlobalSnapshotContextFunctions.make[F](globalSnapshotAcceptanceManager)
      val globalSnapshotContextService =
        GlobalSnapshotContextService.make(globalSnapshotStateChannelEventsProcessor, globalSnapshotContextFns)
      new TessellationServices[F](globalSnapshotContextService) {}
    }
  }

}

sealed abstract class TessellationServices[F[_]] private (
  val globalSnapshotContextService: GlobalSnapshotContextService[F]
)
