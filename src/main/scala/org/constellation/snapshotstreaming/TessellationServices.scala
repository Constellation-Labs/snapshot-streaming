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
import org.tessellation.node.shared.modules.{SharedStorages, SharedValidators}
import org.tessellation.schema.balance.Amount
import org.tessellation.security.signature.SignedValidator
import org.tessellation.security.Hasher
import org.tessellation.security.HasherSelector
import org.tessellation.security.SecurityProvider
import eu.timepit.refined.auto._
import org.tessellation.env.AppEnvironment
import org.tessellation.node.shared.config.types.SharedConfigReader
import org.tessellation.node.shared.domain.block.processing.BlockAcceptanceManager
import org.tessellation.node.shared.domain.statechannel.FeeCalculator
import org.tessellation.schema.SnapshotOrdinal
import org.tessellation.schema.cluster.ClusterId
import org.constellation.snapshotstreaming.Configuration

import java.util.UUID

object TessellationServices {

  def make[F[_]: Async: JsonSerializer: KryoSerializer: SecurityProvider: Parallel](
    env: AppEnvironment,
    configuration: SharedConfigReader
  )(implicit hasherSelector: HasherSelector[F]): F[TessellationServices[F]] = {
    implicit val txHasher = Hasher.forKryo
    val nodeConfig = Configuration.nodeSharedConfig(env, configuration)
    val validators = SharedValidators.make[F](
      None,
      None,
      None,
      nodeConfig.feeConfigs,
      nodeConfig.snapshotSize.maxStateChannelSnapshotBinarySizeInBytes,
      txHasher
    )

    for {
      stateChannelManager <- GlobalSnapshotStateChannelAcceptanceManager.make(None)
      jsonBrotliBinarySerializer <- JsonBrotliBinarySerializer.forSync[F]
      feeCalculator = FeeCalculator.make(nodeConfig.feeConfigs)
      currencySnapshotContextFns = {
        val currencySnapshotAcceptanceManager: CurrencySnapshotAcceptanceManager[F] =
          CurrencySnapshotAcceptanceManager.make(
            BlockAcceptanceManager.make[F](validators.currencyBlockValidator, txHasher),
            Amount(0L),
            validators.currencyMessageValidator
          )

        val currencyEventsCutter = CurrencyEventsCutter.make[F](None)
        val currencySnapshotCreator = CurrencySnapshotCreator
          .make[F](currencySnapshotAcceptanceManager, None, nodeConfig.snapshotSize, currencyEventsCutter)

        val currencySnapshotValidator = CurrencySnapshotValidator
          .make[F](currencySnapshotCreator, SignedValidator.make[F], None, None)
        CurrencySnapshotContextFunctions.make(currencySnapshotValidator)
      }

    } yield {
      val globalSnapshotStateChannelEventsProcessor: GlobalSnapshotStateChannelEventsProcessor[F] =
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
