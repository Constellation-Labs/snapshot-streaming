package org.constellation.snapshotstreaming

import cats.data.NonEmptyMap

import scala.collection.immutable.SortedMap
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.util.Try
import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.peer.L0Peer
import io.constellationnetwork.schema.peer.PeerId
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import eu.timepit.refined.types.numeric.NonNegLong
import eu.timepit.refined.types.numeric.PosLong
import fs2.io.file.Path
import io.circe.parser.decode
import org.http4s.Uri
import io.constellationnetwork.node.shared.config.types
import io.constellationnetwork.node.shared.config.types.SharedConfigReader
import io.constellationnetwork.node.shared.domain.statechannel.FeeCalculatorConfig
import io.constellationnetwork.schema.epoch.EpochProgress

class Configuration(val sharedConfigReader: SharedConfigReader) {
  private val config: Config = ConfigFactory.load().resolve()

  private val httpClient = config.getConfig("snapshotStreaming.httpClient")
  private val node = config.getConfig("snapshotStreaming.node")
  private val opensearch = config.getConfig("snapshotStreaming.opensearch")
  private val s3 = config.getConfig("snapshotStreaming.s3")

  val lastFullSnapshotPath: Path = Path(config.getString("snapshotStreaming.lastSnapshotPath"))
  val lastIncrementalSnapshotPath: Path = Path(config.getString("snapshotStreaming.lastIncrementalSnapshotPath"))
  val collateral: Amount = Amount(NonNegLong.unsafeFrom(config.getLong("snapshotStreaming.collateral")))

  val environment: AppEnvironment =
    AppEnvironment.withNameInsensitive(config.getString("snapshotStreaming.environment"))

  val lastKryoHashOrdinal: SnapshotOrdinal =
    sharedConfigReader.lastKryoHashOrdinal.getOrElse(environment, SnapshotOrdinal.MinValue)

  val snapshotSize: types.SnapshotSizeConfig = sharedConfigReader.snapshot.size

  val feeConfigs: SortedMap[SnapshotOrdinal, FeeCalculatorConfig] = sharedConfigReader.feeConfigs.get(environment)
    .map(configs => SortedMap.from(configs))
    .getOrElse(SortedMap.empty[SnapshotOrdinal, FeeCalculatorConfig])

  val l0Peers: NonEmptyMap[PeerId, L0Peer] = NonEmptyMap.fromMapUnsafe(
    SortedMap.from(
      node.getStringList("l0Peers").asScala.toList.map(decode[L0Peer](_).toOption.get).map(p => p.id -> p)
    )
  )

  val pullInterval: FiniteDuration = {
    val d = Duration(node.getString("pullInterval"))
    FiniteDuration(d._1, d._2)
  }

  val pullLimit: PosLong = PosLong.from(node.getLong("pullLimit")).toOption.get

  val terminalSnapshotOrdinal: Option[SnapshotOrdinal] =
    Try(node.getLong("terminalSnapshotOrdinal")).toOption.map(NonNegLong.from(_).toOption.get).map(SnapshotOrdinal(_))

  val httpClientTimeout: Duration = Duration(httpClient.getString("timeout"))
  val httpClientIdleTime: Duration = Duration(httpClient.getString("idleTimeInPool"))

  private val opensearchHost: String = opensearch.getString("host")
  private val opensearchPort: Int = opensearch.getInt("port")
  val opensearchUrl = Uri.unsafeFromString(s"$opensearchHost:$opensearchPort")
  val snapshotsIndex: String = opensearch.getString("indexes.snapshots")
  val blocksIndex: String = opensearch.getString("indexes.blocks")
  val transactionsIndex: String = opensearch.getString("indexes.transactions")
  val balancesIndex: String = opensearch.getString("indexes.balances")
  val currencySnapshotsIndex: String = opensearch.getString("indexes.currency.snapshots")
  val currencyBlocksIndex: String = opensearch.getString("indexes.currency.blocks")
  val currencyTransactionsIndex: String = opensearch.getString("indexes.currency.transactions")
  val currencyFeeTransactionsIndex: String = opensearch.getString("indexes.currency.fee-transactions")
  val currencyBalancesIndex: String = opensearch.getString("indexes.currency.balances")
  val bulkSize: Int = opensearch.getInt("bulkSize")

  val bucketRegion: String = s3.getString("bucketRegion")
  val bucketName: String = s3.getString("bucketName")
  val bucketDir: String = s3.getString("bucketDir")
  val s3ApiEndpoint: Option[String] = Try(s3.getString("api.endpoint")).toOption
  val s3ApiRegion: Option[String] = Try(s3.getString("api.region")).toOption
  val s3ApiPathStyleEnabled: Option[Boolean] = Try(s3.getBoolean("api.pathStyleEnabled")).toOption

  val delegatedStakingWithdrawalTimeLimit = sharedConfigReader.delegatedStaking.withdrawalTimeLimit.getOrElse(environment, EpochProgress.MinValue)

  val tessellation3Migration = sharedConfigReader.fieldsAddedOrdinals.tessellation3Migration.getOrElse(environment, SnapshotOrdinal.MinValue)

}
