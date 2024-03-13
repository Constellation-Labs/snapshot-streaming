package org.constellation.snapshotstreaming

import cats.data.NonEmptyMap

import scala.collection.immutable.SortedMap
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.CollectionConverters._
import scala.util.Try

import org.tessellation.env.AppEnvironment
import org.tessellation.schema.SnapshotOrdinal
import org.tessellation.schema.balance.Amount
import org.tessellation.schema.peer.{L0Peer, PeerId}

import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import fs2.io.file.Path
import io.circe.parser.decode
import org.http4s.Uri

class Configuration {
  private val config: Config = ConfigFactory.load().resolve()

  private val httpClient = config.getConfig("snapshotStreaming.httpClient")
  private val node = config.getConfig("snapshotStreaming.node")
  private val opensearch = config.getConfig("snapshotStreaming.opensearch")
  private val s3 = config.getConfig("snapshotStreaming.s3")

  val lastFullSnapshotPath: Path = Path(config.getString("snapshotStreaming.lastSnapshotPath"))
  val lastIncrementalSnapshotPath: Path = Path(config.getString("snapshotStreaming.lastIncrementalSnapshotPath"))
  val collateral: Amount = Amount(NonNegLong.unsafeFrom(config.getLong("snapshotStreaming.collateral")))
  val environment: AppEnvironment = AppEnvironment.withNameInsensitive(config.getString("snapshotStreaming.environment"))

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
  val currencyBalancesIndex: String = opensearch.getString("indexes.currency.balances")
  val bulkSize: Int = opensearch.getInt("bulkSize")

  val bucketRegion: String = s3.getString("bucketRegion")
  val bucketName: String = s3.getString("bucketName")
  val bucketDir: String = s3.getString("bucketDir")
  val s3ApiEndpoint: Option[String] = Try(s3.getString("api.endpoint")).toOption
  val s3ApiRegion: Option[String] = Try(s3.getString("api.region")).toOption
  val s3ApiPathStyleEnabled: Option[Boolean] = Try(s3.getBoolean("api.pathStyleEnabled")).toOption

}
