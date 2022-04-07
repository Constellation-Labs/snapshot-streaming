package org.constellation.snapshotstreaming

import com.typesafe.config.{Config, ConfigFactory}
import org.http4s.Uri

import scala.jdk.CollectionConverters._
import scala.concurrent.duration.Duration

class Configuration {
  private val config: Config = ConfigFactory.load().resolve()

  private val httpClient = config.getConfig("snapshotStreaming.httpClient")
  private val node = config.getConfig("snapshotStreaming.node")
  private val opensearch = config.getConfig("snapshotStreaming.opensearch")

  val nextOrdinalPath: String = config.getString("snapshotStreaming.nextOrdinalPath")

  val httpClientTimeout = Duration(httpClient.getString("timeout"))
  val httpClientIdleTime = Duration(httpClient.getString("idleTimeInPool"))

  val nodeIntervalInSeconds: Int = node.getInt("retryIntervalInSeconds")
  val nodeUrls: List[Uri] = node.getStringList("urls").asScala.toList.map(Uri.unsafeFromString)

  private val opensearchHost: String = opensearch.getString("host")
  private val opensearchPort: Int = opensearch.getInt("port")
  val opensearchUrl = Uri.unsafeFromString(s"$opensearchHost:$opensearchPort")
  val opensearchTimeout: String = opensearch.getString("timeout")
  val snapshotsIndex: String = opensearch.getString("indexes.snapshots")
  val blocksIndex: String = opensearch.getString("indexes.blocks")
  val transactionsIndex: String = opensearch.getString("indexes.transactions")
  val balancesIndex: String = opensearch.getString("indexes.balances")
  val balancesLimit: Int = opensearch.getInt("balancesLimit")
  val bulkSize: Int = opensearch.getInt("bulkSize")
}
