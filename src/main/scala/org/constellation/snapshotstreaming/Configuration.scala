package org.constellation.snapshotstreaming

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Try

import scala.collection.JavaConverters._

class Configuration {

  private val config: Config = ConfigFactory.load().resolve()
  private val mode = config.getConfig("snapshot-streaming.mode")
  private val elasticsearch =
    config.getConfig("snapshot-streaming.elasticsearch")
  private val bucket = config.getConfig("snapshot-streaming.bucket")
  private val interval = config.getConfig("snapshot-streaming.interval")

  val getGenesis: Boolean = mode.getBoolean("getGenesis")

  val getSnapshots: Boolean = mode.getBoolean("getSnapshots")

  val startingHeight: Option[Long] =
    Try(interval.getLong("startingHeight")).toOption

  val endingHeight: Option[Long] = Try(interval.getLong("endingHeight")).toOption

  val lastSentHeightPath: String =
    Try(config.getString("snapshot-streaming.last-sent-height-path")).getOrElse("last-sent-height")

  val fileWithHeights: Option[String] =
    Try(interval.getString("fileWithHeights")).toOption

  val elasticsearchUrl: String =
    elasticsearch.getString("url")

  val elasticsearchPort: Int =
    elasticsearch.getInt("port")

  val elasticsearchTimeout: String =
    elasticsearch.getString("timeout")

  val elasticsearchTransactionsIndex: String =
    elasticsearch.getString("indexes.transactions")

  val elasticsearchCheckpointBlocksIndex: String =
    elasticsearch.getString("indexes.checkpoint-blocks")

  val elasticsearchSnapshotsIndex: String =
    elasticsearch.getString("indexes.snapshots")

  val elasticsearchBalancesIndex: String =
    elasticsearch.getString("indexes.balances")

  val maxParallelRequests: Int = elasticsearch.getInt("maxParallelRequests")
  val maxWaitQueueLimit: Int = elasticsearch.getInt("maxWaitQueueLimit")

  val bucketRegion: String =
    bucket.getString("region")

  val bucketNames: List[String] =
    bucket.getStringList("urls").asScala.toList

  val skipHeightOnFailure: Boolean = bucket.getBoolean("skipHeightOnFailure")

  val retryIntervalInSeconds: Int = bucket.getInt("retryIntervalInSeconds")
}
