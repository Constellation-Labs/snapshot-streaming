package org.constellation.snapshotstreaming

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Try

class Configuration {

  private val config: Config = ConfigFactory.load().resolve()
  private val elasticsearch =
    config.getConfig("snapshot-streaming.elasticsearch")
  private val bucket = config.getConfig("snapshot-streaming.bucket")
  private val interval = config.getConfig("interval")

  val startingHeight: Long = Try(interval.getLong("start")).getOrElse(2L)
  val endingHeight: Option[Long] = Try(interval.getLong("end")).toOption

  val elasticsearchUrl: String =
    elasticsearch.getString("url")

  val elasticsearchPort: Int =
    elasticsearch.getInt("port")

  val elasticsearchTransactionsIndex: String =
    elasticsearch.getString("indexes.transactions")

  val elasticsearchCheckpointBlocksIndex: String =
    elasticsearch.getString("indexes.checkpoint-blocks")

  val elasticsearchSnapshotsIndex: String =
    elasticsearch.getString("indexes.snapshots")

  val bucketRegion: String =
    bucket.getString("region")

  val bucketName: String =
    bucket.getString("url")
}
