package org.constellation.snapshotstreaming

import cats.data.NonEmptyMap
import cats.effect.Sync
import com.comcast.ip4s.{Host, Port}
import eu.timepit.refined.types.all.PosLong
import eu.timepit.refined.pureconfig._

import scala.collection.immutable.SortedMap
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import org.tessellation.env.AppEnvironment
import org.tessellation.schema.SnapshotOrdinal
import org.tessellation.schema.peer.L0Peer
import org.tessellation.schema.peer.PeerId
import fs2.io.file.Path
import org.http4s.Uri
import org.tessellation.node.shared.cli.CliMethod
import org.tessellation.node.shared.config.types.{SharedConfig, SharedConfigReader}
import org.tessellation.security.hex.Hex
import org.tessellation.node.shared.ext.pureconfig._
import pureconfig.module.enumeratum._
import pureconfig.generic.auto._
import pureconfig._
import pureconfig.generic.ProductHint
import pureconfig.module.catseffect.syntax.CatsEffectConfigSource

final case class DbConfig(
  host: String,
  port: Int,
  user: String,
  password: Option[String],
  database: String,
  maxSessions: Int
)

final case class S3ApiConfig(endpoint: Option[String], region: Option[String], pathStyleEnabled: Option[Boolean])
final case class S3Config(bucketRegion: String, bucketName: String, bucketDir: String, api: S3ApiConfig)

final case class OpenSearchConfig(uri: Uri, bulkSize: Int, indexes: IndexesConfig)

final case class IndexesConfig(
  snapshots: String,
  blocks: String,
  transactions: String,
  balances: String,
  currency: CurrencyIndexConfig
)

final case class CurrencyIndexConfig(
  snapshots: String,
  blocks: String,
  transactions: String,
  balances: String,
  feeTransactions: String
)

final case class HttpClientConfig(
  timeout: FiniteDuration,
  idleTimeInPool: FiniteDuration
)

final case class NodeConfig(
  l0Peers: List[L0Peer],
  pullInterval: FiniteDuration,
  pullLimit: PosLong,
  terminalSnapshotOrdinal: Option[SnapshotOrdinal]
) {
  val l0PeersMap = NonEmptyMap.fromMapUnsafe(SortedMap.from(l0Peers.map(p => p.id -> p)))
}

final case class Reindexer(
  s3Parallelism: Int,
  s3Prefetch: Int,
  snapshotContextPrefetch: Int,
  dbChunks: Int,
  dbParallelism: Int
)

final case class SnapshotStreamingConfig(
  lastSnapshotPath: Path,
  lastIncrementalSnapshotPath: Path,
  environment: AppEnvironment,
  httpClient: HttpClientConfig,
  node: NodeConfig,
  s3: Option[S3Config],
  db: Option[DbConfig],
  opensearch: Option[OpenSearchConfig],
  reindexer: Option[Reindexer]
)

final case class AppConfig(
  snapshotStreaming: SnapshotStreamingConfig
)

object Configuration {

  implicit val finiteDurationReader: ConfigReader[FiniteDuration] =
    ConfigReader[String].map(Duration.apply).map(_.asInstanceOf[FiniteDuration])

  implicit val hostReader: ConfigReader[Host] = ConfigReader[String].map(Host.fromString).map(_.get)
  implicit val portReader: ConfigReader[Port] = ConfigReader[String].map(Port.fromString).map(_.get)
  implicit val peerIdReader: ConfigReader[PeerId] = ConfigReader[String].map(s => PeerId(Hex(s)))
  implicit val pathReader: ConfigReader[Path] = ConfigReader[String].map(Path.apply)
  implicit val uriReader: ConfigReader[Uri] = ConfigReader[String].map(Uri.unsafeFromString)

  implicit def hint[A]: ProductHint[A] = ProductHint[A](ConfigFieldMapping(CamelCase, CamelCase))

  ConfigSource.default.load[PosLong]
  ConfigSource.default.load[L0Peer]
  ConfigSource.default.load[FiniteDuration]
  ConfigSource.default.load[SnapshotOrdinal]
  ConfigSource.default.load[NodeConfig]

  def load[F[_]: Sync]: F[AppConfig] = ConfigSource.default
    .loadF[F, AppConfig]()

  def nodeSharedConfig(env: AppEnvironment, c: SharedConfigReader): SharedConfig =
    SharedConfig(
      env,
      c.gossip,
      null, // http: HttpConfig,
      c.leavingDelay,
      c.stateAfterJoining,
      CliMethod.collateralConfig(env, c.collateral.map(_.amount)),
      c.trust.storage,
      c.priorityPeerIds.get(env),
      c.snapshot.size,
      c.feeConfigs.get(env).map(SortedMap.from(_)).getOrElse(SortedMap.empty),
      c.forkInfoStorage,
      c.lastKryoHashOrdinal
    )

}
