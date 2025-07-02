package org.constellation.snapshotstreaming.opensearch.mapper

import java.security.KeyPair
import cats.data.NonEmptySet
import cats.effect.IO
import cats.effect.Resource
import cats.implicits.catsSyntaxOptionId
import cats.syntax.all._

import scala.collection.immutable.SortedMap
import scala.collection.immutable.SortedSet
import org.tessellation.ext.cats.effect.ResourceIO
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.transaction._
import org.tessellation.schema.GlobalSnapshotInfo
import org.tessellation.node.shared.nodeSharedKryoRegistrar
import org.tessellation.security.hash.Hash
import org.tessellation.security.key.ops.PublicKeyOps
import org.tessellation.security.KeyPairGenerator
import org.tessellation.security.SecurityProvider
import org.tessellation.shared.sharedKryoRegistrar
import eu.timepit.refined.auto._
import org.constellation.snapshotstreaming.data.applyTransactions
import org.constellation.snapshotstreaming.data.createBalances
import org.constellation.snapshotstreaming.data.createBlocksWithTransactions
import org.constellation.snapshotstreaming.data.createRewards
import org.constellation.snapshotstreaming.data.createTxn
import org.constellation.snapshotstreaming.data.hashSelect
import org.constellation.snapshotstreaming.data.incrementalGlobalSnapshot
import org.constellation.snapshotstreaming.mapper.GlobalSnapshotMapper
import weaver.MutableIOSuite
import org.tessellation.security.Hasher
import org.tessellation.json.JsonSerializer
import org.tessellation.schema.GlobalIncrementalSnapshot
import org.tessellation.schema.balance.Balance
import org.tessellation.security.Hashed
import org.tessellation.security.HasherSelector

object GlobalSnapshotMapperSuite extends MutableIOSuite {

  type Res = (HasherSelector[IO], KryoSerializer[IO], SecurityProvider[IO], KeyPair, KeyPair, KeyPair, KeyPair)

  override def sharedResource: Resource[IO, Res] =
    SecurityProvider.forAsync[IO].flatMap { implicit sp =>
      KryoSerializer.forAsync[IO](sharedKryoRegistrar ++ nodeSharedKryoRegistrar).flatMap { implicit kp =>
        for {
          key1 <- KeyPairGenerator.makeKeyPair[IO].asResource
          key2 <- KeyPairGenerator.makeKeyPair[IO].asResource
          key3 <- KeyPairGenerator.makeKeyPair[IO].asResource
          key4 <- KeyPairGenerator.makeKeyPair[IO].asResource

          js <- JsonSerializer.forSync[IO].asResource
          hasherSelector = {
            implicit val j: JsonSerializer[IO] = js
            HasherSelector.forSync[IO](Hasher.forJson[IO], Hasher.forKryo[IO], hashSelect)
          }
        } yield (hasherSelector, kp, sp, key1, key2, key3, key4)
      }
    }

  def mkInitialSnapshot()(implicit h: HasherSelector[IO]): IO[Hashed[GlobalIncrementalSnapshot]] =
    incrementalGlobalSnapshot[IO](100L, 10L, 20L, Hash("abc"), Hash("def"))

}
