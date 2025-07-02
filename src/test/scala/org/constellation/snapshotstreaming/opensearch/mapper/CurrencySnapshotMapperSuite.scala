package org.constellation.snapshotstreaming.opensearch.mapper

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.syntax.all._
import eu.timepit.refined.auto._
import org.tessellation.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshotInfo}
import org.tessellation.ext.cats.effect.ResourceIO
import org.tessellation.json.JsonSerializer
import org.tessellation.kryo.KryoSerializer
import org.tessellation.node.shared.nodeSharedKryoRegistrar
import org.tessellation.schema.BlockAsActiveTip
import org.tessellation.schema.balance.Balance
import org.tessellation.schema.transaction._
import org.tessellation.security._
import org.tessellation.security.hash.Hash
import org.tessellation.security.key.ops.PublicKeyOps
import org.tessellation.shared.sharedKryoRegistrar
import org.constellation.snapshotstreaming.data._
import org.constellation.snapshotstreaming.mapper.CurrencyIncrementalSnapshotMapper
import weaver.MutableIOSuite

import java.security.KeyPair
import scala.collection.immutable.{SortedMap, SortedSet}

object CurrencySnapshotMapperSuite extends MutableIOSuite {

  type Res = (
    HasherSelector[IO],
      KryoSerializer[IO],
      JsonSerializer[IO],
      SecurityProvider[IO],
      KeyPair,
      KeyPair,
      KeyPair,
      KeyPair
    )

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
        } yield (hasherSelector, kp, js, sp, key1, key2, key3, key4)
      }
    }

  def mkInitialSnapshot()(implicit
                          h: HasherSelector[IO]
  ): IO[Hashed[CurrencyIncrementalSnapshot]] =
    incrementalCurrencySnapshot[IO](100L, 10L, 20L, Hash("abc"), Hash("def"))

}
