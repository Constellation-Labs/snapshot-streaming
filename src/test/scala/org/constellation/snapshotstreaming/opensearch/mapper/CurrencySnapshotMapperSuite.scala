package org.constellation.snapshotstreaming.opensearch.mapper

import java.security.KeyPair
import cats.data.NonEmptySet
import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all._

import scala.collection.immutable.SortedMap
import scala.collection.immutable.SortedSet
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.schema.transaction._
import io.constellationnetwork.node.shared.nodeSharedKryoRegistrar
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.KeyPairGenerator
import io.constellationnetwork.security.SecurityProvider
import io.constellationnetwork.shared.sharedKryoRegistrar
import eu.timepit.refined.auto._
import org.constellation.snapshotstreaming.data.applyTransactions
import org.constellation.snapshotstreaming.data.createBalances
import org.constellation.snapshotstreaming.data.createBlocksWithTransactions
import org.constellation.snapshotstreaming.data.createFeeTxn
import org.constellation.snapshotstreaming.data.createRewards
import org.constellation.snapshotstreaming.data.createTxn
import org.constellation.snapshotstreaming.data.emptyCurrencySnapshotInfo
import org.constellation.snapshotstreaming.data.hashSelect
import org.constellation.snapshotstreaming.data.incrementalCurrencySnapshot
import io.constellationnetwork.currency.schema.currency.CurrencyIncrementalSnapshot
import io.constellationnetwork.currency.schema.currency.CurrencySnapshotInfo
import org.constellation.snapshotstreaming.mapper.CurrencyIncrementalSnapshotMapper
import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshotInfo}
import weaver.MutableIOSuite
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.BlockAsActiveTip
import io.constellationnetwork.security.Hashed
import io.constellationnetwork.security.HasherSelector

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
    incrementalCurrencySnapshot(100L, 10L, 20L, Hash("abc"), Hash("def"))

  test("explicitly sets balance to 0 for addressees missing in in info") { res =>
    implicit val (h, ks, js, sp, key1, key2, key3, _) = res
    val address1 = key1.getPublic.toAddress
    val address2 = key2.getPublic.toAddress
    val address3 = key3.getPublic.toAddress
    val initialBalances = createBalances(address1, address2)

    for {
      txn1 <- createTxn(address1, key1, address2, TransactionAmount(1000L))
      txn2 <- createTxn(address2, key2, address3, TransactionAmount(2000L))

      blocks <- createBlocksWithTransactions(
        key1,
        NonEmptySet.fromSetUnsafe(SortedSet(txn1)),
        NonEmptySet.fromSetUnsafe(SortedSet(txn2))
      )

      updatedBalances = applyTransactions(
        initialBalances,
        blocks.flatMap(_.block.transactions.toList).toList,
        List.empty,
        List.empty
      )

      updatedInfo = CurrencySnapshotInfo(SortedMap.empty, updatedBalances, None, None, None, None, None, None, None)

      snapshot <- incrementalCurrencySnapshot[IO](
        100L,
        10L,
        20L,
        Hash("abc"),
        Hash("def"),
        updatedInfo,
        blocks,
        feeTransactions = None
      )

      result = CurrencyIncrementalSnapshotMapper
        .make()
        .balanceDiff(snapshot, initialBalances.some, emptyCurrencySnapshotInfo)
    } yield expect.all(
      initialBalances(address1) === Balance(1000L),
      initialBalances(address2) === Balance(1000L),
      !initialBalances.contains(address3),
      !updatedBalances.contains(address1),
      !updatedBalances.contains(address2),
      updatedBalances(address3) === Balance(2000L),
      result === SortedMap(address1 -> Balance(0L), address2 -> Balance(0L))
    )
  }

  test("removes addresses that have transactions but the result balance hasn't changed") { res =>
    implicit val (h, ks, js, sp, key1, key2, _, _) = res
    val address1 = key1.getPublic.toAddress
    val address2 = key2.getPublic.toAddress
    val initialBalances = createBalances(address1, address2)

    for {
      txn1 <- createTxn(address1, key1, address2, TransactionAmount(1L))
      txn2 <- createTxn(address2, key2, address1, TransactionAmount(1L))
      blocks <- createBlocksWithTransactions(
        key1,
        NonEmptySet.fromSetUnsafe(SortedSet(txn1, txn2))
      )
      updatedBalances = applyTransactions(
        initialBalances,
        blocks.flatMap(_.block.transactions.toList).toList,
        List.empty,
        List.empty
      )
      updatedInfo = CurrencySnapshotInfo(SortedMap.empty, updatedBalances, None, None, None, None, None, None, None)

      snapshot <- incrementalCurrencySnapshot[IO](
        100L,
        10L,
        20L,
        Hash("abc"),
        Hash("def"),
        updatedInfo,
        blocks,
        feeTransactions = None
      )

      result = CurrencyIncrementalSnapshotMapper
        .make()
        .balanceDiff(snapshot, initialBalances.some, updatedInfo)
    } yield expect.same(
      result,
      updatedBalances - address1 - address2
    )
  }

  test("removes addresses that have fee transactions but the result balance hasn't changed") { res =>
    implicit val (h, ks, js, sp, key1, key2, _, _) = res
    val address1 = key1.getPublic.toAddress
    val address2 = key2.getPublic.toAddress
    val initialBalances = createBalances(address1, address2)

    for {
      txn1 <- createFeeTxn(address1, key1, address2)
      txn2 <- createFeeTxn(address2, key2, address1)
      blocks = SortedSet.empty[BlockAsActiveTip]
      feeTransactions = SortedSet(txn1, txn2).some
      updatedBalances = applyTransactions(
        initialBalances,
        blocks.flatMap(_.block.transactions.toList).toList,
        List.empty,
        feeTransactions.toList.flatten
      )
      updatedInfo = CurrencySnapshotInfo(SortedMap.empty, updatedBalances, None, None, None, None, None, None, None)
      snapshot <- incrementalCurrencySnapshot[IO](
        100L,
        10L,
        20L,
        Hash("abc"),
        Hash("def"),
        updatedInfo,
        blocks,
        feeTransactions = feeTransactions
      )

      result = CurrencyIncrementalSnapshotMapper
        .make()
        .balanceDiff(snapshot, initialBalances.some, updatedInfo)
    } yield expect.same(
      result,
      updatedBalances - address1 - address2
    )
  }

  test("leaves addresses that changed") { res =>
    implicit val (h, ks, js, sp, key1, key2, key3, key4) = res
    val address1 = key1.getPublic.toAddress
    val address2 = key2.getPublic.toAddress
    val address3 = key3.getPublic.toAddress
    val address4 = key4.getPublic.toAddress
    val initialBalances = createBalances(address1, address2, address3, address4)

    for {
      txn1 <- createTxn(address1, key1, address2, TransactionAmount(3L))
      txn2 <- createTxn(address2, key2, address1, TransactionAmount(5L))
      txn3 <- createTxn(address2, key2, address3, TransactionAmount(13L))
      blocks <- createBlocksWithTransactions(
        key1,
        NonEmptySet.fromSetUnsafe(SortedSet(txn1, txn2)),
        NonEmptySet.fromSetUnsafe(SortedSet(txn3))
      )
      updatedBalances = applyTransactions(
        initialBalances,
        blocks.flatMap(_.block.transactions.toList).toList,
        List.empty,
        List.empty
      )
      updatedInfo = CurrencySnapshotInfo(SortedMap.empty, updatedBalances, None, None, None, None, None, None, None)
      snapshot <- incrementalCurrencySnapshot[IO](
        100L,
        10L,
        20L,
        Hash("abc"),
        Hash("def"),
        updatedInfo,
        blocks,
        feeTransactions = None
      )

      result = CurrencyIncrementalSnapshotMapper
        .make()
        .balanceDiff(snapshot, initialBalances.some, updatedInfo)
    } yield expect.same(
      result,
      updatedBalances - address4
    )
  }

  test("leave balances for addresses from rewards") { res =>
    implicit val (h, _, js, _, key1, key2, key3, key4) = res
    val address1 = key1.getPublic.toAddress
    val address2 = key2.getPublic.toAddress
    val address3 = key3.getPublic.toAddress
    val address4 = key4.getPublic.toAddress

    val initialBalances = createBalances(address1, address2, address3, address4)
    val rewards = createRewards(address1, address2)

      val updatedBalances = applyTransactions(
        initialBalances,
        List.empty,
        rewards.toList,
        List.empty,
      )
      val updatedInfo = CurrencySnapshotInfo(SortedMap.empty, updatedBalances, None, None, None, None, None, None, None)

    for {
      snapshot <- incrementalCurrencySnapshot[IO](
        100L,
        10L,
        20L,
        Hash("abc"),
        Hash("def"),
        updatedInfo,
        rewards = rewards
      )

      result = CurrencyIncrementalSnapshotMapper.make().balanceDiff(snapshot, initialBalances.some, updatedInfo)
    } yield expect.same(
      result,
      updatedBalances - address3 - address4
    )
  }

}
