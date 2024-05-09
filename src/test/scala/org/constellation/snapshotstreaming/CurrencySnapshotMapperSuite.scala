package org.constellation.snapshotstreaming

import java.security.KeyPair
import cats.data.NonEmptySet
import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all._

import scala.collection.immutable.SortedMap
import scala.collection.immutable.SortedSet
import org.tessellation.ext.cats.effect.ResourceIO
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.address.Address
import org.tessellation.schema.balance.Balance
import org.tessellation.schema.transaction._
import org.tessellation.node.shared.nodeSharedKryoRegistrar
import org.tessellation.security.hash.Hash
import org.tessellation.security.key.ops.PublicKeyOps
import org.tessellation.security.KeyPairGenerator
import org.tessellation.security.SecurityProvider
import org.tessellation.shared.sharedKryoRegistrar
import org.tessellation.syntax.sortedCollection._
import eu.timepit.refined.auto._
import org.constellation.snapshotstreaming.data.createBalances
import org.constellation.snapshotstreaming.data.createBlocksWithTransactions
import org.constellation.snapshotstreaming.data.createRewards
import org.constellation.snapshotstreaming.data.createTxn
import org.constellation.snapshotstreaming.data.emptyCurrencySnapshotInfo
import org.constellation.snapshotstreaming.data.hashSelect
import org.constellation.snapshotstreaming.data.incrementalCurrencySnapshot
import org.constellation.snapshotstreaming.opensearch.mapper.CurrencyIncrementalSnapshotMapper
import org.tessellation.currency.schema.currency.CurrencyIncrementalSnapshot
import org.tessellation.currency.schema.currency.CurrencySnapshotInfo
import weaver.MutableIOSuite
import org.tessellation.security.Hasher
import org.tessellation.json.JsonSerializer
import org.tessellation.security.Hashed
import org.tessellation.security.HasherSelector

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
    ks: KryoSerializer[IO],
    h: HasherSelector[IO]
  ): IO[Hashed[CurrencyIncrementalSnapshot]] =
    incrementalCurrencySnapshot(100L, 10L, 20L, Hash("abc"), Hash("def"))

  private val address5 = Address("DAG2AUdecqFwEGcgAcH1ac2wrsg8acrgGwrQojzw")
  private val address6 = Address("DAG2EUdecqFwEGcgAcH1ac2wrsg8acrgGwrQivxq")
  private val address7 = Address("DAG2EUdecqFwEGcgAcH1ac2wrsg8acrgGwrQitrs")

  test("set balance to 0 for source when not in info") { res =>
    implicit val (h, ks, js, sp, key1, key2, key3, key4) = res
    val address1 = key1.getPublic.toAddress
    val address2 = key2.getPublic.toAddress
    val address4 = key4.getPublic.toAddress
    val totalInfo = emptyCurrencySnapshotInfo

    val rewards = createRewards(address1, address2, address7)
    for {
      txn1 <- createTxn(address1, key1, address2)
      txn2 <- createTxn(address2, key2, address1)
      txn3 <- createTxn(address2, key2, address4)

      blocks <- createBlocksWithTransactions(
        key1,
        NonEmptySet.fromSetUnsafe(SortedSet(txn1, txn2)),
        NonEmptySet.fromSetUnsafe(SortedSet(txn3))
      )
      snapshot <- incrementalCurrencySnapshot[IO](
        100L,
        10L,
        20L,
        Hash("abc"),
        Hash("def"),
        totalInfo,
        blocks,
        rewards = rewards,
        feeTransactions = None
      )

      result = CurrencyIncrementalSnapshotMapper.make().snapshotReferredBalancesInfo(snapshot, totalInfo)
    } yield expect.same(
      result,
      SortedMap(address1 -> Balance(0L), address2 -> Balance(0L))
    )

  }

  test("leave balances for addresses from transactions") { res =>
    implicit val (h, ks, js, sp, key1, key2, key3, key4) = res
    val address1 = key1.getPublic.toAddress
    val address2 = key2.getPublic.toAddress
    val balances = createBalances(address1, address2, address5, address6, address7)
    val totalInfo = CurrencySnapshotInfo(SortedMap.empty, balances, None, None)

    for {
      txn1 <- createTxn(address1, key1, address2)
      txn2 <- createTxn(address2, key2, address1)
      txn3 <- createTxn(address2, key2, address7)
      blocks <- createBlocksWithTransactions(
        key1,
        NonEmptySet.fromSetUnsafe(SortedSet(txn1, txn2)),
        NonEmptySet.fromSetUnsafe(SortedSet(txn3))
      )
      snapshot <- incrementalCurrencySnapshot[IO](
        100L,
        10L,
        20L,
        Hash("abc"),
        Hash("def"),
        totalInfo,
        blocks,
        feeTransactions = None
      )

      result = CurrencyIncrementalSnapshotMapper.make().snapshotReferredBalancesInfo(snapshot, totalInfo)
      expectedBalances = createBalances(address1, address2, address7)
    } yield expect.same(result, expectedBalances)
  }

  test(
    "leave balances for addresses from transactions, but not set zero for destinations not in balances "
  ) { res =>
    implicit val (h, ks, js, sp, key1, key2, key3, key4) = res
    val address1 = key1.getPublic.toAddress
    val address2 = key2.getPublic.toAddress

    val balances = createBalances(address1, address5, address6)
    val totalInfo = CurrencySnapshotInfo(SortedMap.empty, balances, None, None)

    for {
      txn1 <- createTxn(address1, key1, address2)
      txn2 <- createTxn(address2, key2, address1)
      txn3 <- createTxn(address2, key2, address7)

      blocks <- createBlocksWithTransactions(
        key1,
        NonEmptySet.fromSetUnsafe(SortedSet(txn1, txn2)),
        NonEmptySet.fromSetUnsafe(SortedSet(txn3))
      )
      snapshot <- incrementalCurrencySnapshot[IO](
        100L,
        10L,
        20L,
        Hash("abc"),
        Hash("def"),
        totalInfo,
        blocks,
        feeTransactions = None
      )

      result = CurrencyIncrementalSnapshotMapper.make().snapshotReferredBalancesInfo(snapshot, totalInfo)
      expectedBalances = SortedMap(
        address1 -> Balance(1000L),
        address2 -> Balance(0L),
      )
    } yield expect.same(result, expectedBalances)
  }

  test(
    "leave balances for addresses from transactions, but not set zero for destinations not in balances "
  ) { res =>
    implicit val (h, ks, js, sp, key1, key2, key3, key4) = res
    val address1 = key1.getPublic.toAddress
    val address2 = key2.getPublic.toAddress
    val address3 = key3.getPublic.toAddress
    val address4 = key4.getPublic.toAddress
    val balances = createBalances(address1, address3, address6)
    val totalInfo = CurrencySnapshotInfo(SortedMap.empty, balances, None, None)

    for {
      txn1 <- createTxn(address1, key1, address2)
      txn2 <- createTxn(address2, key2, address1)
      txn3 <- createTxn(address2, key2, address5)
      blocks <- createBlocksWithTransactions(
        key1,
        NonEmptySet.fromSetUnsafe(SortedSet(txn1, txn2)),
        NonEmptySet.fromSetUnsafe(SortedSet(txn3))
      )
      snapshot <- incrementalCurrencySnapshot[IO](100L, 10L, 20L, Hash("abc"), Hash("def"), totalInfo, blocks)

      result = CurrencyIncrementalSnapshotMapper.make().snapshotReferredBalancesInfo(snapshot, totalInfo)
      expectedBalances = SortedMap(address1 -> Balance(1000L), address2 -> Balance(0L))
    } yield expect.same(result, expectedBalances)
  }

  test("leave balances for addresses from rewards") { res =>
    implicit val (h, ks, js, sp, key1, key2, key3, key4) = res
    val address1 = key1.getPublic.toAddress
    val address2 = key2.getPublic.toAddress
    val address3 = key3.getPublic.toAddress
    val address4 = key4.getPublic.toAddress
    val balances = createBalances(address1, address2, address7, address6, address7)
    val totalInfo = CurrencySnapshotInfo(SortedMap.empty, balances, None, None)

    val rewards = createRewards(address1, address2, address7)
    for {
      snapshot <- incrementalCurrencySnapshot[IO](
        100L,
        10L,
        20L,
        Hash("abc"),
        Hash("def"),
        totalInfo,
        rewards = rewards
      )

      result = CurrencyIncrementalSnapshotMapper.make().snapshotReferredBalancesInfo(snapshot, totalInfo)
      expectedBalances = createBalances(address1, address2, address7)
    } yield expect.same(result, expectedBalances)
  }

  test("leave balances for addresses from rewards, but not set zero for these not in balances") { res =>
    implicit val (h, ks, js, sp, key1, key2, key3, key4) = res
    val address1 = key1.getPublic.toAddress
    val address2 = key2.getPublic.toAddress
    val balances = createBalances(address1, address2, address5, address6)
    val totalInfo = CurrencySnapshotInfo(SortedMap.empty, balances, None, None)

    val rewards = createRewards(address1, address2, address7)
    for {
      snapshot <- incrementalCurrencySnapshot[IO](
        100L,
        10L,
        20L,
        Hash("abc"),
        Hash("def"),
        totalInfo,
        rewards = rewards
      )

      result = CurrencyIncrementalSnapshotMapper.make().snapshotReferredBalancesInfo(snapshot, totalInfo)
      expectedBalances = createBalances(address1, address2)
    } yield expect.same(result, expectedBalances)
  }

}
