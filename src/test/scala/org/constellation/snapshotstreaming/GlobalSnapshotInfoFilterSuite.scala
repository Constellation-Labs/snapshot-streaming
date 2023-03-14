package org.constellation.snapshotstreaming

import java.security.KeyPair

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{Async, IO, Resource}
import cats.syntax.functor._
import cats.syntax.traverse._

import scala.collection.immutable.{SortedMap, SortedSet}

import org.tessellation.ext.cats.effect.ResourceIO
import org.tessellation.kryo.KryoSerializer
import org.tessellation.schema.address.Address
import org.tessellation.schema.balance.Balance
import org.tessellation.schema.block.DAGBlock
import org.tessellation.schema.height.Height
import org.tessellation.schema.transaction.DAGTransaction._
import org.tessellation.schema.transaction._
import org.tessellation.schema.{BlockAsActiveTip, BlockReference, GlobalSnapshotInfo}
import org.tessellation.sdk.sdkKryoRegistrar
import org.tessellation.security.hash.{Hash, ProofsHash}
import org.tessellation.security.key.ops.PublicKeyOps
import org.tessellation.security.signature.Signed
import org.tessellation.security.signature.Signed.forAsyncKryo
import org.tessellation.security.{KeyPairGenerator, SecurityProvider}
import org.tessellation.shared.sharedKryoRegistrar
import org.tessellation.syntax.sortedCollection._

import eu.timepit.refined.auto._
import org.constellation.snapshotstreaming.data.incrementalGlobalSnapshot
import weaver.MutableIOSuite

object GlobalSnapshotInfoFilterSuite extends MutableIOSuite {

  type Res = (KryoSerializer[IO], SecurityProvider[IO], KeyPair, KeyPair)

  override def sharedResource: Resource[IO, Res] =
    SecurityProvider.forAsync[IO].flatMap { implicit sp =>
      KryoSerializer.forAsync[IO](sharedKryoRegistrar ++ sdkKryoRegistrar).flatMap { implicit kp =>
        for {
          key1 <- KeyPairGenerator.makeKeyPair[IO].asResource
          key2 <- KeyPairGenerator.makeKeyPair[IO].asResource
        } yield (kp, sp, key1, key2)
      }
    }

  def mkInitialSnapshot()(implicit ks: KryoSerializer[IO]) =
    incrementalGlobalSnapshot(100L, 10L, 20L, Hash("abc"), Hash("def"))

  private val address3 = Address("DAG2AUdecqFwEGcgAcH1ac2wrsg8acrgGwrQojzw")
  private val address4 = Address("DAG2EUdecqFwEGcgAcH1ac2wrsg8acrgGwrQivxq")
  private val address5 = Address("DAG2EUdecqFwEGcgAcH1ac2wrsg8acrgGwrQitrs")

  test("set balance to 0 for source when not in info") { res =>
    implicit val (ks, sp, key1, key2) = res
    val address1 = key1.getPublic().toAddress
    val address2 = key2.getPublic().toAddress
    val totalInfo = GlobalSnapshotInfo.empty

    val rewards = createRewards(address1, address2, address5)
    for {
      txn1 <- createTxn(address1, key1, address2)
      txn2 <- createTxn(address2, key2, address1)
      txn3 <- createTxn(address2, key2, address5)
      blocks <- createBlocksWithTransactions(
        key1,
        NonEmptySet.fromSetUnsafe(SortedSet(txn1, txn2)),
        NonEmptySet.fromSetUnsafe(SortedSet(txn3))
      )
      snapshot <- incrementalGlobalSnapshot[IO](
        100L,
        10L,
        20L,
        Hash("abc"),
        Hash("def"),
        totalInfo,
        blocks,
        rewards = rewards
      )

      result = GlobalSnapshotInfoFilter.snapshotReferredBalancesInfo(snapshot, totalInfo)
    } yield expect.same(
      result,
      GlobalSnapshotInfo.empty.copy(balances = SortedMap(address1 -> Balance(0L), address2 -> Balance(0L)))
    )

  }

  test("leave balances for addresses from transactions") { res =>
    implicit val (ks, sp, key1, key2) = res
    val address1 = key1.getPublic().toAddress
    val address2 = key2.getPublic().toAddress
    val balances = createBalances(address1, address2, address3, address4, address5)
    val totalInfo = GlobalSnapshotInfo(SortedMap.empty, SortedMap.empty, balances, SortedMap.empty, SortedMap.empty)

    for {
      txn1 <- createTxn(address1, key1, address2)
      txn2 <- createTxn(address2, key2, address1)
      txn3 <- createTxn(address2, key2, address5)
      blocks <- createBlocksWithTransactions(
        key1,
        NonEmptySet.fromSetUnsafe(SortedSet(txn1, txn2)),
        NonEmptySet.fromSetUnsafe(SortedSet(txn3))
      )
      snapshot <- incrementalGlobalSnapshot[IO](100L, 10L, 20L, Hash("abc"), Hash("def"), totalInfo, blocks)

      result = GlobalSnapshotInfoFilter.snapshotReferredBalancesInfo(snapshot, totalInfo)
      expectedBalances = createBalances(address1, address2, address5)
    } yield expect.same(
      result,
      GlobalSnapshotInfo(SortedMap.empty, SortedMap.empty, expectedBalances, SortedMap.empty, SortedMap.empty)
    )
  }

  test("leave balances for addresses from transactions, but not set zero for destinations not in balances ") { res =>
    implicit val (ks, sp, key1, key2) = res
    val address1 = key1.getPublic().toAddress
    val address2 = key2.getPublic().toAddress
    val balances = createBalances(address1, address3, address4)
    val totalInfo = GlobalSnapshotInfo(SortedMap.empty, SortedMap.empty, balances, SortedMap.empty, SortedMap.empty)

    for {
      txn1 <- createTxn(address1, key1, address2)
      txn2 <- createTxn(address2, key2, address1)
      txn3 <- createTxn(address2, key2, address5)
      blocks <- createBlocksWithTransactions(
        key1,
        NonEmptySet.fromSetUnsafe(SortedSet(txn1, txn2)),
        NonEmptySet.fromSetUnsafe(SortedSet(txn3))
      )
      snapshot <- incrementalGlobalSnapshot[IO](100L, 10L, 20L, Hash("abc"), Hash("def"), totalInfo, blocks)

      result = GlobalSnapshotInfoFilter.snapshotReferredBalancesInfo(snapshot, totalInfo)
      expectedBalances = SortedMap(address1 -> Balance(1000L), address2 -> Balance(0L))
    } yield expect.same(
      result,
      GlobalSnapshotInfo(SortedMap.empty, SortedMap.empty, expectedBalances, SortedMap.empty, SortedMap.empty)
    )
  }

  test("leave balances for addresses from rewards") { res =>
    implicit val (ks, _, key1, key2) = res
    val address1 = key1.getPublic().toAddress
    val address2 = key2.getPublic().toAddress
    val balances = createBalances(address1, address2, address3, address4, address5)
    val totalInfo = GlobalSnapshotInfo(SortedMap.empty, SortedMap.empty, balances, SortedMap.empty, SortedMap.empty)

    val rewards = createRewards(address1, address2, address5)
    for {
      snapshot <- incrementalGlobalSnapshot[IO](100L, 10L, 20L, Hash("abc"), Hash("def"), totalInfo, rewards = rewards)

      result = GlobalSnapshotInfoFilter.snapshotReferredBalancesInfo(snapshot, totalInfo)
      expectedBalances = createBalances(address1, address2, address5)
    } yield expect.same(
      result,
      GlobalSnapshotInfo(SortedMap.empty, SortedMap.empty, expectedBalances, SortedMap.empty, SortedMap.empty)
    )
  }

  test("leave balances for addresses from rewards, but not set zero for these not in balances") { res =>
    implicit val (ks, _, key1, key2) = res
    val address1 = key1.getPublic().toAddress
    val address2 = key2.getPublic().toAddress
    val balances = createBalances(address1, address2, address3, address4)
    val totalInfo = GlobalSnapshotInfo(SortedMap.empty, SortedMap.empty, balances, SortedMap.empty, SortedMap.empty)

    val rewards = createRewards(address1, address2, address5)
    for {
      snapshot <- incrementalGlobalSnapshot[IO](100L, 10L, 20L, Hash("abc"), Hash("def"), totalInfo, rewards = rewards)

      result = GlobalSnapshotInfoFilter.snapshotReferredBalancesInfo(snapshot, totalInfo)
      expectedBalances = createBalances(address1, address2)
    } yield expect.same(
      result,
      GlobalSnapshotInfo(SortedMap.empty, SortedMap.empty, expectedBalances, SortedMap.empty, SortedMap.empty)
    )
  }

  private def createBalances(addresses: Address*) =
    addresses.map(address => address -> Balance(1000L)).toMap.toSortedMap

  private def createRewards(addresses: Address*) =
    addresses.map(address => RewardTransaction(address, TransactionAmount(1000L))).toSortedSet

  private def createBlocksWithTransactions[F[_]: Async: KryoSerializer: SecurityProvider](
    keyToSign: KeyPair,
    transactionsForBlock: NonEmptySet[Signed[DAGTransaction]]*
  ) = {
    val parent = BlockReference(Height(4L), ProofsHash("parent"))
    transactionsForBlock
      .traverse(txns =>
        forAsyncKryo[F, DAGBlock](DAGBlock(NonEmptyList.one(parent), txns), keyToSign)
          .map(BlockAsActiveTip(_, 0L))
      )
      .map(_.toList.toSortedSet)

  }

  def createTxn[F[_]: Async: KryoSerializer: SecurityProvider](
    src: Address,
    srcKey: KeyPair,
    dst: Address
  ): F[Signed[DAGTransaction]] =
    forAsyncKryo[F, DAGTransaction](
      DAGTransaction(
        src,
        dst,
        TransactionAmount(1L),
        TransactionFee.zero,
        TransactionReference.empty,
        TransactionSalt(0L)
      ),
      srcKey
    )

}
