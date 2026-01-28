package org.constellation.snapshotstreaming.opensearch.mapper

import cats.Show
import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap
import eu.timepit.refined.auto._
import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshotInfo}
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.nodeSharedKryoRegistrar
import io.constellationnetwork.schema.{BlockAsActiveTip, SnapshotOrdinal}
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.round.RoundId
import io.constellationnetwork.schema.tokenLock.{TokenLock => TessTokenLock, TokenLockAmount, TokenLockBlock, TokenLockFee, TokenLockReference}
import io.constellationnetwork.schema.transaction._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.shared.sharedKryoRegistrar
import org.constellation.snapshotstreaming.data._
import org.constellation.snapshotstreaming.mapper.CurrencyIncrementalSnapshotMapper
import org.constellation.snapshotstreaming.schema.TokenLocks.{TokenLock => SchemaTokenLock}
import weaver.MutableIOSuite

import java.security.KeyPair
import java.time.LocalDateTime
import java.util.UUID
import scala.collection.immutable.{SortedMap, SortedSet}

object CurrencySnapshotMapperSuite extends MutableIOSuite {

  // Resolve ambiguous implicit between tessellation's showSortedMapAsList and cats' catsShowForSortedMap
  implicit def showSortedMap[K: Show, V: Show]: Show[SortedMap[K, V]] = Show.catsShowForSortedMap

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

          js <- JsonSerializer.forAsync[IO].asResource
          hasherSelector = {
            implicit val j: JsonSerializer[IO] = js
            HasherSelector.forSync[IO](Hasher.forJson[IO], Hasher.forKryo[IO], hashSelect)
          }
        } yield (hasherSelector, kp, js, sp, key1, key2, key3, key4)
      }
    }

  def mkInitialSnapshot()(implicit
    h: HasherSelector[IO], js: JsonSerializer[IO]): IO[Hashed[CurrencyIncrementalSnapshot]] =
    incrementalCurrencySnapshot[IO](100L, 10L, 20L, Hash("abc"), Hash("def"))

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

  val testSignature = NonEmptySet.one(SignatureProof(Id(Hex("")), Signature(Hex(""))))

  test("map TokenLocks with replacementHash field") { res =>
    implicit val (h, ks, js, sp, key1, key2, _, _) = res
    val address1 = key1.getPublic.toAddress
    val address2 = key2.getPublic.toAddress

    val roundId = RoundId(UUID.randomUUID())

    // Token lock with replacement (e.g., increased stake)
    val tokenLockWithReplacement = TessTokenLock(
      source = address1,
      amount = TokenLockAmount(1000L),
      fee = TokenLockFee(10L),
      parent = TokenLockReference.empty,
      currencyId = None,
      unlockEpoch = Some(EpochProgress(100L)),
      replaceTokenLockRef = Some(Hash("ReplacedTokenLockHash123"))
    )

    // Token lock without replacement (original lock)
    val tokenLockWithoutReplacement = TessTokenLock(
      source = address2,
      amount = TokenLockAmount(2000L),
      fee = TokenLockFee(20L),
      parent = TokenLockReference.empty,
      currencyId = None,
      unlockEpoch = None,
      replaceTokenLockRef = None
    )

    val tokenLockBlock = TokenLockBlock(
      roundId = roundId,
      tokenLocks = NonEmptySet.of(
        Signed(tokenLockWithReplacement, testSignature),
        Signed(tokenLockWithoutReplacement, testSignature)
      )
    )

    implicit val hasher: Hasher[IO] = h.getCurrent
    val mapper = CurrencyIncrementalSnapshotMapper.make[IO]()

    for {
      snapshot <- incrementalCurrencySnapshot[IO](100L, 10L, 20L, Hash("abc"), Hash("def"))
      // Create a snapshot with token lock blocks
      snapshotWithTokenLocks = Hashed(
        Signed(
          snapshot.signed.value.copy(
            tokenLockBlocks = Some(SortedSet(Signed(tokenLockBlock, testSignature)))
          ),
          snapshot.signed.proofs
        ),
        snapshot.hash,
        snapshot.proofsHash
      )
      result <- mapper.mapTokenLocks(snapshotWithTokenLocks, LocalDateTime.now(), hasher)
      sorted = result.sortBy(_.amount)
    } yield {
      // Verify token lock with replacement has replacementHash set
      expect.same(sorted.head.replacementHash, Some("ReplacedTokenLockHash123")) and
      expect.same(sorted.head.amount, 1000L) and
      // Verify token lock without replacement has None for replacementHash
      expect.same(sorted(1).replacementHash, None) and
      expect.same(sorted(1).amount, 2000L)
    }
  }

}
