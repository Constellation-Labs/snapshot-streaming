package org.constellation.snapshotstreaming.opensearch.mapper

import java.security.KeyPair
import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.implicits.catsSyntaxOptionId
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.schema.transaction._
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.node.shared.nodeSharedKryoRegistrar
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.KeyPairGenerator
import io.constellationnetwork.security.SecurityProvider
import io.constellationnetwork.shared.sharedKryoRegistrar
import eu.timepit.refined.auto._
import io.constellationnetwork.env.AppEnvironment
import org.constellation.snapshotstreaming.data.applyTransactions
import org.constellation.snapshotstreaming.data.createBalances
import org.constellation.snapshotstreaming.data.createBlocksWithTransactions
import org.constellation.snapshotstreaming.data.createRewards
import org.constellation.snapshotstreaming.data.createTxn
import org.constellation.snapshotstreaming.data.hashSelect
import org.constellation.snapshotstreaming.data.incrementalGlobalSnapshot
import org.constellation.snapshotstreaming.mapper.GlobalSnapshotMapper
import weaver.MutableIOSuite
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.types.{SharedConfig, SharedConfigReader}
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.security.Hashed
import io.constellationnetwork.security.HasherSelector
import org.constellation.snapshotstreaming.Configuration
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import io.constellationnetwork.node.shared.ext.pureconfig._
import eu.timepit.refined.pureconfig._
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.delegatedStake.{
  DelegatedStakeAmount,
  DelegatedStakeRecord,
  PendingDelegatedStakeWithdrawal,
  UpdateDelegatedStake
}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import org.constellation.snapshotstreaming.schema.{DelegatedStakingCreate, DelegatedStakingWithdraw}
import pureconfig.module.enumeratum._
import eu.timepit.refined.types.all._
import io.constellationnetwork.schema.epoch.EpochProgress

object GlobalSnapshotMapperSuite extends MutableIOSuite {

  val sharedCfg =
    Configuration.nodeSharedConfig(AppEnvironment.Dev, ConfigSource.default.loadOrThrow[SharedConfigReader])

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

  test("explicitly sets balance to 0 for addressees missing in in info") { res =>
    implicit val (h, ks, sp, key1, key2, key3, _) = res
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

      updatedInfo = GlobalSnapshotInfo(
        SortedMap.empty,
        SortedMap.empty,
        updatedBalances,
        SortedMap.empty,
        SortedMap.empty,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None
      )

      snapshot <- incrementalGlobalSnapshot[IO](
        100L,
        10L,
        20L,
        Hash("abc"),
        Hash("def"),
        updatedInfo,
        blocks
      )

      result = GlobalSnapshotMapper
        .make(sharedCfg)
        .balanceDiff(snapshot, initialBalances.some, GlobalSnapshotInfo.empty)
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
    implicit val (h, ks, sp, key1, key2, _, _) = res
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
      updatedInfo = GlobalSnapshotInfo(
        SortedMap.empty,
        SortedMap.empty,
        updatedBalances,
        SortedMap.empty,
        SortedMap.empty,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None
      )

      snapshot <- incrementalGlobalSnapshot[IO](
        100L,
        10L,
        20L,
        Hash("abc"),
        Hash("def"),
        updatedInfo,
        blocks
      )

      result = GlobalSnapshotMapper
        .make(sharedCfg)
        .balanceDiff(snapshot, initialBalances.some, updatedInfo)
    } yield expect.same(
      result,
      updatedBalances - address1 - address2
    )
  }

  test("leaves addresses that changed") { res =>
    implicit val (h, ks, sp, key1, key2, key3, key4) = res
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
      updatedInfo = GlobalSnapshotInfo(
        SortedMap.empty,
        SortedMap.empty,
        updatedBalances,
        SortedMap.empty,
        SortedMap.empty,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None
      )
      snapshot <- incrementalGlobalSnapshot[IO](
        100L,
        10L,
        20L,
        Hash("abc"),
        Hash("def"),
        updatedInfo,
        blocks
      )

      result = GlobalSnapshotMapper
        .make(sharedCfg)
        .balanceDiff(snapshot, initialBalances.some, updatedInfo)
    } yield expect.same(
      result,
      updatedBalances - address4
    )
  }

  test("leave balances for addresses from rewards") { res =>
    implicit val (h, ks, _, key1, key2, key3, key4) = res
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
      List.empty
    )
    val updatedInfo = GlobalSnapshotInfo(
      SortedMap.empty,
      SortedMap.empty,
      updatedBalances,
      SortedMap.empty,
      SortedMap.empty,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None
    )

    for {
      snapshot <- incrementalGlobalSnapshot[IO](
        100L,
        10L,
        20L,
        Hash("abc"),
        Hash("def"),
        updatedInfo,
        rewards = rewards
      )

      result = GlobalSnapshotMapper.make(sharedCfg).balanceDiff(snapshot, initialBalances.some, updatedInfo)
    } yield expect.same(
      result,
      updatedBalances - address3 - address4
    )
  }

  val signature = NonEmptySet.one(SignatureProof(Id(Hex("")), Signature(Hex(""))))

  def buildSignedCreateStakeEvent(address: Address, peerId: String, amount: NonNegLong, tokenLockRef: String) = Signed(
    UpdateDelegatedStake.Create(
      source = address,
      nodeId = PeerId(Hex(peerId)),
      amount = DelegatedStakeAmount(amount),
      tokenLockRef = Hash(tokenLockRef)
    ),
    signature
  )

  test("extract only new and updated create stake references") { res =>
    implicit val (hs, ks, sp, key1, key2, key3, key4) = res
    val address1 = key1.getPublic.toAddress
    val address2 = key2.getPublic.toAddress

    def buildDSR(
      address: Address,
      peerId: String,
      amount: NonNegLong,
      tokenLockRef: String,
      createdAt: NonNegLong,
      rewards: NonNegLong
    ) = DelegatedStakeRecord(
      event = buildSignedCreateStakeEvent(address, peerId, amount, tokenLockRef),
      createdAt = SnapshotOrdinal(createdAt),
      rewards = Amount(rewards)
    )

    val oldStakes = Seq(
      address1 -> SortedSet(
        buildDSR(address1, "Peer1", 150L, "TokenRef1", 10L, 5L),
        buildDSR(address1, "Peer2", 150L, "TokenRef2", 11L, 10L)
      ),
      address2 -> SortedSet(buildDSR(address2, "Peer3", 150L, "TokenRef3", 12L, 15L))
    )

    // new staking for TokenRef22 and move staking for TokenRef3 moved to peer4 and
    val newStakes = Seq(
      address1 -> SortedSet(
        buildDSR(address1, "Peer2", 150L, "TokenRef2", 11L, 15L),
        buildDSR(address1, "Peer2a", 150L, "TokenRef22", 12L, 10L)
      ),
      address2 -> SortedSet(buildDSR(address2, "Peer4", 150L, "TokenRef3", 25L, 20L))
    )

    val oldSnapshotInfo = GlobalSnapshotInfo(
      SortedMap.empty,
      SortedMap.empty,
      SortedMap.empty,
      SortedMap.empty,
      SortedMap.empty,
      None,
      None,
      None,
      None,
      None,
      None,
      Some(SortedMap.from(oldStakes)),
      None,
      None,
      None,
      None
    )

    val newSnapshotInfo = GlobalSnapshotInfo(
      SortedMap.empty,
      SortedMap.empty,
      SortedMap.empty,
      SortedMap.empty,
      SortedMap.empty,
      None,
      None,
      None,
      None,
      None,
      None,
      Some(SortedMap.from(newStakes)),
      None,
      None,
      None,
      None
    )
    val hasher = hs.getCurrent
    val gsm = GlobalSnapshotMapper.make(sharedCfg)
    for {
      activeHashedDelegatedStakes <- gsm.activeHashedDelegatedStakes(newSnapshotInfo)(hasher)
      result <- gsm.mapDelegatedStakingCreates(
        Hash("SnapshotHash1"),
        activeHashedDelegatedStakes,
        Some(oldSnapshotInfo),
        hasher
      )
      sorted = result.sortBy(_.createdAtOrdinal)
    } yield expect.same(
      sorted,
      Vector(
        DelegatedStakingCreate(
          "SnapshotHash1",
          sorted(0).hash,
          12L,
          address1.value,
          "Peer2a",
          150L,
          0L,
          10L,
          "TokenRef22",
          "0000000000000000000000000000000000000000000000000000000000000000",
          None
        ),
        DelegatedStakingCreate(
          "SnapshotHash1",
          sorted(1).hash,
          25L,
          address2.value,
          "Peer4",
          150L,
          0L,
          20L,
          "TokenRef3",
          "0000000000000000000000000000000000000000000000000000000000000000",
          sorted(1).transferFrom
        )
      )
    )
  }

  test("extract only new and completed stakes withdrawal") { res =>
    implicit val (hs, ks, sp, key1, key2, key3, key4) = res
    val address1 = key1.getPublic.toAddress
    val address2 = key2.getPublic.toAddress

    val withdrawalTimeLimit = sharedCfg.delegatedStaking.withdrawalTimeLimit(sharedCfg.environment)

    def buildDSW(
      address: Address,
      peerId: String,
      amount: NonNegLong,
      tokenLockRef: String,
      acceptedOrdinal: NonNegLong,
      rewards: NonNegLong,
      epochProgress: NonNegLong
    ) = PendingDelegatedStakeWithdrawal(
      event = buildSignedCreateStakeEvent(address, peerId, amount, tokenLockRef),
      rewards = Amount(rewards),
      acceptedOrdinal = SnapshotOrdinal(acceptedOrdinal),
      createdAt = EpochProgress(epochProgress)
    )

    val addr1P1 = buildDSW(address1, "Peer1", 200L, "TokenRef1", 10L, 111L, 10L)
    val addr1P2 = buildDSW(address1, "Peer2", 250L, "TokenRef2", 11L, 222L, 11L)
    val addr1P5 = buildDSW(address1, "Peer0", 166L, "TokenRef0", 11L, 555L, 12L)
    val addr2P3 = buildDSW(address2, "Peer3", 222L, "TokenRef3", 12L, 333L, 13L)
    val addr2P4 = buildDSW(address2, "Peer4", 444L, "TokenRef4", 15L, 444L, 24L)

    val oldWithdrawalStakes = Seq(
      address1 -> SortedSet(addr1P1, addr1P2),
      address2 -> SortedSet(addr2P3)
    )

    // new withdrawal for address1,peer5 and address2,Peer4
    // still pending for address1,Peer2 and address2,Peer3
    // still pending for address1,Peer2 and address2,Peer3
    // completed for address1,Peer1
    val newWithdrawalStakes = Seq(
      address1 -> SortedSet(addr1P5, addr1P2),
      address2 -> SortedSet(addr2P3, addr2P4)
    )

    val oldSnapshotInfo = GlobalSnapshotInfo(
      SortedMap.empty,
      SortedMap.empty,
      SortedMap.empty,
      SortedMap.empty,
      SortedMap.empty,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      Some(SortedMap.from(oldWithdrawalStakes)),
      None,
      None,
      None
    )

    val newSnapshotInfo = GlobalSnapshotInfo(
      SortedMap.empty,
      SortedMap.empty,
      SortedMap.empty,
      SortedMap.empty,
      SortedMap.empty,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      Some(SortedMap.from(newWithdrawalStakes)),
      None,
      None,
      None
    )
    val hasher = hs.getCurrent
    val gsm = GlobalSnapshotMapper.make(sharedCfg)
    for {
      result <- gsm.mapDelegatedStakingWithdraws(
        Hash("SnapshotHash1"),
        newSnapshotInfo,
        Some(oldSnapshotInfo),
        hasher
      )
      sorted = result.sortBy(_.createdAtEpoch)

    } yield expect.same(
      sorted,
      Vector(
        DelegatedStakingWithdraw(
          "SnapshotHash1",
          sorted(0).hash,
          address1.value,
          sorted(0).hash,
          111L,
          10L,
          (addr1P1.createdAt |+| withdrawalTimeLimit).value.value,
          completed = true
        ),
        DelegatedStakingWithdraw(
          "SnapshotHash1",
          sorted(1).hash,
          address1.value,
          sorted(1).hash,
          555L,
          12L,
          (addr1P5.createdAt |+| withdrawalTimeLimit).value.value,
          completed = false
        ),
        DelegatedStakingWithdraw(
          "SnapshotHash1",
          sorted(2).hash,
          address2.value,
          sorted(2).hash,
          444L,
          24L,
          (addr2P4.createdAt |+| withdrawalTimeLimit).value.value,
          completed = false
        )
      )
    )
  }

}
