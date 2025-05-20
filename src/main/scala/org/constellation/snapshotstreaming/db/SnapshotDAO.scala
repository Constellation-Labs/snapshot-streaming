package org.constellation.snapshotstreaming.db

import cats.Parallel
import cats.effect.{Async, Resource}
import cats.syntax.all._
import org.constellation.snapshotstreaming.schema.AllowSpends.{AllowSpend, AllowSpendExpiration, SpendTransaction}
import org.constellation.snapshotstreaming.schema.extractors.{AddressExtractor, MetagraphExtractor}
import org.constellation.snapshotstreaming.schema.schema.{GlobalData, MetagraphData}
import org.constellation.snapshotstreaming.schema.{AddressBalance, Block, BlockReference, CurrencyData, CurrencySnapshot, DelegatedStakingCreate, DelegatedStakingReward, DelegatedStakingWithdraw, FeeTransaction, RewardTransaction, Snapshot, Transaction => STransaction}
import io.constellationnetwork.security.signature.signature.SignatureProof
import org.constellation.snapshotstreaming.schema.TokenLocks.{TokenLock, TokenUnlock}
import skunk._
import skunk.codec.all._
import skunk.implicits._

trait SnapshotDAO[F[_]] {
  def insertGlobalData(snapshot: GlobalData, mgSnaphotsCount: Int): F[Unit]
  def insertMetagraphData(globalSnapshotHash: String, mgSnapshot: MetagraphData): F[Unit]
}

object SnapshotDAO {

  private val insertGlobalSnapshotCommand: Command[(Snapshot, Long)] =
    sql"""
    INSERT INTO global_snapshots (
      ordinal, hash, height, subheight, last_snapshot_hash, metagraph_snapshot_count, epoch_progress, version, created_at
    ) VALUES ($int8, $varchar, $int8, $int8, $varchar, $int8, $int8, $varchar, $timestamp)
    ON CONFLICT (hash) DO NOTHING;
  """.command.contramap { case (s, snapshotCount) =>
      (
        s.ordinal,
        s.hash,
        s.height,
        s.subHeight,
        s.lastSnapshotHash,
        snapshotCount,
        s.epochProgress,
        s.version,
        s.timestamp
      )
    }

  private val insertDagBlockCommand: Command[Block] =
    sql"""
      INSERT INTO dag_blocks (
        hash,
        height,
        snapshot_hash,
        created_at
      ) VALUES ($varchar, $int8, $varchar, $timestamp)
      ON CONFLICT (hash) DO NOTHING;
    """.command.contramap(block => (block.hash, block.height, block.snapshotHash, block.timestamp))

  private val insertBlockParentCommand: Command[(String, BlockReference)] =
    sql"""
      INSERT INTO block_parents (
        hash,
        parent_proof_hash,
        parent_height
      ) VALUES ($varchar, $varchar, $int8)
      ON CONFLICT (hash, parent_proof_hash) DO NOTHING;
    """.command.contramap { case (hash, BlockReference(snapshotHash, height)) => (hash, snapshotHash, height) }

  private val insertDagTxCommand: Command[STransaction] =
    sql"""
      INSERT INTO dag_transactions (
        hash,
        source_addr,
        destination_addr,
        amount,
        fee,
        salt,
        parent_ordinal,
        parent_hash,
        ordinal,
        block_hash,
        snapshot_hash,
        created_at
      ) VALUES ($varchar, $varchar, $varchar, $int8, $int8, $int8, $int8, $varchar, $int8, $varchar, $varchar, $timestamp)
      ON CONFLICT (hash) DO NOTHING;
    """.command.contramap { tx: STransaction =>
      (
        tx.hash,
        tx.source,
        tx.destination,
        tx.amount,
        tx.fee,
        tx.salt,
        tx.parent.ordinal,
        tx.parent.hash,
        tx.ordinal,
        tx.blockHash,
        tx.snapshotHash,
        tx.timestamp
      )
    }

  private val insertDagAllowSpendCommand: Command[AllowSpend] =
    sql"""
      INSERT INTO dag_allow_spends (
        hash,
        source_addr,
        destination_addr,
        amount,
        fee,
        parent_ordinal,
        parent_hash,
        last_valid_epoch_progress,
        round_id,
        ordinal,
        snapshot_hash
      ) VALUES ($varchar, $varchar, $varchar, $int8, $int8, $int8, $varchar, $int8, $uuid, $int8, $varchar)
      ON CONFLICT (hash) DO NOTHING;
    """.command.contramap { tx: AllowSpend =>
      (
        tx.hash,
        tx.source,
        tx.destination,
        tx.amount,
        tx.fee,
        tx.parent.ordinal,
        tx.parent.hash,
        tx.lastValidEpochProgress,
        tx.roundId,
        tx.ordinal,
        tx.snapshotHash
      )
    }

  private val insertDagSpendTransactionCommand: Command[SpendTransaction] =
    sql"""
    INSERT INTO dag_spend_transactions (
      hash,
      source_addr,
      amount,
      destination_addr,
      allow_spend_ref,
      snapshot_hash
    ) VALUES ($varchar, $varchar, $int8, $varchar, ${varchar.opt}, $varchar)
    ON CONFLICT (hash) DO NOTHING;
  """.command.contramap { tx: SpendTransaction =>
      (
        tx.hash,
        tx.source,
        tx.amount,
        tx.destination,
        tx.allowSpendRef,
        tx.snapshotHash
      )
    }

  private val insertDagExpiredSpendTransactionCommand: Command[AllowSpendExpiration] =
    sql"""
    INSERT INTO dag_expired_spend_transactions (
      snapshot_hash,
      hash,
      source_addr,
      amount,
      allow_spend_ref
    )
    SELECT
      $varchar,
      $varchar,                       -- hash from AllowSpendExpiration
      das.source_addr,
      das.amount,
      $varchar                        -- allowSpendRef from AllowSpendExpiration
    FROM dag_allow_spends das
    WHERE das.hash = $varchar
    ON CONFLICT (hash) DO NOTHING;
  """.command.contramap { exp: AllowSpendExpiration =>
      (
        exp.snapshotHash,
        exp.hash,
        exp.allowSpendRef,
        exp.allowSpendRef // used again in WHERE clause

      )
    }

  private val insertDagTokenLockCommand: Command[TokenLock] =
    sql"""
      INSERT INTO dag_token_locks (
        snapshot_hash,
        hash,
        source_addr,
        amount,
        unlock_epoch,
        ordinal,
        round_id,
        parent_hash
      ) VALUES ($varchar, $varchar, $varchar, $int8, ${int8.opt}, $int8, $uuid, $varchar)
      ON CONFLICT (hash) DO NOTHING;
    """.command.contramap { tx: TokenLock =>
      (tx.snapshotHash, tx.hash, tx.source, tx.amount, tx.unlockEpoch, tx.ordinal, tx.roundId, tx.parentHash)
    }

  private val insertDagTokenUnlockCommand: Command[TokenUnlock] =
    sql"""
      INSERT INTO dag_token_unlocks (
        hash,
        lock_reference_hash,
        amount,
        source_addr,
        snapshot_hash
      ) VALUES ($varchar, $varchar, $int8, $varchar, $varchar)
      ON CONFLICT (hash) DO NOTHING;
    """.command.contramap { tx: TokenUnlock =>
      (tx.hash, tx.lockReference, tx.amount, tx.address, tx.snapshotHash)
    }

  private val insertDelegatedStakingCreateCommand: Command[DelegatedStakingCreate] =
    sql"""
      INSERT INTO delegate_stake_create_events (
        hash,
        ordinal,
        source_addr,
        node_id,
        amount,
        fee,
        lock_reference_hash,
        parent_hash,
        transfer_from_hash,
        global_snapshot_hash
      ) VALUES ($varchar, $int8, $varchar, $varchar, $int8, $int8, $varchar, $varchar, ${varchar.opt}, $varchar)
      ON CONFLICT (hash) DO NOTHING;
    """.command.contramap { tx: DelegatedStakingCreate =>
      (
        tx.hash,
        tx.createdAtOrdinal,
        tx.sourceAddress,
        tx.nodeId,
        tx.amount,
        tx.fee,
        tx.tokenLockHash,
        tx.parentHash,
        tx.transferFrom,
        tx.snapshotHash
      )
    }

  private val insertDelegatedStakingCreateWithdrawCommand: Command[DelegatedStakingWithdraw] =
    sql"""
      INSERT INTO delegate_stake_withdraw_events (
        hash,
        source_addr,
        stake_create_hash,
        global_snapshot_hash,
        created_at_epoch,
        unlock_epoch,
        is_completed
      ) VALUES ($varchar, $varchar, $varchar, $varchar, $int8, $int8, $bool)
      ON CONFLICT (hash) DO NOTHING;
    """.command.contramap { tx: DelegatedStakingWithdraw =>
      (tx.hash, tx.sourceAddress, tx.stakeCreateHash, tx.snapshotHash, tx.createdAtEpoch, tx.unlockEpoch, tx.completed)
    }

  private val insertDelegatedStakingRewardsCommand: Command[DelegatedStakingReward] =
    sql"""
      INSERT INTO delegate_stake_rewards (
        global_snapshot_hash,
        address,
        node_id,
        rewards,
        stake_create_hash
      ) VALUES ($varchar, $varchar, $varchar, $int8, $varchar)
      ON CONFLICT (global_snapshot_hash, address, node_id, rewards) DO NOTHING;
    """.command.contramap { tx =>
      (tx.snapshotHash, tx.address, tx.nodeId, tx.amount, tx.stakeHash)
    }

  private val insertDagRewardTxCommand: Command[(String, RewardTransaction)] =
    sql"""
      INSERT INTO dag_reward_transactions (
        global_snapshot_hash,
        destination_addr,
        amount
      ) VALUES ($varchar, $varchar, $int8)
      ON CONFLICT (global_snapshot_hash, destination_addr) DO NOTHING;
    """.command.contramap { case (gsHash, reward) =>
      (gsHash, reward.destination, reward.amount)
    }

  private val insertAddressBalanceCommand: Command[AddressBalance] =
    sql"""
      INSERT INTO dag_balance_changes (
        snapshot_ordinal,
        snapshot_hash,
        address,
        balance,
        created_at
      ) VALUES ($int8, $varchar, $varchar, $int8, $timestamp )
      ON CONFLICT (snapshot_ordinal, address) DO NOTHING;
    """.command.contramap { ab =>
      (ab.snapshotOrdinal, ab.snapshotHash, ab.address, ab.balance, ab.timestamp)
    }

  private val insertProofCommand: Command[(String, SignatureProof)] =
    sql"""
      INSERT INTO global_snapshot_proofs (
        id,
        signature,
        snapshot_hash
      ) VALUES ($varchar, $varchar, $varchar)
      ON CONFLICT (snapshot_hash, id) DO NOTHING;
    """.command.contramap { case (snapshotHash, SignatureProof(id, signature)) =>
      (id.hex.value, signature.value.value, snapshotHash)
    }

  private val insertMetagraphSnapshotCommand: Command[(String, CurrencyData[CurrencySnapshot])] =
    sql"""
    INSERT INTO metagraph_snapshots (
      metagraph_id,
      ordinal,
      global_snapshot_hash,
      hash,
      height,
      subheight,
      last_snapshot_hash,
      fee,
      owner_address,
      staking_address,
      epoch_progress,
      size,
      version,
      created_at
    ) VALUES (
      $varchar, $int8, $varchar, $varchar, $int8, $int8, $varchar, $int8, ${varchar.opt}, ${varchar.opt}, $int8, $int8, $varchar, $timestamp
    )
    ON CONFLICT (metagraph_id, hash) DO NOTHING;
  """.command.contramap { case (gsHash, CurrencyData(id, cs: CurrencySnapshot)) =>
      (
        id,
        cs.ordinal,
        gsHash,
        cs.hash,
        cs.height,
        cs.subHeight,
        cs.lastSnapshotHash,
        cs.fee,
        cs.ownerAddress,
        cs.stakingAddress,
        cs.epochProgress,
        cs.sizeInKB,
        cs.version,
        cs.timestamp
      )
    }

  private val insertMetagraphBlockCommand: Command[CurrencyData[Block]] =
    sql"""
      INSERT INTO metagraph_blocks (
        metagraph_id,
        hash,
        height,
        metagraph_snapshot_hash,
        created_at
      ) VALUES ($varchar, $varchar, $int8, $varchar, $timestamp)
      ON CONFLICT (metagraph_id, hash) DO NOTHING;
    """.command.contramap { case CurrencyData(identifier, b) =>
      (identifier, b.hash, b.height, b.snapshotHash, b.timestamp)
    }

  private val insertMetagraphTxCommand: Command[CurrencyData[STransaction]] =
    sql"""
      INSERT INTO metagraph_transactions (
        metagraph_id,
        hash,
        source_addr,
        destination_addr,
        amount,
        fee,
        salt,
        parent_ordinal,
        parent_hash,
        ordinal,
        block_hash,
        created_at
      ) VALUES ($varchar, $varchar, $varchar, $varchar, $int8, $int8, $int8, $int8, $varchar, $int8, $varchar, $timestamp)
      ON CONFLICT (metagraph_id, hash) DO NOTHING;
    """.command.contramap { case CurrencyData(id, tx) =>
      (
        id,
        tx.hash,
        tx.source,
        tx.destination,
        tx.amount,
        tx.fee,
        tx.salt,
        tx.parent.ordinal,
        tx.parent.hash,
        tx.ordinal,
        tx.blockHash,
        tx.timestamp
      )
    }

  private val insertMetagraphAllowSpendCommand: Command[CurrencyData[AllowSpend]] =
    sql"""
    INSERT INTO metagraph_allow_spends (
      metagraph_id,
      hash,
      source_addr,
      destination_addr,
      amount,
      fee,
      parent_ordinal,
      parent_hash,
      last_valid_epoch_progress,
      round_id,
      ordinal,
      snapshot_hash
    ) VALUES ($varchar, $varchar, $varchar, $varchar, $int8, $int8, $int8, $varchar, $int8, $uuid, $int8, $varchar)
    ON CONFLICT (hash) DO NOTHING;
  """.command.contramap { case CurrencyData(id, tx) =>
      (
        id,
        tx.hash,
        tx.source,
        tx.destination,
        tx.amount,
        tx.fee,
        tx.parent.ordinal,
        tx.parent.hash,
        tx.lastValidEpochProgress,
        tx.roundId,
        tx.ordinal,
        tx.snapshotHash
      )
    }

  private val insertMetagraphSpendTransactionCommand: Command[CurrencyData[SpendTransaction]] =
    sql"""
    INSERT INTO metagraph_spend_transactions (
      metagraph_id,
      hash,
      source_addr,
      amount,
      destination_addr,
      allow_spend_ref,
      snapshot_hash
    ) VALUES ($varchar, $varchar, $varchar, $int8, $varchar, ${varchar.opt}, $varchar)
    ON CONFLICT (hash) DO NOTHING;
  """.command.contramap { case CurrencyData(id, tx: SpendTransaction) =>
      (
        id,
        tx.hash,
        tx.source,
        tx.amount,
        tx.destination,
        tx.allowSpendRef,
        tx.snapshotHash
      )
    }

  private val insertMetagraphExpiredSpendTransactionCommand: Command[CurrencyData[AllowSpendExpiration]] =
    sql"""
    INSERT INTO metagraph_expired_spend_transactions (
      metagraph_id,
      snapshot_hash,
      hash,
      source_addr,
      amount,
      allow_spend_ref
    )
    SELECT
      $varchar,                      -- metagraphId
      $varchar,                      -- metagraph snapshot hash
      $varchar,                      -- hash from MetagraphAllowSpendExpiration
      mas.source_addr,
      mas.amount,
      $varchar                       -- allowSpendRef
    FROM metagraph_allow_spends mas
    WHERE mas.hash = $varchar
    ON CONFLICT (hash) DO NOTHING;
  """.command.contramap { case CurrencyData(id, exp: AllowSpendExpiration) =>
      (
        id,
        exp.snapshotHash,
        exp.hash,
        exp.allowSpendRef,
        exp.allowSpendRef
      )
    }

  private val insertMetagraphTokenLockCommand: Command[CurrencyData[TokenLock]] =
    sql"""
    INSERT INTO metagraph_token_locks (
      metagraph_id,
      hash,
      source_addr,
      amount,
      unlock_epoch,
      ordinal,
      round_id,
      parent_hash,
      snapshot_hash
    ) VALUES ($varchar, $varchar, $varchar, $int8, ${int8.opt}, $int8, $uuid, $varchar, $varchar)
    ON CONFLICT (metagraph_id, hash) DO NOTHING;
  """.command.contramap { case CurrencyData(id, tx: TokenLock) =>
      (
        id,
        tx.hash,
        tx.source,
        tx.amount,
        tx.unlockEpoch,
        tx.ordinal,
        tx.roundId,
        tx.parentHash,
        tx.snapshotHash
      )
    }

  private val insertMetagraphTokenUnlockCommand: Command[CurrencyData[TokenUnlock]] =
    sql"""
    INSERT INTO metagraph_token_unlocks (
      metagraph_id,
      hash,
      lock_reference_hash,
      amount,
      source_addr,
      snapshot_hash
    ) VALUES ($varchar, $varchar, $varchar, $int8, $varchar, $varchar)
    ON CONFLICT (metagraph_id, hash) DO NOTHING;
  """.command.contramap { case CurrencyData(id, tx: TokenUnlock) =>
      (
        id,
        tx.hash,
        tx.lockReference,
        tx.amount,
        tx.address,
        tx.snapshotHash
      )
    }

  private val insertMetagraphFeeTransactionCommand: Command[CurrencyData[FeeTransaction]] =
    sql"""
      INSERT INTO metagraph_fee_transactions (
        metagraph_id,
        hash,
        source_addr,
        destination_addr,
        amount,
        data_update_ref,
        metagraph_snapshot_hash,
        metagraph_snapshot_ordinal,
        created_at
      ) VALUES ($varchar, $varchar, $varchar, $varchar, $int8, $varchar, $varchar, $int8, $timestamp)
      ON CONFLICT (metagraph_id, hash) DO NOTHING;
    """.command.contramap { case CurrencyData(id, tx: FeeTransaction) =>
      (
        id,
        tx.hash,
        tx.source,
        tx.destination,
        tx.amount,
        tx.dataUpdateRef,
        tx.snapshotHash,
        tx.snapshotOrdinal,
        tx.timestamp
      )
    }

  private val insertMetagraphRewardTxCommand: Command[(String, CurrencyData[RewardTransaction])] =
    sql"""
      INSERT INTO metagraph_reward_transactions (
        metagraph_id,
        metagraph_snapshot_hash,
        destination_addr,
        amount
      ) VALUES ($varchar, $varchar, $varchar, $int8)
      ON CONFLICT (metagraph_id, metagraph_snapshot_hash, destination_addr) DO NOTHING;
    """.command.contramap { case (mgHash, CurrencyData(id, reward)) =>
      (
        id,
        mgHash,
        reward.destination,
        reward.amount
      )
    }

  private val insertMetagraphAddressBalanceCommand: Command[CurrencyData[AddressBalance]] =
    sql"""
      INSERT INTO metagraph_balance_changes (
        metagraph_id,
        metagraph_snapshot_hash,
        metagraph_snapshot_ordinal,
        address,
        balance,
        created_at
      ) VALUES ($varchar, $varchar, $int8, $varchar, $int8, $timestamp)
      ON CONFLICT (metagraph_id, address, metagraph_snapshot_ordinal) DO NOTHING;
    """.command.contramap { case CurrencyData(id, ab) =>
      (id, ab.snapshotHash, ab.snapshotOrdinal, ab.address, ab.balance, ab.timestamp)
    }

  private val insertAddressCommand: Command[String] =
    sql"""
      INSERT INTO addresses (
        address
      ) VALUES ($varchar)
      ON CONFLICT (address) DO UPDATE SET
        updated_at = now();
    """.command

  private val insertMetagraphsCommand: Command[String] =
    sql"""
      INSERT INTO metagraphs (
        id
      ) VALUES ($varchar)
      ON CONFLICT (id) DO UPDATE SET
        updated_at = now();
    """.command

  private def pairWith[V, T](elem: V, elements: Seq[T]) = elements.map((elem, _))

  def make[F[_]: Async: Parallel](pool: Resource[F, Session[F]]): SnapshotDAO[F] = new SnapshotDAO[F] {

    def insertGlobalData(snapshot: GlobalData, mgSnaphotsCount: Int): F[Unit] =
      pool.use { session =>
        session.transaction.use { xa =>
          val gsHash = snapshot.snapshot.hash
          val blockParents = snapshot.blocks.toList.flatMap(b => b.parent.map((b.hash, _)))
          for {
            preparedGlobalSnapshot <- session.prepare(insertGlobalSnapshotCommand)
            preparedDagBlock <- session.prepare(insertDagBlockCommand)
            preparedDagTxs <- session.prepare(insertDagTxCommand)
            preparedDagAllowSpend <- session.prepare(insertDagAllowSpendCommand)
            preparedDagSpendTxs <- session.prepare(insertDagSpendTransactionCommand)
            preparedDagExpiredSpends <- session.prepare(insertDagExpiredSpendTransactionCommand)
            preparedDagTokenLock <- session.prepare(insertDagTokenLockCommand)
            preparedDagTokenUnlock <- session.prepare(insertDagTokenUnlockCommand)
            preparedDagDelegatedStakingCreate <- session.prepare(insertDelegatedStakingCreateCommand)
            preparedDagDelegatedStakingWithdraw <- session.prepare(insertDelegatedStakingCreateWithdrawCommand)
            preparedDagDelegatedStakingRewards <- session.prepare(insertDelegatedStakingRewardsCommand)
            preparedDagRewardTxs <- session.prepare(insertDagRewardTxCommand)
            preparedDagAddressBalance <- session.prepare(insertAddressBalanceCommand)
            preparedProofs <- session.prepare(insertProofCommand)
            preparedBlockParent <- session.prepare(insertBlockParentCommand)
            preparedAddress <- session.prepare(insertAddressCommand)
            _ <- executeCmd(preparedAddress)(AddressExtractor.extract(snapshot).toSeq)
            _ <- executeCmd(preparedGlobalSnapshot)(Seq((snapshot.snapshot, mgSnaphotsCount)))
            _ <- executeCmd(preparedDagBlock)(snapshot.blocks.toList)
            _ <- executeCmd(preparedDagTxs)(snapshot.txs)
            _ <- executeCmd(preparedDagAllowSpend)(snapshot.allowSpends)
            _ <- executeCmd(preparedDagSpendTxs)(snapshot.spendTransactions)
            _ <- executeCmd(preparedDagExpiredSpends)(snapshot.allowSpendExpirations)
            _ <- executeCmd(preparedDagTokenLock)(snapshot.tokenLocks)
            _ <- executeCmd(preparedDagTokenUnlock)(snapshot.tokenUnlocks)
            _ <- executeCmd(preparedDagDelegatedStakingCreate)(snapshot.delegatedStakingCreate)
            _ <- executeCmd(preparedDagDelegatedStakingWithdraw)(snapshot.delegatedStakingWithdraw)
            _ <- executeCmd(preparedDagDelegatedStakingRewards)(snapshot.delegatedStakingRewards)
            _ <- executeCmd(preparedDagAddressBalance)(snapshot.balances)
            _ <- executeCmd(preparedDagRewardTxs)(pairWith(gsHash, snapshot.snapshot.rewards.toSeq))
            _ <- executeCmd(preparedBlockParent)(blockParents)
            _ <- executeCmd(preparedProofs)(pairWith(gsHash, snapshot.proofs.toSeq))
            _ <- xa.commit
          } yield ()
        }
      }

    def insertMetagraphData(globalSnapshotHash: String, mgSnapshot: MetagraphData): F[Unit] =
      pool.use { session =>
        session.transaction.use { xa =>
          val blockParents = mgSnapshot.blocks.flatMap { currencyData =>
            currencyData.data.parent.map(parent => (currencyData.identifier, currencyData.data.hash, parent))
          }
          val unifiedSnapshots = mgSnapshot.snapshots
          for {
            preparedMetagraphs <- session.prepare(insertMetagraphsCommand)
            preparedMetagraphSnapshot <- session.prepare(insertMetagraphSnapshotCommand)
            preparedBlockParent <- session.prepare(insertBlockParentCommand)
            preparedMetagraphBlock <- session.prepare(insertMetagraphBlockCommand)
            preparedMgTxs <- session.prepare(insertMetagraphTxCommand)
            preparedMgAllowSpends <- session.prepare(insertMetagraphAllowSpendCommand)
            preparedMgSpendsTxs <- session.prepare(insertMetagraphSpendTransactionCommand)
            preparedMgExpiredSpends <- session.prepare(insertMetagraphExpiredSpendTransactionCommand)
            preparedMgTokenLocks <- session.prepare(insertMetagraphTokenLockCommand)
            preparedMgTokenUnlocks <- session.prepare(insertMetagraphTokenUnlockCommand)
            preparedMgFeeTxs <- session.prepare(insertMetagraphFeeTransactionCommand)
            preparedMgRewardTxs <- session.prepare(insertMetagraphRewardTxCommand)
            preparedMgAddressBalance <- session.prepare(insertMetagraphAddressBalanceCommand)
            preparedAddress <- session.prepare(insertAddressCommand)
            _ <- executeCmd(preparedAddress)(AddressExtractor.extract(mgSnapshot).toSeq)
            _ <- executeCmd(preparedMetagraphs)(MetagraphExtractor.extract(mgSnapshot).toSeq)
            _ <- executeCmd(preparedMetagraphSnapshot)(pairWith(globalSnapshotHash, unifiedSnapshots))
            _ <- executeCmd(preparedMetagraphBlock)(mgSnapshot.blocks)
            _ <- executeCmd(preparedMgTxs)(mgSnapshot.txs)
            _ <- executeCmd(preparedMgAllowSpends)(mgSnapshot.allowSpends)
            _ <- executeCmd(preparedMgSpendsTxs)(mgSnapshot.spendTransactions)
            _ <- executeCmd(preparedMgExpiredSpends)(mgSnapshot.allowSpendExpirations)
            _ <- executeCmd(preparedMgTokenLocks)(mgSnapshot.tokenLocks)
            _ <- executeCmd(preparedMgTokenUnlocks)(mgSnapshot.tokenUnlocks)
            _ <- executeCmd(preparedMgFeeTxs)(mgSnapshot.feeTxs)
            _ <- executeCmd(preparedMgRewardTxs)(
              unifiedSnapshots.flatMap(mgs =>
                mgs.data.rewards.map(r => (mgs.data.hash, CurrencyData(mgs.identifier, r)))
              )
            )
            _ <- executeCmd(preparedBlockParent)(blockParents.map { case (_, hash, parent) => (hash, parent) })
            _ <- executeCmd(preparedMgAddressBalance)(mgSnapshot.balances)
            _ <- xa.commit
          } yield ()
        }
      }

  }

}
