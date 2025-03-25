package org.constellation.snapshotstreaming.db

import cats.effect.{Async, Resource}
import cats.syntax.all._
import org.constellation.snapshotstreaming.schema.AllowSpends.{AllowSpend, TokenLock, TokenUnlock}
import org.constellation.snapshotstreaming.schema.extractors.{AddressExtractor, MetagraphExtractor}
import org.constellation.snapshotstreaming.schema.schema.{GlobalData, MetagraphData}
import org.constellation.snapshotstreaming.schema.{
  AddressBalance,
  Block,
  BlockReference,
  CurrencyData,
  CurrencySnapshot,
  FeeTransaction,
  RewardTransaction,
  Snapshot,
  Transaction => STransaction
}
import org.tessellation.security.signature.signature.SignatureProof
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
        created_at
      ) VALUES ($varchar, $varchar, $varchar, $int8, $int8, $int8, $int8, $varchar, $int8, $varchar, $timestamp)
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
        ordinal
      ) VALUES ($varchar, $varchar, $varchar, $int8, $int8, $int8, $varchar, $int8, $uuid, $int8)
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
        tx.ordinal
      )
    }

  private val insertDagTokenLockCommand: Command[TokenLock] =
    sql"""
      INSERT INTO dag_token_locks (
        hash,
        source_addr,
        amount,
        ordinal,
        unlock_epoch,
        global_snapshot_hash
      ) VALUES ($varchar, $varchar, $int8, $int8, $int8, $varchar)
      ON CONFLICT (hash) DO NOTHING;
    """.command.contramap { tx =>
      (tx.hash, tx.source, tx.amount, tx.ordinal, tx.unlockEpoch, tx.snapshotHash)
    }

  private val insertDagTokenUnlockCommand: Command[TokenUnlock] =
    sql"""
      INSERT INTO dag_token_unlocks (
        lock_reference_ordinal,
        lock_reference_hash,
        amount,
        source_addr
      ) VALUES ($int8, $varchar, $int8, $varchar)
      ON CONFLICT (lock_reference_ordinal, lock_reference_hash) DO NOTHING;
    """.command.contramap { tx =>
      (tx.lockReference.ordinal, tx.lockReference.hash, tx.amount, tx.address)
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
      version,
      created_at
    ) VALUES (
      $varchar, $int8, $varchar, $varchar, $int8, $int8, $varchar, ${int8.opt}, ${varchar.opt}, ${varchar.opt}, $int8, $varchar, $timestamp
    )
    ON CONFLICT (metagraph_id, hash) DO NOTHING;
  """.command.contramap { case (gsHash, CurrencyData(id, cs)) =>
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
      ordinal
    ) VALUES ($varchar, $varchar, $varchar, $varchar, $int8, $int8, $int8, $varchar, $int8, $uuid, $int8)
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
        tx.ordinal
      )
    }

  private val insertMetagraphTokenLockCommand: Command[CurrencyData[TokenLock]] =
    sql"""
    INSERT INTO metagraph_token_locks (
      metagraph_id,
      hash,
      source_addr,
      amount,
      ordinal,
      unlock_epoch
    ) VALUES ($varchar, $varchar, $varchar, $int8, $int8, $int8)
    ON CONFLICT (metagraph_id, hash) DO NOTHING;
  """.command.contramap { case CurrencyData(id, tx) =>
      (
        id,
        tx.hash,
        tx.source,
        tx.amount,
        tx.ordinal,
        tx.unlockEpoch
      )
    }

  private val insertMetagraphTokenUnlockCommand: Command[CurrencyData[TokenUnlock]] =
    sql"""
    INSERT INTO metagraph_token_unlocks (
      metagraph_id,
      lock_reference_ordinal,
      lock_reference_hash,
      amount,
      source_addr
    ) VALUES ($varchar, $int8, $varchar, $int8, $varchar)
    ON CONFLICT (lock_reference_ordinal, lock_reference_hash) DO NOTHING;
  """.command.contramap { case CurrencyData(id, tx) =>
      (
        id,
        tx.lockReference.ordinal,
        tx.lockReference.hash,
        tx.amount,
        tx.address
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

  def make[F[_]: Async](pool: Resource[F, Session[F]]): SnapshotDAO[F] = new SnapshotDAO[F] {

    def insertGlobalData(snapshot: GlobalData, mgSnaphotsCount: Int): F[Unit] = (for {
      session <- pool
    } yield {
      val gsHash = snapshot.snapshot.hash
      session.prepare(insertAddressCommand).flatMap(executeCmd(_)(AddressExtractor.extract(snapshot).toSeq)) >>
      session.prepare(insertGlobalSnapshotCommand).flatMap(executeCmd(_)(Seq((snapshot.snapshot, mgSnaphotsCount)))) >>
        session.prepare(insertDagBlockCommand).flatMap(executeCmd(_)(snapshot.blocks.toList)) >>
        session.prepare(insertDagTxCommand).flatMap(executeCmd(_)(snapshot.txs)) >>
        session.prepare(insertDagAllowSpendCommand).flatMap(executeCmd(_)(snapshot.allowSpends)) >>
        session.prepare(insertDagTokenLockCommand).flatMap(executeCmd(_)(snapshot.tokenLocks)) >>
        session.prepare(insertDagTokenUnlockCommand).flatMap(executeCmd(_)(snapshot.tokenUnlocks)) >>
        session.prepare(insertAddressBalanceCommand).flatMap(executeCmd(_)(snapshot.balances)) >>
        session.prepare(insertDagRewardTxCommand).flatMap(executeCmd(_)(pairWith(gsHash, snapshot.snapshot.rewards.toSeq))) >>
        session.prepare(insertProofCommand).flatMap(executeCmd(_)(pairWith(gsHash, snapshot.proofs.toSeq)))
    }).use(_.void)

    def insertMetagraphData(globalSnapshotHash: String, mgSnapshot: MetagraphData): F[Unit] =
      (for {
        session <- pool
      } yield {
        session.prepare(insertAddressCommand).flatMap(executeCmd(_)(AddressExtractor.extract(mgSnapshot).toSeq)) >>
        session.prepare(insertMetagraphsCommand).flatMap(executeCmd(_)(MetagraphExtractor.extract(mgSnapshot).toSeq)) >>
          session.prepare(insertMetagraphSnapshotCommand).flatMap(executeCmd(_)(pairWith(globalSnapshotHash, mgSnapshot.snapshots))) >>
          session.prepare(insertMetagraphBlockCommand).flatMap(executeCmd(_)(mgSnapshot.blocks)) >>
          session.prepare(insertMetagraphTxCommand).flatMap(executeCmd(_)(mgSnapshot.txs)) >>
          session.prepare(insertMetagraphAllowSpendCommand).flatMap(executeCmd(_)(mgSnapshot.allowSpends)) >>
          session.prepare(insertMetagraphTokenLockCommand).flatMap(executeCmd(_)(mgSnapshot.tokenLocks)) >>
          session.prepare(insertMetagraphTokenUnlockCommand).flatMap(executeCmd(_)(mgSnapshot.tokenUnlocks)) >>
          session.prepare(insertMetagraphFeeTransactionCommand).flatMap(executeCmd(_)(mgSnapshot.feeTxs)) >>
          session.prepare(insertMetagraphRewardTxCommand).flatMap(executeCmd(_)(
            mgSnapshot.snapshots.flatMap(mgs =>
              mgs.data.rewards.map(r => (mgs.data.hash, CurrencyData(mgs.identifier, r)))
            )
          )) >>
          session.prepare(insertMetagraphAddressBalanceCommand).flatMap(executeCmd(_)(mgSnapshot.balances))
      }).use(_.void)

  }

}
