package org.constellation.snapshotstreaming.db

import cats.effect.{Async, Resource}
import cats.syntax.all._
import org.constellation.snapshotstreaming.schema.AllowSpends.{AllowSpend, TokenLock, TokenUnlock}
import org.constellation.snapshotstreaming.schema.extractors.{AddressExtractor, MetagraphExtractor}
import org.constellation.snapshotstreaming.schema.schema.{GlobalData, MetagraphData, SignatureProof}
import org.constellation.snapshotstreaming.schema.{AddressBalance, Block, BlockReference, CurrencyData, CurrencySnapshot, FeeTransaction, RewardTransaction, Snapshot, Transaction => STransaction}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import skunk.{*:, _}
import skunk.codec.all._
import skunk.implicits._

trait SnapshotDAO[F[_]] {
  def insertGlobalData(snapshots: Seq[GlobalData]): F[Unit]
  def insertMetagraphData(mgSnapshotz: Seq[MetagraphData]): F[Unit]
}

object SnapshotDAO {

  private val insertGlobalSnapshotCommand: Command[Snapshot] =
    sql"""
    INSERT INTO global_snapshots (
      ordinal, hash, height, subheight, last_snapshot_hash, metagraph_snapshot_count, epoch_progress, version, created_at
    ) VALUES ($int8, $varchar, $int8, $int8, $varchar, $int8, $int8, $varchar, $timestamp)
    ON CONFLICT (hash) DO NOTHING;
  """.command.contramap { s =>
      (
        s.ordinal,
        s.hash,
        s.height,
        s.subHeight,
        s.lastSnapshotHash,
        s.metagraphSnapshotsCount,
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

  private val insertDagRewardTxCommand: Command[RewardTransaction] =
    sql"""
      INSERT INTO dag_reward_transactions (
        global_snapshot_hash,
        destination_addr,
        amount
      ) VALUES ($varchar, $varchar, $int8)
      ON CONFLICT (global_snapshot_hash, destination_addr) DO NOTHING;
    """.command.contramap { reward =>
      (reward.snapshotHash, reward.destination, reward.amount)
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

  private val insertProofCommand: Command[SignatureProof] =
    sql"""
      INSERT INTO global_snapshot_proofs (
        id,
        signature,
        snapshot_hash
      ) VALUES ($varchar, $varchar, $varchar)
      ON CONFLICT (snapshot_hash, id) DO NOTHING;
    """.command.contramap { case SignatureProof(snapshotHash, id, signature) =>
      (id, signature, snapshotHash)
    }

  private val insertMetagraphSnapshotCommand: Command[CurrencyData[CurrencySnapshot]] =
    sql"""
    INSERT INTO metagraph_snapshots (
      metagraph_id,
      global_snapshot_hash,
      hash,
      ordinal,
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
      $varchar, $varchar, $varchar, $int8, $int8, $int8, $varchar, ${int8.opt}, ${varchar.opt}, ${varchar.opt}, $int8, $varchar, $timestamp
    )
    ON CONFLICT (metagraph_id, hash) DO NOTHING;
  """.command.contramap { case CurrencyData(id, cs) =>
      (
        id,
        cs.globalSnapshotHash,
        cs.hash,
        cs.ordinal,
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

  def insertMany(n: Int): Command[List[(String, Short)]] = {
    val enc = (varchar ~ int2).values.list(n)
    sql"INSERT INTO pets VALUES $enc".command
  }

  private def insertMetagraphRewardTxMany( txs: Seq[CurrencyData[RewardTransaction]]) = {

    val enc = (varchar *: varchar *: varchar *: int8).values.contramap({ t: CurrencyData[RewardTransaction] =>
      (
        t.identifier ,t.data.snapshotHash, t.data.destination, t.data.amount
      )}).list(txs.toList)

    sql"""
      INSERT INTO metagraph_reward_transactions (
        metagraph_id,
        metagraph_snapshot_hash,
        destination_addr,
        amount
      ) VALUES $enc
      ON CONFLICT (metagraph_id, metagraph_snapshot_hash, destination_addr) DO NOTHING;
    """.command
  }

  private val insertMetagraphRewardTxCommand: Command[CurrencyData[RewardTransaction]] =
    sql"""
      INSERT INTO metagraph_reward_transactions (
        metagraph_id,
        metagraph_snapshot_hash,
        destination_addr,
        amount
      ) VALUES ($varchar, $varchar, $varchar, $int8)
      ON CONFLICT (metagraph_id, metagraph_snapshot_hash, destination_addr) DO NOTHING;
    """.command.contramap { case CurrencyData(id, reward) =>
      (
        id,
        reward.snapshotHash,
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
      ON CONFLICT (address) DO NOTHING;
    """.command

  private def insertAddressMany(addrs: List[String]): Command[addrs.type] = {
    val enc = varchar.values.list(addrs)
    sql"""
      INSERT INTO addresses (
        address
      ) VALUES $enc
      ON CONFLICT (address) DO NOTHING;
    """.command
  }



  private val insertMetagraphsCommand: Command[String] =
    sql"""
      INSERT INTO metagraphs (
        id
      ) VALUES ($varchar)
      ON CONFLICT (id) DO UPDATE SET
        updated_at = now();
    """.command

  def make[F[_]: Async](pool: Resource[F, Session[F]]): SnapshotDAO[F] = new SnapshotDAO[F] {

    private val logger = Slf4jLogger.getLogger[F]

    def insertGlobalData(globalSnapshots: Seq[GlobalData]): F[Unit] =
      pool.flatMap( session => session.transaction.map( (_, session))).use { case (xa, session) =>
        val addresses = globalSnapshots.flatMap(AddressExtractor.extract(_)).toList
        logger.debug(s"insert gs addresses ${globalSnapshots.flatMap(AddressExtractor.extract(_).toSeq)}") >>
          executeMany(session, addresses)(insertAddressMany(addresses)) >>
          logger.debug(s"insert gs1 snapshots ${globalSnapshots.map(_.snapshot)}") >>
          session.prepare(insertGlobalSnapshotCommand).flatMap(executeCmd(_)(globalSnapshots.map(_.snapshot))) >>
          logger.debug("insert gs2") >>
          session.prepare(insertDagBlockCommand).flatMap(executeCmd(_)(globalSnapshots.flatMap(_.blocks.toList))) >>
          logger.debug("insert gs3") >>
          session.prepare(insertDagTxCommand).flatMap(executeCmd(_)(globalSnapshots.flatMap(_.txs))) >>
          logger.debug("insert gs4") >>
          session.prepare(insertDagAllowSpendCommand).flatMap(executeCmd(_)(globalSnapshots.flatMap(_.allowSpends))) >>
          logger.debug("insert gs 2") >>
        session.prepare(insertDagTokenLockCommand).flatMap(executeCmd(_)(globalSnapshots.flatMap(_.tokenLocks))) >>
          logger.debug("insert gs 21") >>
          session.prepare(insertDagTokenUnlockCommand).flatMap(executeCmd(_)(globalSnapshots.flatMap(_.tokenUnlocks))) >>
          logger.debug("insert gs 211") >>
          session.prepare(insertAddressBalanceCommand).flatMap(executeCmd(_)(globalSnapshots.flatMap(_.balances))) >>
          logger.debug("insert gs 2111") >>
          session.prepare(insertDagRewardTxCommand).flatMap(executeCmd(_)(globalSnapshots.flatMap(_.snapshot.rewards.toSeq))) >>
          logger.debug("insert gs 21111") >>
          session.prepare(insertProofCommand).flatMap(executeCmd(_)(globalSnapshots.flatMap(_.proofs.toSeq))) >>
          logger.debug("insert gs finish") >>
      xa.commit.void
    }

    def insertMetagraphData(metagraphSnapshotss: Seq[MetagraphData]): F[Unit] =
      pool.flatMap( session => session.transaction.map( (_, session))).use { case (xa, session) =>
        val addresses = metagraphSnapshotss.flatMap(AddressExtractor.extract(_)).toList
        logger.debug("insert mg") >>
        //session.prepare(insertAddressCommand).flatMap(executeCmd(_)(metagraphSnapshotss.flatMap(AddressExtractor.extract(_).toSeq))) >>
          executeMany(session, addresses)(insertAddressMany(addresses)) >>
          logger.debug("insert mg 1") >>
          session.prepare(insertMetagraphSnapshotCommand).flatMap(executeCmd(_)(metagraphSnapshotss.flatMap(_.snapshots))) >>
          logger.debug("insert mg 11") >>
          session.prepare(insertMetagraphBlockCommand).flatMap(executeCmd(_)(metagraphSnapshotss.flatMap(_.blocks))) >>
          logger.debug("insert mg 111") >>
          session.prepare(insertMetagraphsCommand).flatMap(executeCmd(_)(metagraphSnapshotss.flatMap(MetagraphExtractor.extract(_).toSeq))) >>
          logger.debug("insert mg 1111") >>
          session.prepare(insertMetagraphTxCommand).flatMap(executeCmd(_)(metagraphSnapshotss.flatMap(_.txs))) >>
          logger.debug("insert mg 11111") >>
          session.prepare(insertMetagraphAllowSpendCommand).flatMap(executeCmd(_)(metagraphSnapshotss.flatMap(_.allowSpends))) >>
          logger.debug("insert mg 2") >>
          session.prepare(insertMetagraphTokenLockCommand).flatMap(executeCmd(_)(metagraphSnapshotss.flatMap(_.tokenLocks))) >>
          logger.debug("insert mg 22") >>
          session.prepare(insertMetagraphTokenUnlockCommand).flatMap(executeCmd(_)(metagraphSnapshotss.flatMap(_.tokenUnlocks))) >>
          logger.debug("insert mg 222") >>
          session.prepare(insertMetagraphFeeTransactionCommand).flatMap(executeCmd(_)(metagraphSnapshotss.flatMap(_.feeTxs))) >>
          logger.debug("insert mg 2222") >>
          session.prepare(insertMetagraphRewardTxCommand).flatMap(executeCmd(_)(
            metagraphSnapshotss.flatMap(_.snapshots.flatMap(mgs =>
              mgs.data.rewards.map(r => CurrencyData(mgs.identifier, r))
            ))
          )) >>
          session.prepare(insertMetagraphAddressBalanceCommand).flatMap(executeCmd(_)(metagraphSnapshotss.flatMap(_.balances))) >>
          logger.debug("insert mg finish") >>
          xa.commit.void
      }

  }

}
