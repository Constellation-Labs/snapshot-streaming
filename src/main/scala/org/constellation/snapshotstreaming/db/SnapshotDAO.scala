package org.constellation.snapshotstreaming.db

import cats.Parallel
import cats.effect.{Async, Resource}
import cats.syntax.all._
import org.constellation.snapshotstreaming.schema.AllowSpends.{AllowSpend, TokenLock, TokenUnlock}
import org.constellation.snapshotstreaming.schema.extractors.{AddressExtractor, MetagraphExtractor}
import org.constellation.snapshotstreaming.schema.schema.{GlobalData, MetagraphData, SignatureProof}
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
import org.typelevel.log4cats.slf4j.Slf4jLogger
import skunk._
import skunk.codec.all._
import skunk.implicits._

trait SnapshotDAO[F[_]] {
  def insertGlobalData(snapshots: List[GlobalData]): F[Unit]
  def insertMetagraphData(mgSnapshotz: List[MetagraphData]): F[Unit]
}

object SnapshotDAO {

  private def insertGlobalSnapshotsMany(size: Int): Command[List[Snapshot]] = {
    val enc = (
      int8 *: varchar *: int8 *: int8 *: varchar *: int8 *: int8 *: varchar *: timestamp
    ).values.contramap { s: Snapshot =>
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
    }.list(size)

    sql"""
      INSERT INTO global_snapshots (
        ordinal, hash, height, subheight, last_snapshot_hash, metagraph_snapshot_count, epoch_progress, version, created_at
      ) VALUES $enc
      ON CONFLICT (hash) DO NOTHING;
    """.command
  }

  private def insertDagBlocksMany(size: Int): Command[List[Block]] = {
    val enc = (
      varchar *: int8 *: varchar *: timestamp
    ).values.contramap { block: Block =>
      (
        block.hash,
        block.height,
        block.snapshotHash,
        block.timestamp
      )
    }.list(size)

    sql"""
      INSERT INTO dag_blocks (
        hash,
        height,
        snapshot_hash,
        created_at
      ) VALUES $enc
      ON CONFLICT (hash) DO NOTHING;
    """.command
  }

  private def insertBlockParentsMany(size: Int): Command[List[(String, BlockReference)]] = {
    val enc = (
      varchar *: varchar *: int8
    ).values.contramap { sbr: (String, BlockReference) =>
      val (hash, BlockReference(snapshotHash, height)) = sbr
      (
        hash,
        snapshotHash,
        height
      )
    }.list(size)

    sql"""
      INSERT INTO block_parents (
        hash,
        parent_proof_hash,
        parent_height
      ) VALUES $enc
      ON CONFLICT (hash, parent_proof_hash) DO NOTHING;
    """.command
  }

  private def insertDagTransactionsMany(size: Int): Command[List[STransaction]] = {
    val enc = (
      varchar *: varchar *: varchar *: int8 *: int8 *: int8 *: int8 *: varchar *: int8 *: varchar *: timestamp
    ).values.contramap { tx: STransaction =>
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
    }.list(size)

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
      ) VALUES $enc
      ON CONFLICT (hash) DO NOTHING;
    """.command
  }

  private def insertDagAllowSpendsMany(size: Int): Command[List[AllowSpend]] = {
    val enc = (
      varchar *: varchar *: varchar *: int8 *: int8 *: int8 *: varchar *: int8 *: uuid *: int8
    ).values.contramap { tx: AllowSpend =>
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
    }.list(size)

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
      ) VALUES $enc
      ON CONFLICT (hash) DO NOTHING;
    """.command
  }

  private def insertDagTokenLocksMany(size: Int): Command[List[TokenLock]] = {
    val enc = (
      varchar *: varchar *: int8 *: int8 *: int8 *: varchar
    ).values.contramap { tx: TokenLock =>
      (
        tx.hash,
        tx.source,
        tx.amount,
        tx.ordinal,
        tx.unlockEpoch,
        tx.snapshotHash
      )
    }.list(size)

    sql"""
      INSERT INTO dag_token_locks (
        hash,
        source_addr,
        amount,
        ordinal,
        unlock_epoch,
        global_snapshot_hash
      ) VALUES $enc
      ON CONFLICT (hash) DO NOTHING;
    """.command
  }

  private def insertDagTokenUnlocksMany(size: Int): Command[List[TokenUnlock]] = {
    val enc = (
      int8 *: varchar *: int8 *: varchar
    ).values.contramap { tx: TokenUnlock =>
      (
        tx.lockReference.ordinal,
        tx.lockReference.hash,
        tx.amount,
        tx.address
      )
    }.list(size)

    sql"""
      INSERT INTO dag_token_unlocks (
        lock_reference_ordinal,
        lock_reference_hash,
        amount,
        source_addr
      ) VALUES $enc
      ON CONFLICT (lock_reference_ordinal, lock_reference_hash) DO NOTHING;
    """.command
  }

  private def insertDagRewardTransactionsMany(size: Int): Command[List[RewardTransaction]] = {
    val enc = (
      varchar *: varchar *: int8
    ).values.contramap { reward: RewardTransaction =>
      (
        reward.snapshotHash,
        reward.destination,
        reward.amount
      )
    }.list(size)

    sql"""
      INSERT INTO dag_reward_transactions (
        global_snapshot_hash,
        destination_addr,
        amount
      ) VALUES $enc
      ON CONFLICT (global_snapshot_hash, destination_addr) DO NOTHING;
    """.command
  }

  private def insertAddressBalancesMany(size: Int): Command[List[AddressBalance]] = {
    val enc = (
      int8 *: varchar *: varchar *: int8 *: timestamp
    ).values.contramap { ab: AddressBalance =>
      (
        ab.snapshotOrdinal,
        ab.snapshotHash,
        ab.address,
        ab.balance,
        ab.timestamp
      )
    }.list(size)

    sql"""
      INSERT INTO dag_balance_changes (
        snapshot_ordinal,
        snapshot_hash,
        address,
        balance,
        created_at
      ) VALUES $enc
      ON CONFLICT (snapshot_ordinal, address) DO NOTHING;
    """.command
  }

  private def insertGlobalSnapshotProofsMany(size: Int): Command[List[SignatureProof]] = {
    val enc = (
      varchar *: varchar *: varchar
    ).values.contramap { (sp: SignatureProof) =>
      (
        sp.id,
        sp.signature,
        sp.snapshotHash
      )
    }.list(size)

    sql"""
      INSERT INTO global_snapshot_proofs (
        id,
        signature,
        snapshot_hash
      ) VALUES $enc
      ON CONFLICT (snapshot_hash, id) DO NOTHING;
    """.command
  }

  private def insertMetagraphSnapshotsMany(size: Int): Command[List[CurrencyData[CurrencySnapshot]]] = {
    val enc = (
      varchar *: varchar *: varchar *: int8 *: int8 *: int8 *: varchar *: int8.opt *: varchar.opt *: varchar.opt *: int8 *: varchar *: timestamp
    ).values.contramap { cd: CurrencyData[CurrencySnapshot] =>
      (
        cd.identifier,
        cd.data.globalSnapshotHash,
        cd.data.hash,
        cd.data.ordinal,
        cd.data.height,
        cd.data.subHeight,
        cd.data.lastSnapshotHash,
        cd.data.fee,
        cd.data.ownerAddress,
        cd.data.stakingAddress,
        cd.data.epochProgress,
        cd.data.version,
        cd.data.timestamp
      )
    }.list(size)

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
      ) VALUES $enc
      ON CONFLICT (metagraph_id, hash) DO NOTHING;
    """.command
  }

  private def insertMetagraphBlocksMany(size: Int): Command[List[CurrencyData[Block]]] = {
    val enc = (
      varchar *: varchar *: int8 *: varchar *: timestamp
    ).values.contramap { cd: CurrencyData[Block] =>
      (
        cd.identifier,
        cd.data.hash,
        cd.data.height,
        cd.data.snapshotHash,
        cd.data.timestamp
      )
    }.list(size)

    sql"""
      INSERT INTO metagraph_blocks (
        metagraph_id,
        hash,
        height,
        metagraph_snapshot_hash,
        created_at
      ) VALUES $enc
      ON CONFLICT (metagraph_id, hash) DO NOTHING;
    """.command
  }

  private def insertMetagraphTransactionsMany(size: Int): Command[List[CurrencyData[STransaction]]] = {
    val enc = (
      varchar *: varchar *: varchar *: varchar *: int8 *: int8 *: int8 *: int8 *: varchar *: int8 *: varchar *: timestamp
    ).values.contramap { cdTx: CurrencyData[STransaction] =>
      (
        cdTx.identifier,
        cdTx.data.hash,
        cdTx.data.source,
        cdTx.data.destination,
        cdTx.data.amount,
        cdTx.data.fee,
        cdTx.data.salt,
        cdTx.data.parent.ordinal,
        cdTx.data.parent.hash,
        cdTx.data.ordinal,
        cdTx.data.blockHash,
        cdTx.data.timestamp
      )
    }.list(size)

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
      ) VALUES $enc
      ON CONFLICT (metagraph_id, hash) DO NOTHING;
    """.command
  }

  private def insertMetagraphAllowSpendsMany(size: Int): Command[List[CurrencyData[AllowSpend]]] = {
    val enc = (
      varchar *: varchar *: varchar *: varchar *: int8 *: int8 *: int8 *: varchar *: int8 *: uuid *: int8
    ).values.contramap { cdAs: CurrencyData[AllowSpend] =>
      (
        cdAs.identifier,
        cdAs.data.hash,
        cdAs.data.source,
        cdAs.data.destination,
        cdAs.data.amount,
        cdAs.data.fee,
        cdAs.data.parent.ordinal,
        cdAs.data.parent.hash,
        cdAs.data.lastValidEpochProgress,
        cdAs.data.roundId,
        cdAs.data.ordinal
      )
    }.list(size)

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
      ) VALUES $enc
      ON CONFLICT (hash) DO NOTHING;
    """.command
  }

  private def insertMetagraphTokenLocksMany(size: Int): Command[List[CurrencyData[TokenLock]]] = {
    val enc = (
      varchar *: varchar *: varchar *: int8 *: int8 *: int8
    ).values.contramap { cdTl: CurrencyData[TokenLock] =>
      (
        cdTl.identifier,
        cdTl.data.hash,
        cdTl.data.source,
        cdTl.data.amount,
        cdTl.data.ordinal,
        cdTl.data.unlockEpoch
      )
    }.list(size)

    sql"""
      INSERT INTO metagraph_token_locks (
        metagraph_id,
        hash,
        source_addr,
        amount,
        ordinal,
        unlock_epoch
      ) VALUES $enc
      ON CONFLICT (metagraph_id, hash) DO NOTHING;
    """.command
  }

  private def insertMetagraphTokenUnlocksMany(size: Int): Command[List[CurrencyData[TokenUnlock]]] = {
    val enc = (
      varchar *: int8 *: varchar *: int8 *: varchar
    ).values.contramap { cdTu: CurrencyData[TokenUnlock] =>
      (
        cdTu.identifier,
        cdTu.data.lockReference.ordinal,
        cdTu.data.lockReference.hash,
        cdTu.data.amount,
        cdTu.data.address
      )
    }.list(size)

    sql"""
      INSERT INTO metagraph_token_unlocks (
        metagraph_id,
        lock_reference_ordinal,
        lock_reference_hash,
        amount,
        source_addr
      ) VALUES $enc
      ON CONFLICT (lock_reference_ordinal, lock_reference_hash) DO NOTHING;
    """.command
  }

  private def insertMetagraphFeeTransactionsMany(size: Int): Command[List[CurrencyData[FeeTransaction]]] = {
    val enc = (
      varchar *: varchar *: varchar *: varchar *: int8 *: varchar *: varchar *: int8 *: timestamp
    ).values.contramap { cdFtx: CurrencyData[FeeTransaction] =>
      (
        cdFtx.identifier,
        cdFtx.data.hash,
        cdFtx.data.source,
        cdFtx.data.destination,
        cdFtx.data.amount,
        cdFtx.data.dataUpdateRef,
        cdFtx.data.snapshotHash,
        cdFtx.data.snapshotOrdinal,
        cdFtx.data.timestamp
      )
    }.list(size)

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
      ) VALUES $enc
      ON CONFLICT (metagraph_id, hash) DO NOTHING;
    """.command
  }

  private def insertMetagraphRewardTransactionsMany(size: Int): Command[List[CurrencyData[RewardTransaction]]] = {
    val enc = (
      varchar *: varchar *: varchar *: int8
    ).values.contramap { cdRtx: CurrencyData[RewardTransaction] =>
      (
        cdRtx.identifier,
        cdRtx.data.snapshotHash,
        cdRtx.data.destination,
        cdRtx.data.amount
      )
    }.list(size)

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

  private def insertMetagraphAddressBalancesMany(size: Int): Command[List[CurrencyData[AddressBalance]]] = {
    val enc = (
      varchar *: varchar *: int8 *: varchar *: int8 *: timestamp
    ).values.contramap { cdAb: CurrencyData[AddressBalance] =>
      (
        cdAb.identifier,
        cdAb.data.snapshotHash,
        cdAb.data.snapshotOrdinal,
        cdAb.data.address,
        cdAb.data.balance,
        cdAb.data.timestamp
      )
    }.list(size)

    sql"""
      INSERT INTO metagraph_balance_changes (
        metagraph_id,
        metagraph_snapshot_hash,
        metagraph_snapshot_ordinal,
        address,
        balance,
        created_at
      ) VALUES $enc
      ON CONFLICT (metagraph_id, address, metagraph_snapshot_ordinal) DO NOTHING;
    """.command
  }

  private def insertAddressMany(size: Int): Command[List[String]] = {
    val enc = varchar.values.list(size)
    sql"""
      INSERT INTO addresses (
        address
      ) VALUES $enc
      ON CONFLICT (address) DO NOTHING;
    """.command
  }

  private def insertMetagraphsMany(size: Int): Command[List[String]] = {
    val enc = varchar.values.list(size)

    sql"""
      INSERT INTO metagraphs (
        id
      ) VALUES $enc
      ON CONFLICT (id) DO NOTHING;
    """.command
  }

  def make[F[_]: Async: Parallel](pool: Resource[F, Session[F]]): SnapshotDAO[F] = new SnapshotDAO[F] {

    private val logger = Slf4jLogger.getLogger[F]

    def insertGlobalData(globalSnapshots: List[GlobalData]): F[Unit] =
      pool.flatMap(session => session.transaction.map((_, session))).use { case (xa, session) =>
        val addresses = globalSnapshots.flatMap(AddressExtractor.extract(_))
        val snapshots = globalSnapshots.map(_.snapshot)
        val blocks = globalSnapshots.flatMap(_.blocks)
        val transactions = globalSnapshots.flatMap(_.txs)
        val allowSpends = globalSnapshots.flatMap(_.allowSpends)
        val tokenLocks = globalSnapshots.flatMap(_.tokenLocks)
        val tokenUnlocks = globalSnapshots.flatMap(_.tokenUnlocks)
        val balances = globalSnapshots.flatMap(_.balances)
        val rewards = globalSnapshots.flatMap(_.snapshot.rewards.toList)
        val blockParents = globalSnapshots.flatMap(_.blocks.flatMap { block =>
          block.parent.map(parent => (block.hash, parent))
        })
        val proofs = globalSnapshots.flatMap(_.proofs.toList)
        logger.debug("insert dag addresses") >>
          executeMany(session, addresses, insertAddressMany) >>
          logger.debug("insert gs") >>
          executeMany(session, snapshots, insertGlobalSnapshotsMany) >>
          logger.debug("insert dag blocks") >>
          executeMany(session, blocks, insertDagBlocksMany) >>
          logger.debug("insert dag parallels ") >>
          (
            executeMany(session, transactions, insertDagTransactionsMany),
            executeMany(session, allowSpends, insertDagAllowSpendsMany),
            executeMany(session, tokenLocks, insertDagTokenLocksMany),
            executeMany(session, tokenUnlocks, insertDagTokenUnlocksMany),
            executeMany(session, balances, insertAddressBalancesMany),
            executeMany(session, rewards, insertDagRewardTransactionsMany),
            executeMany(session, blockParents, insertBlockParentsMany),
            executeMany(session, proofs, insertGlobalSnapshotProofsMany)
          ).parTupled >>
          logger.debug("dag commit") >>
          xa.commit.void
      }

    def insertMetagraphData(metagraphSnapshots: List[MetagraphData]): F[Unit] =
      pool.flatMap(session => session.transaction.map((_, session))).use { case (xa, session) =>
        val addresses = metagraphSnapshots.flatMap(AddressExtractor.extract(_))
        val snapshots = metagraphSnapshots.flatMap(_.snapshots)
        val blocks = metagraphSnapshots.flatMap(_.blocks)
        val metagraphs = metagraphSnapshots.flatMap(MetagraphExtractor.extract(_).toList)
        val transactions = metagraphSnapshots.flatMap(_.txs)
        val allowSpends = metagraphSnapshots.flatMap(_.allowSpends)
        val tokenLocks = metagraphSnapshots.flatMap(_.tokenLocks)
        val tokenUnlocks = metagraphSnapshots.flatMap(_.tokenUnlocks)
        val feeTransactions = metagraphSnapshots.flatMap(_.feeTxs)
        val rewards = metagraphSnapshots
          .flatMap(_.snapshots.flatMap(mgs => mgs.data.rewards.map(r => CurrencyData(mgs.identifier, r))))
        val balances = metagraphSnapshots.flatMap(_.balances)
        val blockParents = metagraphSnapshots.flatMap(_.blocks.flatMap { currencyData =>
          currencyData.data.parent.map(parent => (currencyData.data.hash, parent))
        })
        logger.debug("insert mg addresses") >>
          executeMany(session, addresses, insertAddressMany) >>
          logger.debug("insert mg metagraphs") >>
          executeMany(session, metagraphs, insertMetagraphsMany) >>
          logger.debug("insert mg snapshots") >>
          executeMany(session, snapshots, insertMetagraphSnapshotsMany) >>
          logger.debug("insert mg blocks") >>
          executeMany(session, blocks, insertMetagraphBlocksMany) >>
          logger.debug("insert mg parallels") >>
          (
            executeMany(session, transactions, insertMetagraphTransactionsMany),
            executeMany(session, allowSpends, insertMetagraphAllowSpendsMany),
            executeMany(session, tokenLocks, insertMetagraphTokenLocksMany),
            executeMany(session, tokenUnlocks, insertMetagraphTokenUnlocksMany),
            executeMany(session, feeTransactions, insertMetagraphFeeTransactionsMany),
            executeMany(session, rewards, insertMetagraphRewardTransactionsMany),
            executeMany(session, balances, insertMetagraphAddressBalancesMany),
            executeMany(session, blockParents, insertBlockParentsMany)
          ).parTupled >>
          logger.debug("mg commit") >>
          xa.commit.void
      }

  }

}
