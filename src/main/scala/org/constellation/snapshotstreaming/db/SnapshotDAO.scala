package org.constellation.snapshotstreaming.db

import cats.Parallel
import cats.effect.{Async, Resource}
import cats.syntax.all._
import org.constellation.snapshotstreaming._
import org.constellation.snapshotstreaming.schema.extractors.{AddressExtractor, MetagraphExtractor}
import org.constellation.snapshotstreaming.schema.schema.{GlobalData, MetagraphData}
import org.constellation.snapshotstreaming.schema.{AddressBalance, Block, BlockReference, CurrencyData, CurrencySnapshot, FeeTransaction, RewardTransaction, Snapshot, Transaction => STransaction}
import org.tessellation.security.signature.signature.SignatureProof
import org.typelevel.log4cats.slf4j.Slf4jLogger
import skunk._
import skunk.circe.codec.all.jsonb
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
        transaction_original,
        created_at
      ) VALUES ($varchar, $varchar, $varchar, $int8, $int8, $int8, $int8, $varchar, $int8, $varchar, $varchar, $jsonb, $timestamp)
      ON CONFLICT (hash) DO UPDATE
      SET transaction_original = EXCLUDED.transaction_original;
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
        tx.transactionOriginal,
        tx.timestamp
      )
    }

  private def updateCompletedDelegatedStakingWithdrawCommand(n: Int): Command[List[String]] =
    sql"""
      UPDATE delegate_stake_withdraw_events
      SET is_completed = true
      WHERE hash IN (${varchar.list(n)})
    """.command

  private val insertDagRewardTxCommand: Command[(String, Int, RewardTransaction)] =
    sql"""
      INSERT INTO dag_reward_transactions (
        global_snapshot_hash,
        idx,
        destination_addr,
        amount
      ) VALUES ($varchar, $int4, $varchar, $int8)
      ON CONFLICT (global_snapshot_hash, destination_addr, idx) DO NOTHING;
    """.command.contramap { case (gsHash, index, reward) =>
      (gsHash, index, reward.destination, reward.amount)
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

  private def insertMetagraphTransactionsMany(size: Int): Command[List[CurrencyData[STransaction]]] = {
    val enc = (
      varchar *: varchar *: varchar *: varchar *: int8 *: int8 *: int8 *: int8 *: varchar *: int8 *: varchar *: varchar *: jsonb *: timestamp
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
        cdTx.data.snapshotHash,
        cdTx.data.blockHash,
        cdTx.data.transactionOriginal,
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
        snapshot_hash,
        block_hash,
        transaction_original,
        created_at
      ) VALUES $enc
      ON CONFLICT (metagraph_id, hash) DO UPDATE
      SET transaction_original = EXCLUDED.transaction_original;
    """.command
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

  private val insertMetagraphRewardTxCommand: Command[(String, Int, CurrencyData[RewardTransaction])] =
    sql"""
      INSERT INTO metagraph_reward_transactions (
        metagraph_id,
        metagraph_snapshot_hash,
        idx,
        destination_addr,
        amount
      ) VALUES ($varchar, $varchar, $int4, $varchar, $int8)
      ON CONFLICT (metagraph_id, metagraph_snapshot_hash, destination_addr, idx) DO NOTHING;
    """.command.contramap { case (mgHash, index, CurrencyData(id, reward)) =>
      (
        id,
        mgHash,
        index,
        reward.destination,
        reward.amount
      )
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

  private val insertMetagraphsCommand: Command[String] =
    sql"""
      INSERT INTO metagraphs (
        id
      ) VALUES ($varchar)
      ON CONFLICT (id)  DO NOTHING;
    """.command

  private def pairWith[V, T](elem: V, elements: Seq[T]) = elements.map((elem, _))

  def make[F[_]: Async: Parallel](pool: Resource[F, Session[F]]): SnapshotDAO[F] = new SnapshotDAO[F] {
    private implicit val logger = Slf4jLogger.getLoggerFromName[F]("SnapshotDAO")

    def insertGlobalData(snapshot: GlobalData, mgSnaphotsCount: Int): F[Unit] =
      retryF(pool.use { session =>
        session.transaction.use { xa =>
          val gsHash = snapshot.snapshot.hash
          val blockParents = snapshot.blocks.toList.flatMap(b => b.parent.map((b.hash, _)))
          for {
            preparedGlobalSnapshot <- session.prepare(insertGlobalSnapshotCommand)
            preparedDagBlock <- session.prepare(insertDagBlockCommand)
            preparedDagTxs <- session.prepare(insertDagTxCommand)
            preparedDagRewardTxs <- session.prepare(insertDagRewardTxCommand)
            preparedProofs <- session.prepare(insertProofCommand)
            preparedBlockParent <- session.prepare(insertBlockParentCommand)
            _ <- executeMany(session, AddressExtractor.extract(snapshot).toList, insertAddressMany).timedLog("[GLOBAL] insert addresses")
            _ <- executeCmd(preparedGlobalSnapshot)(Seq((snapshot.snapshot, mgSnaphotsCount))).timedLog("[GLOBAL] insert preparedGlobalSnapshot")
            _ <- executeCmd(preparedDagBlock)(snapshot.blocks.toList).timedLog("[GLOBAL] insert preparedDagBlock")
            _ <- executeCmd(preparedDagTxs)(snapshot.txs).timedLog("[GLOBAL] insert preparedDagTxs")
            _ <- executeCmd(preparedDagRewardTxs)(pairWith(gsHash, snapshot.snapshot.rewards.zipWithIndex).map {case (gsHash, (tx, idx)) => (gsHash, idx, tx)}).timedLog("[GLOBAL] insert preparedDagRewardTxs")
            _ <- executeCmd(preparedBlockParent)(blockParents).timedLog("[GLOBAL] insert preparedBlockParent")
            _ <- executeCmd(preparedProofs)(pairWith(gsHash, snapshot.proofs.toSeq)).timedLog("[GLOBAL] insert preparedProofs")
            _ <- xa.commit
          } yield ()
        }
      })

    def insertMetagraphData(globalSnapshotHash: String, mgSnapshot: MetagraphData): F[Unit] =
      retryF(pool.use { session =>
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
            preparedMgFeeTxs <- session.prepare(insertMetagraphFeeTransactionCommand)
            preparedMgRewardTxs <- session.prepare(insertMetagraphRewardTxCommand)
            _ <- executeMany(session, AddressExtractor.extract(mgSnapshot).toList, insertAddressMany).timedLog("[METAGRAPH] insert addresses")
            _ <- executeCmd(preparedMetagraphs)(MetagraphExtractor.extract(mgSnapshot).toSeq).timedLog("[METAGRAPH] insert metagraph ids")
            _ <- executeCmd(preparedMetagraphSnapshot)(pairWith(globalSnapshotHash, unifiedSnapshots)).timedLog("[METAGRAPH] insert snapshots")
            _ <- executeCmd(preparedMetagraphBlock)(mgSnapshot.blocks).timedLog("[METAGRAPH] insert blocks")
            _ <- executeMany(session, mgSnapshot.txs.toList, insertMetagraphTransactionsMany).timedLog(s"[METAGRAPH] insert ${mgSnapshot.txs.size} txs ")
            _ <- executeCmd(preparedMgFeeTxs)(mgSnapshot.feeTxs).timedLog(s"[METAGRAPH] insert fee txs")
            _ <- executeCmd(preparedMgRewardTxs)(
              unifiedSnapshots.flatMap(mgs =>
                mgs.data.rewards.zipWithIndex.map { case (tx, idx) => (mgs.data.hash, idx, CurrencyData(mgs.identifier, tx))}
              )
            ).timedLog("[METAGRAPH] insert preparedMgRewardTxs")
            _ <- executeCmd(preparedBlockParent)(blockParents.map { case (_, hash, parent) => (hash, parent) }).timedLog(s"[METAGRAPH] insert preparedBlockParent")
            _ <- xa.commit
          } yield ()
        }
      })

  }

}
