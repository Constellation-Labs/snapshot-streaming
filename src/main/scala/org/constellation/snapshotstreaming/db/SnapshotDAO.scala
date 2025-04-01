package org.constellation.snapshotstreaming.db

import cats.effect.{Async, Resource}
import cats.syntax.all._
import org.constellation.snapshotstreaming.schema.AllowSpends.{AllowSpend, AllowSpendExpiration, SpendTransaction}
import org.constellation.snapshotstreaming.schema.extractors.{AddressExtractor, MetagraphExtractor}
import org.constellation.snapshotstreaming.schema.schema.{GlobalData, MetagraphData}
import org.constellation.snapshotstreaming.schema.{AddressBalance, Block, BlockReference, CurrencyData, CurrencySnapshot, FeeTransaction, RewardTransaction, Snapshot, Transaction => STransaction}
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
      allow_spend_ref
    ) VALUES ($varchar, $varchar, $int8, $varchar, ${varchar.opt})
    ON CONFLICT (hash) DO NOTHING;
  """.command.contramap { tx: SpendTransaction =>
      (
        tx.hash,
        tx.source,
        tx.amount,
        tx.destination,
        tx.allowSpendRef
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
        global_snapshot_hash,
        hash,
        source_addr,
        amount,
        unlock_epoch,
        ordinal,
        round_id
      ) VALUES ($varchar, $varchar, $varchar, $int8, ${int8.opt}, $int8, $uuid)
      ON CONFLICT (hash) DO NOTHING;
    """.command.contramap { tx: TokenLock =>
      (tx.snapshotHash, tx.hash, tx.source, tx.amount, tx.unlockEpoch, tx.ordinal, tx.roundId)
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
      $varchar, $int8, $varchar, $varchar, $int8, $int8, $varchar, $int8, ${varchar.opt}, ${varchar.opt}, $int8, $varchar, $timestamp
    )
    ON CONFLICT (metagraph_id, hash) DO NOTHING;
  """.command.contramap { case (gsHash, CurrencyData(id, cs:CurrencySnapshot)) =>
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

  private val insertMetagraphSpendTransactionCommand: Command[CurrencyData[SpendTransaction]] =
    sql"""
    INSERT INTO metagraph_spend_transactions (
      metagraph_id,
      hash,
      source_addr,
      amount,
      destination_addr,
      allow_spend_ref
    ) VALUES ($varchar, $varchar, $varchar, $int8, $varchar, ${varchar.opt})
    ON CONFLICT (hash) DO NOTHING;
  """.command.contramap { case CurrencyData(id, tx: SpendTransaction) =>
      (
        id,
        tx.hash,
        tx.source,
        tx.amount,
        tx.destination,
        tx.allowSpendRef
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
      snapshot_hash
    ) VALUES ($varchar, $varchar, $varchar, $int8, ${int8.opt}, $int8, $uuid, $varchar)
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



  def make[F[_]: Async](pool: Resource[F, Session[F]]): SnapshotDAO[F] = new SnapshotDAO[F] {

    def insertGlobalData(snapshot: GlobalData, mgSnaphotsCount: Int): F[Unit] = (for {
      session <- pool
      preparedGlobalSnapshot <- session.prepareR(insertGlobalSnapshotCommand)
      preparedDagBlock <- session.prepareR(insertDagBlockCommand)
      preparedDagTxs <- session.prepareR(insertDagTxCommand)
      preparedDagAllowSpend <- session.prepareR(insertDagAllowSpendCommand)
      preparedDagSpendTxs <- session.prepareR(insertDagSpendTransactionCommand)
      preparedDagExpiredSpends <- session.prepareR(insertDagExpiredSpendTransactionCommand)
      preparedDagTokenLock <- session.prepareR(insertDagTokenLockCommand)
      preparedDagTokenUnlock <- session.prepareR(insertDagTokenUnlockCommand)
      preparedDagRewardTxs <- session.prepareR(insertDagRewardTxCommand)
      preparedDagAddressBalance <- session.prepareR(insertAddressBalanceCommand)
      preparedProofs <- session.prepareR(insertProofCommand)
      preparedBlockParent <- session.prepareR(insertBlockParentCommand)
      preparedAddress <- session.prepareR(insertAddressCommand)
      _ <- Resource.eval(executeCmd(preparedAddress)(AddressExtractor.extract(snapshot).toSeq))
      xa <- session.transaction
    } yield {
      val gsHash = snapshot.snapshot.hash
      val blockParents = snapshot.blocks.toList.flatMap(b => b.parent.map((b.hash, _)))
      executeCmd(preparedGlobalSnapshot)(Seq((snapshot.snapshot, mgSnaphotsCount))) >>
        executeCmd(preparedDagBlock)(snapshot.blocks.toList) >>
        executeCmd(preparedDagTxs)(snapshot.txs) >>
        executeCmd(preparedDagAllowSpend)(snapshot.allowSpends) >>
        executeCmd(preparedDagSpendTxs)(snapshot.spendTransactions) >>
        executeCmd(preparedDagExpiredSpends)(snapshot.allowSpendExpirations) >>
        executeCmd(preparedDagTokenLock)(snapshot.tokenLocks) >>
        executeCmd(preparedDagTokenUnlock)(snapshot.tokenUnlocks) >>
        executeCmd(preparedDagAddressBalance)(snapshot.balances) >>
        executeCmd(preparedDagRewardTxs)(pairWith(gsHash, snapshot.snapshot.rewards.toSeq)) >>
        executeCmd(preparedBlockParent)(blockParents) >>
        executeCmd(preparedProofs)(pairWith(gsHash, snapshot.proofs.toSeq)) >> xa.commit
    }).use(_.void)

    def insertMetagraphData(globalSnapshotHash: String, mgSnapshot: MetagraphData): F[Unit] =
      (for {
        session <- pool
        preparedMetagraphs <- session.prepareR(insertMetagraphsCommand)
        preparedMetagraphSnapshot <- session.prepareR(insertMetagraphSnapshotCommand)
        preparedBlockParent <- session.prepareR(insertBlockParentCommand)
        preparedMetagraphBlock <- session.prepareR(insertMetagraphBlockCommand)
        preparedMgTxs <- session.prepareR(insertMetagraphTxCommand)
        preparedMgAllowSpends <- session.prepareR(insertMetagraphAllowSpendCommand)
        preparedMgSpendsTxs <- session.prepareR(insertMetagraphSpendTransactionCommand)
        preparedMgExpiredSpends <- session.prepareR(insertMetagraphExpiredSpendTransactionCommand)
        preparedMgTokenLocks <- session.prepareR(insertMetagraphTokenLockCommand)
        preparedMgTokenUnlocks <- session.prepareR(insertMetagraphTokenUnlockCommand)
        preparedMgFeeTxs <- session.prepareR(insertMetagraphFeeTransactionCommand)
        preparedMgRewardTxs <- session.prepareR(insertMetagraphRewardTxCommand)
        preparedMgAddressBalance <- session.prepareR(insertMetagraphAddressBalanceCommand)
        preparedAddress <- session.prepareR(insertAddressCommand)
        _ <- Resource.eval(executeCmd(preparedAddress)(AddressExtractor.extract(mgSnapshot).toSeq))
        _ <- Resource.eval(executeCmd(preparedMetagraphs)(MetagraphExtractor.extract(mgSnapshot).toSeq))
        xa <- session.transaction
      } yield {
        val blockParents = mgSnapshot.blocks.flatMap { currencyData =>
          currencyData.data.parent.map(parent => (currencyData.identifier, currencyData.data.hash, parent))
        }
        val unifiedSnapshots = mgSnapshot.allAsIncremental
        executeCmd(preparedMetagraphSnapshot)(pairWith(globalSnapshotHash, unifiedSnapshots) )>>
          executeCmd(preparedMetagraphBlock)(mgSnapshot.blocks) >>
          executeCmd(preparedMgTxs)(mgSnapshot.txs) >>
          executeCmd(preparedMgAllowSpends)(mgSnapshot.allowSpends) >>
          executeCmd(preparedMgSpendsTxs)(mgSnapshot.spendTransactions) >>
          executeCmd(preparedMgExpiredSpends)(mgSnapshot.allowSpendExpirations) >>
          executeCmd(preparedMgTokenLocks)(mgSnapshot.tokenLocks) >>
          executeCmd(preparedMgTokenUnlocks)(mgSnapshot.tokenUnlocks) >>
          executeCmd(preparedMgFeeTxs)(mgSnapshot.feeTxs) >>
          executeCmd(preparedMgRewardTxs)(
            unifiedSnapshots.flatMap(mgs =>
              mgs.data.rewards.map(r => (mgs.data.hash, CurrencyData(mgs.identifier, r)))
            )
          ) >>
          executeCmd(preparedBlockParent)(blockParents.map { case (_, hash, parent) => (hash, parent) }) >>
          executeCmd(preparedMgAddressBalance)(mgSnapshot.balances) >> xa.commit
      }).use(_.void)

  }

}
