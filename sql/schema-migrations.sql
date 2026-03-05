-- Schema migrations from block_explorer prisma/migrations
-- Applied in order to bring the initial schema up to date with snapshot-streaming code

-- === 20250429 ===
ALTER TABLE dag_token_unlocks DROP COLUMN IF EXISTS parent_hash;
ALTER TABLE dag_token_unlocks DROP COLUMN IF EXISTS lock_reference_ordinal;

-- === 20250502/01_migration.sql ===
-- remove intermediate block tables (ignore errors if already dropped)
ALTER TABLE metagraph_allow_spends DROP CONSTRAINT IF EXISTS allow_spends_block_fk;
DROP TABLE IF EXISTS dag_token_lock_blocks;
DROP TABLE IF EXISTS metagraph_token_lock_blocks;
DROP TABLE IF EXISTS dag_allow_spend_blocks;
DROP TABLE IF EXISTS metagraph_allow_spend_blocks;

ALTER TABLE dag_token_unlocks DROP COLUMN IF EXISTS lock_reference_ordinal;
ALTER TABLE dag_token_unlocks DROP COLUMN IF EXISTS parent_hash;

DO $$ BEGIN
  ALTER TABLE dag_token_locks ADD CONSTRAINT dag_token_locks_global_snapshots_fk FOREIGN KEY (snapshot_hash) REFERENCES global_snapshots(hash) ON DELETE CASCADE;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

ALTER TABLE dag_token_unlocks DROP CONSTRAINT IF EXISTS dag_token_unlocks_pk;
DO $$ BEGIN
  ALTER TABLE dag_token_unlocks ADD CONSTRAINT dag_token_unlocks_pk PRIMARY KEY (hash);
EXCEPTION WHEN others THEN NULL; END $$;
DO $$ BEGIN
  ALTER TABLE dag_token_unlocks ADD CONSTRAINT dag_token_unlocks_dag_token_locks_fk FOREIGN KEY (lock_reference_hash) REFERENCES dag_token_locks(hash);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

ALTER TABLE metagraph_token_unlocks DROP CONSTRAINT IF EXISTS metagraph_token_unlocks_pk;
DO $$ BEGIN
  ALTER TABLE metagraph_token_unlocks ADD CONSTRAINT metagraph_token_unlocks_pk PRIMARY KEY (hash, metagraph_id);
EXCEPTION WHEN others THEN NULL; END $$;

ALTER TABLE dag_allow_spends DROP CONSTRAINT IF EXISTS dag_allow_spends_ordinal;
ALTER TABLE metagraph_allow_spends DROP CONSTRAINT IF EXISTS metagraph_allow_spends_ordinal;

-- === 20250502/02_snapshot_hash.sql ===
DO $$ BEGIN
  ALTER TABLE abstract_transactions ADD COLUMN snapshot_hash varchar NULL;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;

-- Simplified trigger (no backfill needed on empty DB)
CREATE OR REPLACE FUNCTION public.insert_into_parent_abstract_transactions()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    INSERT INTO abstract_transactions (hash, source_addr, amount, created_at, snapshot_hash)
    VALUES (NEW.hash, NEW.source_addr, NEW.amount, NEW.created_at, NEW.snapshot_hash)
    ON CONFLICT (hash) DO NOTHING;
    RETURN NEW;
END;
$function$;

-- Add snapshot_hash to dag_transactions and metagraph_transactions
DO $$ BEGIN
  ALTER TABLE dag_transactions ADD COLUMN snapshot_hash varchar;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN
  ALTER TABLE dag_transactions ADD COLUMN transaction_original jsonb;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN
  ALTER TABLE metagraph_transactions ADD COLUMN snapshot_hash varchar;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN
  ALTER TABLE metagraph_transactions ADD COLUMN transaction_original jsonb;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;

-- Update view
CREATE OR REPLACE VIEW abstract_transactions_view AS
SELECT tx.hash, tx.source_addr, tx.amount, tx.created_at, tx.updated_at,
    p.relname AS table_name, tx.snapshot_hash
FROM abstract_transactions tx
JOIN pg_class p ON tx.tableoid = p.oid
WHERE p.relname <> 'abstract_transactions'::name;

-- === 20250506/01_update_staking.sql ===
DO $$ BEGIN
  ALTER TABLE delegate_stake_create_events DROP COLUMN is_update;
EXCEPTION WHEN undefined_column THEN NULL; END $$;
DO $$ BEGIN
  ALTER TABLE delegate_stake_create_events ADD COLUMN transfer_from_hash varchar NULL;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN
  ALTER TABLE delegate_stake_create_events ADD COLUMN current_token_lock_hash varchar NULL;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN
  ALTER TABLE delegate_stake_create_events ADD COLUMN current_amount int8 NULL;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN
  ALTER TABLE delegate_stake_withdraw_events ADD COLUMN unlock_epoch int8 NULL;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN
  ALTER TABLE delegate_stake_withdraw_events ADD COLUMN created_at_epoch int8 NULL;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN
  ALTER TABLE delegate_stake_withdraw_events ADD COLUMN is_completed boolean NULL;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN
  ALTER TABLE delegate_stake_rewards ADD COLUMN stake_create_hash varchar;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;

-- === 20250609/01_add_currency_id_to_spends.sql ===
DO $$ BEGIN ALTER TABLE dag_allow_spends ADD COLUMN currency_id varchar NULL; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE dag_spend_transactions ADD COLUMN currency_id varchar NULL; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE dag_expired_spend_transactions ADD COLUMN currency_id varchar NULL; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE metagraph_allow_spends ADD COLUMN currency_id varchar NULL; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE metagraph_spend_transactions ADD COLUMN currency_id varchar NULL; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE metagraph_expired_spend_transactions ADD COLUMN currency_id varchar NULL; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE dag_token_locks ADD COLUMN currency_id varchar NULL; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE dag_token_unlocks ADD COLUMN currency_id varchar NULL; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE metagraph_token_locks ADD COLUMN currency_id varchar NULL; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN ALTER TABLE metagraph_token_unlocks ADD COLUMN currency_id varchar NULL; EXCEPTION WHEN duplicate_column THEN NULL; END $$;

-- === 20250609/03_drop_abstract_triggers.sql ===
DROP TRIGGER IF EXISTS trigger_insert_abstract_transactions_dag_allow_spends ON dag_allow_spends;
DROP TRIGGER IF EXISTS trigger_insert_abstract_blocks_dag ON dag_blocks;
DROP TRIGGER IF EXISTS trigger_insert_abstract_transactions_dag_spend_transactions ON dag_spend_transactions;
DROP TRIGGER IF EXISTS trigger_insert_abstract_transactions_dag_expired_spend_transact ON dag_expired_spend_transactions;
DROP TRIGGER IF EXISTS trigger_insert_abstract_transactions_dag_token_locks ON dag_token_locks;
DROP TRIGGER IF EXISTS trigger_insert_abstract_transactions_dag_token_unlocks ON dag_token_unlocks;
DROP TRIGGER IF EXISTS trigger_insert_abstract_transactions_metagraph_token_locks ON metagraph_token_locks;
DROP TRIGGER IF EXISTS trigger_insert_abstract_transactions_metagraph_token_unlocks ON metagraph_token_unlocks;
DROP TRIGGER IF EXISTS trigger_insert_abstract_transactions_metagraph_allow_spends ON metagraph_allow_spends;
DROP TRIGGER IF EXISTS trigger_insert_abstract_blocks_metagraph ON metagraph_blocks;
DROP TRIGGER IF EXISTS trigger_insert_abstract_transactions_metagraph_fee_transactions ON metagraph_fee_transactions;
DROP TRIGGER IF EXISTS trigger_insert_abstract_transactions_metagraph_spend_transactio ON metagraph_spend_transactions;
DROP TRIGGER IF EXISTS trigger_insert_abstract_transactions_metagraph_expired_spend_tr ON metagraph_expired_spend_transactions;
DROP TRIGGER IF EXISTS trigger_insert_abstract_transactions_dag_transactions ON dag_transactions;
DROP TRIGGER IF EXISTS trigger_insert_abstract_transactions_metagraph_transactions ON metagraph_transactions;

-- === Additional columns needed by snapshot-streaming testing branch ===

-- dag_reward_transactions: add idx column and update primary key
DO $$ BEGIN
  ALTER TABLE dag_reward_transactions ADD COLUMN idx int4 NOT NULL DEFAULT 0;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN
  ALTER TABLE dag_reward_transactions DROP CONSTRAINT dag_reward_transaction_pk;
  ALTER TABLE dag_reward_transactions ADD CONSTRAINT dag_reward_transaction_pk PRIMARY KEY (global_snapshot_hash, destination_addr, idx);
EXCEPTION WHEN others THEN NULL; END $$;

-- metagraph_reward_transactions: add idx column and update primary key
DO $$ BEGIN
  ALTER TABLE metagraph_reward_transactions ADD COLUMN idx int4 NOT NULL DEFAULT 0;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;
DO $$ BEGIN
  ALTER TABLE metagraph_reward_transactions DROP CONSTRAINT metagraph_reward_transaction_pk;
  ALTER TABLE metagraph_reward_transactions ADD CONSTRAINT metagraph_reward_transaction_pk PRIMARY KEY (metagraph_id, metagraph_snapshot_hash, destination_addr, idx);
EXCEPTION WHEN others THEN NULL; END $$;

-- dag_token_locks: add replacement_hash (needed by testing branch)
DO $$ BEGIN
  ALTER TABLE dag_token_locks ADD COLUMN replacement_hash varchar NULL;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;
