-- DROP SCHEMA public;

CREATE SCHEMA public AUTHORIZATION pg_database_owner;
-- public.abstract_blocks definition

-- Drop table

-- DROP TABLE abstract_blocks;

CREATE TABLE abstract_blocks ( hash varchar NOT NULL, height int8 NOT NULL, created_at timestamp DEFAULT now() NOT NULL, updated_at timestamp DEFAULT now() NOT NULL, CONSTRAINT block_pkey PRIMARY KEY (hash));


-- public.abstract_transactions definition

-- Drop table

-- DROP TABLE abstract_transactions;

CREATE TABLE abstract_transactions ( hash varchar NOT NULL, source_addr varchar NOT NULL, amount int8 NOT NULL, created_at timestamp DEFAULT now() NOT NULL, updated_at timestamp DEFAULT now() NOT NULL, snapshot_hash varchar NULL, CONSTRAINT hash_pkey PRIMARY KEY (hash));
CREATE INDEX idx_abstract_transactions_snapshot_hash ON public.abstract_transactions USING btree (snapshot_hash);
CREATE INDEX idx_abstract_transactions_source_addr ON public.abstract_transactions USING btree (source_addr);


-- public.addresses definition

-- Drop table

-- DROP TABLE addresses;

CREATE TABLE addresses ( address varchar NOT NULL, created_at timestamp DEFAULT now() NOT NULL, updated_at timestamp DEFAULT now() NOT NULL, CONSTRAINT address_pkey PRIMARY KEY (address));

-- Table Triggers

create trigger set_updated_at_addresses before
update
    on
    public.addresses for each row execute function update_updated_at_column();


-- public.block_parents definition

-- Drop table

-- DROP TABLE block_parents;

CREATE TABLE block_parents ( hash varchar NOT NULL, parent_proof_hash varchar NOT NULL, parent_height int8 NOT NULL, CONSTRAINT block_parents_pkey PRIMARY KEY (hash, parent_proof_hash));
CREATE INDEX block_parents_hash_idx ON public.block_parents USING btree (hash);
CREATE INDEX idx_block_parents_hash ON public.block_parents USING btree (hash);


-- public.dag_original_transactions definition

-- Drop table

-- DROP TABLE dag_original_transactions;

CREATE TABLE dag_original_transactions ( transaction_hash varchar NOT NULL, original_transaction jsonb NOT NULL, CONSTRAINT dag_original_transactions_pk PRIMARY KEY (transaction_hash));


-- public.dag_transaction_hash_migration definition

-- Drop table

-- DROP TABLE dag_transaction_hash_migration;

CREATE TABLE dag_transaction_hash_migration ( hash varchar NOT NULL, parent_hash varchar NOT NULL, block_hash varchar NOT NULL, "source" varchar NULL, CONSTRAINT dag_transaction_hash_migration_pk PRIMARY KEY (hash));
CREATE INDEX dag_transaction_hash_migration_parent_hash_idx ON public.dag_transaction_hash_migration USING btree (parent_hash, source);


-- public.global_snapshots definition

-- Drop table

-- DROP TABLE global_snapshots;

CREATE TABLE global_snapshots ( hash varchar NOT NULL, ordinal int8 NOT NULL, height int8 NOT NULL, subheight int4 NOT NULL, last_snapshot_hash varchar NOT NULL, metagraph_snapshot_count int8 NULL, epoch_progress int8 NULL, "version" varchar NOT NULL, created_at timestamp DEFAULT now() NOT NULL, updated_at timestamp DEFAULT now() NOT NULL, CONSTRAINT global_snapshot_pk PRIMARY KEY (hash), CONSTRAINT global_snapshot_unique UNIQUE (ordinal));
CREATE INDEX idx_global_snapshots_created_ordinal ON public.global_snapshots USING btree (created_at DESC, ordinal DESC);

-- Table Triggers

create trigger set_updated_at_global_snapshot before
update
    on
    public.global_snapshots for each row execute function update_updated_at_column();


-- public.metagraph_original_transactions definition

-- Drop table

-- DROP TABLE metagraph_original_transactions;

CREATE TABLE metagraph_original_transactions ( metagraph_id varchar NOT NULL, transaction_hash varchar NOT NULL, original_transaction jsonb NOT NULL, CONSTRAINT metagraph_original_transactions_pk PRIMARY KEY (transaction_hash));


-- public.metagraph_transaction_hash_migration definition

-- Drop table

-- DROP TABLE metagraph_transaction_hash_migration;

CREATE TABLE metagraph_transaction_hash_migration ( metagraph_id varchar NOT NULL, hash varchar NOT NULL, parent_hash varchar NOT NULL, block_hash varchar NOT NULL, "source" varchar NULL, CONSTRAINT metagraph_transaction_hash_migration_pk PRIMARY KEY (hash));


-- public.metagraphs definition

-- Drop table

-- DROP TABLE metagraphs;

CREATE TABLE metagraphs ( id varchar NOT NULL, created_at timestamp DEFAULT now() NOT NULL, updated_at timestamp DEFAULT now() NOT NULL, CONSTRAINT metagraph_pkey PRIMARY KEY (id));

-- Table Triggers

create trigger set_updated_at_metagraphs before
update
    on
    public.metagraphs for each row execute function update_updated_at_column();


-- public.dag_allow_spends definition

-- Drop table

-- DROP TABLE dag_allow_spends;

CREATE TABLE dag_allow_spends ( destination_addr varchar NOT NULL, fee int8 NOT NULL, parent_ordinal int8 NULL, parent_hash varchar NULL, last_valid_epoch_progress int8 NOT NULL, round_id uuid NOT NULL, ordinal int8 NOT NULL, snapshot_hash varchar NOT NULL, currency_id varchar NULL, CONSTRAINT dag_allow_spends_pk PRIMARY KEY (hash), CONSTRAINT dag_allow_spends_destination_addr_fk FOREIGN KEY (destination_addr) REFERENCES addresses(address) ON DELETE CASCADE, CONSTRAINT dag_allow_spends_global_snapshots_fk FOREIGN KEY (snapshot_hash) REFERENCES global_snapshots(hash) ON DELETE CASCADE ON UPDATE CASCADE, CONSTRAINT dag_allow_spends_metagraphs_fk FOREIGN KEY (currency_id) REFERENCES metagraphs(id) ON DELETE CASCADE, CONSTRAINT dag_allow_spends_source_addr_fk FOREIGN KEY (source_addr) REFERENCES addresses(address) ON DELETE CASCADE) INHERITS (public.abstract_transactions);
CREATE INDEX dag_allow_spends_destination_addr_idx ON public.dag_allow_spends USING btree (destination_addr);
CREATE INDEX dag_allow_spends_round_id_idx ON public.dag_allow_spends USING btree (round_id);
CREATE INDEX dag_allow_spends_source_addr_idx ON public.dag_allow_spends USING btree (source_addr);
CREATE INDEX idx_dag_allow_spends_currency_id ON public.dag_allow_spends USING btree (currency_id);
CREATE INDEX idx_dag_allow_spends_destination_addr ON public.dag_allow_spends USING btree (destination_addr);
CREATE INDEX idx_dag_allow_spends_snapshot_hash ON public.dag_allow_spends USING btree (snapshot_hash);
CREATE INDEX idx_dag_allow_spends_source_addr ON public.dag_allow_spends USING btree (source_addr);

-- Table Triggers

create trigger set_updated_at_dag_allow_spends before
update
    on
    public.dag_allow_spends for each row execute function update_updated_at_column();
create trigger trigger_insert_abstract_transactions_dag_allow_spends after
insert
    on
    public.dag_allow_spends for each row execute function insert_into_parent_abstract_transactions();


-- public.dag_balance_changes definition

-- Drop table

-- DROP TABLE dag_balance_changes;

CREATE TABLE dag_balance_changes ( snapshot_hash varchar NOT NULL, address varchar NOT NULL, balance int8 NOT NULL, created_at timestamp DEFAULT now() NOT NULL, updated_at timestamp DEFAULT now() NOT NULL, snapshot_ordinal int8 NOT NULL, CONSTRAINT dag_balance_change_pk PRIMARY KEY (snapshot_ordinal, address), CONSTRAINT dag_balance_change_address_fk FOREIGN KEY (address) REFERENCES addresses(address) ON DELETE CASCADE, CONSTRAINT dag_balance_change_global_snapshot_fk FOREIGN KEY (snapshot_hash) REFERENCES global_snapshots(hash) ON DELETE CASCADE);
CREATE INDEX dag_balance_changes_address_idx ON public.dag_balance_changes USING btree (address, created_at);
CREATE INDEX dag_balance_changes_snapshot_hash_idx ON public.dag_balance_changes USING btree (snapshot_hash);
CREATE INDEX idx_dag_balance_changes_address ON public.dag_balance_changes USING btree (address);
CREATE INDEX idx_dag_balance_changes_address_snapshot_ordinal ON public.dag_balance_changes USING btree (address, snapshot_ordinal);
CREATE INDEX idx_dag_balance_changes_snapshot_hash ON public.dag_balance_changes USING btree (snapshot_hash);
CREATE INDEX idx_dag_balance_changes_snapshot_ordinal ON public.dag_balance_changes USING btree (snapshot_ordinal);

-- Table Triggers

create trigger set_updated_at_dag_balance_change before
update
    on
    public.dag_balance_changes for each row execute function update_updated_at_column();


-- public.dag_blocks definition

-- Drop table

-- DROP TABLE dag_blocks;

CREATE TABLE dag_blocks ( snapshot_hash varchar NOT NULL, CONSTRAINT dag_block_pk PRIMARY KEY (hash), CONSTRAINT dag_block_global_snapshot_fk FOREIGN KEY (snapshot_hash) REFERENCES global_snapshots(hash) ON DELETE CASCADE) INHERITS (public.abstract_blocks);
CREATE INDEX dag_blocks_snapshot_hash_idx ON public.dag_blocks USING btree (snapshot_hash);
CREATE INDEX idx_dag_blocks_snapshot_hash ON public.dag_blocks USING btree (snapshot_hash);

-- Table Triggers

create trigger set_updated_at_dag_blocks before
update
    on
    public.dag_blocks for each row execute function update_updated_at_column();
create trigger trigger_insert_abstract_blocks_dag after
insert
    on
    public.dag_blocks for each row execute function insert_into_parent_abstract_blocks();


-- public.dag_expired_spend_transactions definition

-- Drop table

-- DROP TABLE dag_expired_spend_transactions;

CREATE TABLE dag_expired_spend_transactions ( allow_spend_ref varchar NULL, snapshot_hash varchar NOT NULL, currency_id varchar NULL, CONSTRAINT dag_expired_spend_transactions_pk PRIMARY KEY (hash), CONSTRAINT dag_expired_spend_transactions_dag_allow_spends_fk FOREIGN KEY (allow_spend_ref) REFERENCES dag_allow_spends(hash) ON DELETE CASCADE, CONSTRAINT dag_expired_spend_transactions_global_snapshots_fk FOREIGN KEY (snapshot_hash) REFERENCES global_snapshots(hash) ON DELETE CASCADE ON UPDATE CASCADE, CONSTRAINT dag_expired_spend_transactions_metagraphs_fk FOREIGN KEY (currency_id) REFERENCES metagraphs(id) ON DELETE CASCADE) INHERITS (public.abstract_transactions);
CREATE INDEX dag_expired_spend_transactions_allow_spend_ref_idx ON public.dag_expired_spend_transactions USING btree (allow_spend_ref);
CREATE INDEX dag_expired_spend_transactions_snapshot_hash_idx ON public.dag_expired_spend_transactions USING btree (snapshot_hash);
CREATE INDEX dag_expired_spend_transactions_source_addr_idx ON public.dag_expired_spend_transactions USING btree (source_addr);
CREATE INDEX idx_dag_expired_spend_transactions_allow_spend_ref ON public.dag_expired_spend_transactions USING btree (allow_spend_ref);
CREATE INDEX idx_dag_expired_spend_transactions_currency_id ON public.dag_expired_spend_transactions USING btree (currency_id);
CREATE INDEX idx_dag_expired_spend_transactions_snapshot_hash ON public.dag_expired_spend_transactions USING btree (snapshot_hash);

-- Table Triggers

create trigger set_updated_at_dag_expired_spend_transactions before
update
    on
    public.dag_expired_spend_transactions for each row execute function update_updated_at_column();
create trigger trigger_insert_abstract_transactions_dag_expired_spend_transact after
insert
    on
    public.dag_expired_spend_transactions for each row execute function insert_into_parent_abstract_transactions();


-- public.dag_reward_transactions definition

-- Drop table

-- DROP TABLE dag_reward_transactions;

CREATE TABLE dag_reward_transactions ( global_snapshot_hash varchar NOT NULL, destination_addr varchar NOT NULL, amount int8 NOT NULL, created_at timestamp DEFAULT now() NOT NULL, updated_at timestamp DEFAULT now() NOT NULL, idx int4 DEFAULT '-1'::integer NOT NULL, CONSTRAINT dag_reward_transactions_pk PRIMARY KEY (global_snapshot_hash, destination_addr, idx), CONSTRAINT dag_reward_transaction_global_snapshot_fk FOREIGN KEY (global_snapshot_hash) REFERENCES global_snapshots(hash) ON DELETE CASCADE, CONSTRAINT dag_reward_transactions_destination_addr_fk FOREIGN KEY (destination_addr) REFERENCES addresses(address) ON DELETE CASCADE);
CREATE INDEX idx_dag_reward_transactions_destination_addr ON public.dag_reward_transactions USING btree (destination_addr);
CREATE INDEX idx_dag_reward_transactions_global_snapshot_hash ON public.dag_reward_transactions USING btree (global_snapshot_hash);

-- Table Triggers

create trigger set_updated_at_dag_reward_transaction before
update
    on
    public.dag_reward_transactions for each row execute function update_updated_at_column();


-- public.dag_spend_transactions definition

-- Drop table

-- DROP TABLE dag_spend_transactions;

CREATE TABLE dag_spend_transactions ( destination_addr varchar NULL, allow_spend_ref varchar NULL, snapshot_hash varchar NOT NULL, currency_id varchar NULL, CONSTRAINT dag_spend_transactions_pk PRIMARY KEY (hash), CONSTRAINT dag_spend_transactions_dag_allow_spends_fk FOREIGN KEY (allow_spend_ref) REFERENCES dag_allow_spends(hash) ON DELETE CASCADE, CONSTRAINT dag_spend_transactions_destination_addr_fk FOREIGN KEY (destination_addr) REFERENCES addresses(address) ON DELETE CASCADE, CONSTRAINT dag_spend_transactions_global_snapshots_fk FOREIGN KEY (hash) REFERENCES global_snapshots(hash) ON DELETE CASCADE, CONSTRAINT dag_spend_transactions_metagraphs_fk FOREIGN KEY (currency_id) REFERENCES metagraphs(id) ON DELETE CASCADE, CONSTRAINT dag_spend_transactions_source_addresses_fk FOREIGN KEY (source_addr) REFERENCES addresses(address) ON DELETE CASCADE) INHERITS (public.abstract_transactions);
CREATE INDEX dag_spend_transactions_allow_spend_ref_idx ON public.dag_spend_transactions USING btree (allow_spend_ref);
CREATE INDEX dag_spend_transactions_destination_addr_idx ON public.dag_spend_transactions USING btree (destination_addr);
CREATE INDEX dag_spend_transactions_snapshot_hash_idx ON public.dag_spend_transactions USING btree (snapshot_hash);
CREATE INDEX dag_spend_transactions_source_addr_idx ON public.dag_spend_transactions USING btree (source_addr);
CREATE INDEX idx_dag_spend_transactions_allow_spend_ref ON public.dag_spend_transactions USING btree (allow_spend_ref);
CREATE INDEX idx_dag_spend_transactions_currency_id ON public.dag_spend_transactions USING btree (currency_id);
CREATE INDEX idx_dag_spend_transactions_destination_addr ON public.dag_spend_transactions USING btree (destination_addr);

-- Table Triggers

create trigger set_updated_at_dag_spend_transactions before
update
    on
    public.dag_spend_transactions for each row execute function update_updated_at_column();
create trigger trigger_insert_abstract_transactions_dag_spend_transactions after
insert
    on
    public.dag_spend_transactions for each row execute function insert_into_parent_abstract_transactions();


-- public.dag_token_locks definition

-- Drop table

-- DROP TABLE dag_token_locks;

CREATE TABLE dag_token_locks ( ordinal int8 NOT NULL, unlock_epoch int8 NULL, round_id uuid NOT NULL, parent_hash varchar NULL, snapshot_hash varchar NOT NULL, currency_id varchar NULL, CONSTRAINT dag_token_locks_pk PRIMARY KEY (hash), CONSTRAINT dag_token_locks_unique UNIQUE (hash, ordinal), CONSTRAINT dag_token_locks_global_snapshots_fk FOREIGN KEY (snapshot_hash) REFERENCES global_snapshots(hash) ON DELETE CASCADE, CONSTRAINT dag_token_locks_metagraphs_fk FOREIGN KEY (currency_id) REFERENCES metagraphs(id) ON DELETE CASCADE, CONSTRAINT dag_token_locks_source_addr_fk FOREIGN KEY (source_addr) REFERENCES addresses(address) ON DELETE CASCADE) INHERITS (public.abstract_transactions);
CREATE INDEX dag_token_locks_snapshot_hash_idx ON public.dag_token_locks USING btree (snapshot_hash);
CREATE INDEX idx_dag_token_locks_currency_id ON public.dag_token_locks USING btree (currency_id);
CREATE INDEX idx_dag_token_locks_source_addr ON public.dag_token_locks USING btree (source_addr);

-- Table Triggers

create trigger set_updated_at_dag_token_locks before
update
    on
    public.dag_token_locks for each row execute function update_updated_at_column();
create trigger trigger_insert_abstract_transactions_dag_token_locks after
insert
    on
    public.dag_token_locks for each row execute function insert_into_parent_abstract_transactions();


-- public.dag_token_unlocks definition

-- Drop table

-- DROP TABLE dag_token_unlocks;

CREATE TABLE dag_token_unlocks ( lock_reference_hash varchar NOT NULL, snapshot_hash varchar NOT NULL, currency_id varchar NULL, CONSTRAINT dag_token_unlocks_pk PRIMARY KEY (hash), CONSTRAINT dag_token_unlocks_dag_token_locks_fk FOREIGN KEY (lock_reference_hash) REFERENCES dag_token_locks(hash) ON DELETE CASCADE, CONSTRAINT dag_token_unlocks_metagraphs_fk FOREIGN KEY (currency_id) REFERENCES metagraphs(id) ON DELETE CASCADE, CONSTRAINT ddag_token_unlocks_address_fk FOREIGN KEY (source_addr) REFERENCES addresses(address) ON DELETE CASCADE) INHERITS (public.abstract_transactions);
CREATE INDEX idx_dag_token_unlocks_currency_id ON public.dag_token_unlocks USING btree (currency_id);
CREATE INDEX idx_dag_token_unlocks_lock_reference_hash ON public.dag_token_unlocks USING btree (lock_reference_hash);
CREATE INDEX idx_dag_token_unlocks_source_addr ON public.dag_token_unlocks USING btree (source_addr);

-- Table Triggers

create trigger set_updated_at_dag_token_unlocks before
update
    on
    public.dag_token_unlocks for each row execute function update_updated_at_column();
create trigger trigger_insert_abstract_transactions_dag_token_unlocks after
insert
    on
    public.dag_token_unlocks for each row execute function insert_into_parent_abstract_transactions();


-- public.dag_transactions definition

-- Drop table

-- DROP TABLE dag_transactions;

CREATE TABLE dag_transactions ( destination_addr varchar NOT NULL, fee int8 NOT NULL, salt int8 NOT NULL, parent_ordinal int8 NULL, parent_hash varchar NULL, ordinal int8 NOT NULL, block_hash varchar NOT NULL, transaction_original jsonb NULL, snapshot_ordinal int8 NULL, CONSTRAINT dag_transaction_pk PRIMARY KEY (hash), CONSTRAINT dag_transaction_dag_block_fk FOREIGN KEY (block_hash) REFERENCES dag_blocks(hash) ON DELETE CASCADE ON UPDATE CASCADE, CONSTRAINT dag_transactions_destination_addr_fk FOREIGN KEY (destination_addr) REFERENCES addresses(address) ON DELETE CASCADE, CONSTRAINT dag_transactions_source_addr_fk FOREIGN KEY (source_addr) REFERENCES addresses(address) ON DELETE CASCADE) INHERITS (public.abstract_transactions);
CREATE INDEX idx_dag_transactions_block_hash ON public.dag_transactions USING btree (block_hash);
CREATE INDEX idx_dag_transactions_created_at ON public.dag_transactions USING btree (created_at);
CREATE INDEX idx_dag_transactions_destination_addr ON public.dag_transactions USING btree (destination_addr);
CREATE INDEX idx_dag_transactions_parent_hash ON public.dag_transactions USING btree (parent_hash);
CREATE INDEX idx_dag_transactions_snapshot_hash ON public.dag_transactions USING btree (snapshot_hash);
CREATE INDEX idx_dag_transactions_source_addr ON public.dag_transactions USING btree (source_addr);

-- Table Triggers

create trigger set_updated_at_dag_transaction before
update
    on
    public.dag_transactions for each row execute function update_updated_at_column();
create trigger trigger_insert_abstract_transactions_dag_transactions after
insert
    on
    public.dag_transactions for each row execute function insert_into_parent_abstract_transactions_from_block();
create trigger batch_trigger_set_dag_tx_snapshot_ordinal after
insert
    on
    public.dag_transactions for each statement execute function batch_set_dag_tx_snapshot_ordinal();


-- public.delegate_stake_create_events definition

-- Drop table

-- DROP TABLE delegate_stake_create_events;

CREATE TABLE delegate_stake_create_events ( hash varchar NOT NULL, ordinal int8 NOT NULL, source_addr varchar NOT NULL, node_id varchar NOT NULL, amount int8 NOT NULL, fee int8 DEFAULT 0 NOT NULL, lock_reference_hash varchar NOT NULL, parent_hash varchar NOT NULL, global_snapshot_hash varchar NOT NULL, transfer_from_hash varchar NULL, created_at timestamp DEFAULT now() NOT NULL, updated_at timestamp DEFAULT now() NOT NULL, CONSTRAINT delegate_stake_create_events_pkey PRIMARY KEY (hash), CONSTRAINT delegate_stake_create_events_global_snapshot_hash_fkey FOREIGN KEY (global_snapshot_hash) REFERENCES global_snapshots(hash) ON DELETE CASCADE, CONSTRAINT delegate_stake_create_events_lock_reference_hash_fkey FOREIGN KEY (lock_reference_hash) REFERENCES dag_token_locks(hash) ON DELETE CASCADE, CONSTRAINT delegate_stake_create_events_source_addr_fkey FOREIGN KEY (source_addr) REFERENCES addresses(address) ON DELETE CASCADE);
CREATE INDEX delegate_stake_create_events_global_snapshot_hash_idx ON public.delegate_stake_create_events USING btree (global_snapshot_hash);
CREATE INDEX delegate_stake_create_events_lock_reference_hash_idx ON public.delegate_stake_create_events USING btree (lock_reference_hash);
CREATE INDEX delegate_stake_create_events_source_addr_idx ON public.delegate_stake_create_events USING btree (source_addr);
CREATE INDEX idx_create_events_hash ON public.delegate_stake_create_events USING btree (hash);
CREATE INDEX idx_create_events_hash_lock ON public.delegate_stake_create_events USING btree (hash, lock_reference_hash);
CREATE INDEX idx_create_transfer_from_hash ON public.delegate_stake_create_events USING btree (transfer_from_hash);
CREATE INDEX idx_delegate_stake_events_ord_desc ON public.delegate_stake_create_events USING btree (source_addr, node_id, ordinal DESC);


-- public.delegate_stake_rewards definition

-- Drop table

-- DROP TABLE delegate_stake_rewards;

CREATE TABLE delegate_stake_rewards ( global_snapshot_hash varchar NOT NULL, address varchar NOT NULL, node_id varchar NOT NULL, rewards int8 NOT NULL, stake_create_hash varchar NOT NULL, created_at timestamp DEFAULT now() NOT NULL, updated_at timestamp DEFAULT now() NOT NULL, CONSTRAINT delegate_stake_rewards_changes_unique UNIQUE (global_snapshot_hash, address, node_id, rewards), CONSTRAINT delegate_stake_rewards_pkey PRIMARY KEY (global_snapshot_hash, stake_create_hash), CONSTRAINT delegate_stake_rewards_address_fkey FOREIGN KEY (address) REFERENCES addresses(address) ON DELETE CASCADE, CONSTRAINT delegate_stake_rewards_delegate_stake_create_events_fk FOREIGN KEY (stake_create_hash) REFERENCES delegate_stake_create_events(hash) ON DELETE CASCADE, CONSTRAINT delegate_stake_rewards_global_snapshot_hash_fkey FOREIGN KEY (global_snapshot_hash) REFERENCES global_snapshots(hash) ON DELETE CASCADE);
CREATE INDEX idx_delegate_stake_rewards_stake_create_hash ON public.delegate_stake_rewards USING btree (stake_create_hash);
CREATE INDEX idx_rewards_hash_amount ON public.delegate_stake_rewards USING btree (stake_create_hash, rewards);


-- public.delegate_stake_withdraw_events definition

-- Drop table

-- DROP TABLE delegate_stake_withdraw_events;

CREATE TABLE delegate_stake_withdraw_events ( hash varchar NOT NULL, source_addr varchar NOT NULL, stake_create_hash varchar NOT NULL, global_snapshot_hash varchar NOT NULL, unlock_epoch int8 NULL, created_at_epoch int8 NULL, is_completed bool NULL, created_at timestamp DEFAULT now() NOT NULL, updated_at timestamp DEFAULT now() NOT NULL, CONSTRAINT delegate_stake_withdraw_events_pkey PRIMARY KEY (hash), CONSTRAINT delegate_stake_withdraw_events_global_snapshot_hash_fkey FOREIGN KEY (global_snapshot_hash) REFERENCES global_snapshots(hash) ON DELETE CASCADE, CONSTRAINT delegate_stake_withdraw_events_source_addr_fkey FOREIGN KEY (source_addr) REFERENCES addresses(address) ON DELETE CASCADE, CONSTRAINT delegate_stake_withdraw_events_stake_create_hash_fkey FOREIGN KEY (stake_create_hash) REFERENCES delegate_stake_create_events(hash) ON DELETE CASCADE);
CREATE INDEX delegate_stake_withdraw_events_global_snapshot_hash_idx ON public.delegate_stake_withdraw_events USING btree (global_snapshot_hash);
CREATE INDEX delegate_stake_withdraw_events_source_addr_idx ON public.delegate_stake_withdraw_events USING btree (source_addr);
CREATE INDEX delegate_stake_withdraw_events_stake_create_hash_idx ON public.delegate_stake_withdraw_events USING btree (stake_create_hash);
CREATE INDEX idx_dswe_completed_hash ON public.delegate_stake_withdraw_events USING btree (stake_create_hash) WHERE is_completed;
CREATE INDEX idx_withdraw_stake_hash ON public.delegate_stake_withdraw_events USING btree (stake_create_hash);
CREATE INDEX idx_withdraw_stake_hash_completed ON public.delegate_stake_withdraw_events USING btree (stake_create_hash, is_completed);


-- public.global_snapshot_proofs definition

-- Drop table

-- DROP TABLE global_snapshot_proofs;

CREATE TABLE global_snapshot_proofs ( id varchar NOT NULL, signature varchar NOT NULL, snapshot_hash varchar NOT NULL, created_at timestamp DEFAULT now() NOT NULL, updated_at timestamp DEFAULT now() NOT NULL, CONSTRAINT proof_pk PRIMARY KEY (snapshot_hash, id), CONSTRAINT proof_global_snapshot_fk FOREIGN KEY (snapshot_hash) REFERENCES global_snapshots(hash) ON DELETE CASCADE);
CREATE INDEX idx_global_snapshot_proofs_snapshot_hash ON public.global_snapshot_proofs USING btree (snapshot_hash);

-- Table Triggers

create trigger set_updated_at_proof before
update
    on
    public.global_snapshot_proofs for each row execute function update_updated_at_column();


-- public.metagraph_allow_spends definition

-- Drop table

-- DROP TABLE metagraph_allow_spends;

CREATE TABLE metagraph_allow_spends ( metagraph_id varchar NOT NULL, destination_addr varchar NOT NULL, fee int8 NOT NULL, parent_ordinal int8 NULL, parent_hash varchar NULL, last_valid_epoch_progress int8 NOT NULL, round_id uuid NOT NULL, ordinal int8 NOT NULL, snapshot_hash varchar NOT NULL, currency_id varchar NULL, CONSTRAINT metagraph_allow_spends_pk PRIMARY KEY (hash), CONSTRAINT metagraph_allow_spends_destination_addr_fk FOREIGN KEY (destination_addr) REFERENCES addresses(address) ON DELETE CASCADE, CONSTRAINT metagraph_allow_spends_metagraphs_fk FOREIGN KEY (currency_id) REFERENCES metagraphs(id) ON DELETE CASCADE, CONSTRAINT metagraph_allow_spends_source_addr_fk FOREIGN KEY (source_addr) REFERENCES addresses(address) ON DELETE CASCADE, CONSTRAINT metagraph_id_fk FOREIGN KEY (metagraph_id) REFERENCES metagraphs(id) ON DELETE CASCADE) INHERITS (public.abstract_transactions);
CREATE INDEX idx_metagraph_allow_spends_currency_id ON public.metagraph_allow_spends USING btree (currency_id);
CREATE INDEX idx_metagraph_allow_spends_destination_addr ON public.metagraph_allow_spends USING btree (destination_addr);
CREATE INDEX idx_metagraph_allow_spends_metagraph_id ON public.metagraph_allow_spends USING btree (metagraph_id);
CREATE INDEX idx_metagraph_allow_spends_round_id ON public.metagraph_allow_spends USING btree (round_id);
CREATE INDEX idx_metagraph_allow_spends_source_addr ON public.metagraph_allow_spends USING btree (source_addr);

-- Table Triggers

create trigger set_updated_at_metagraph_allow_spends before
update
    on
    public.metagraph_allow_spends for each row execute function update_updated_at_column();
create trigger trigger_insert_abstract_transactions_metagraph_allow_spends after
insert
    on
    public.metagraph_allow_spends for each row execute function insert_into_parent_abstract_transactions();


-- public.metagraph_expired_spend_transactions definition

-- Drop table

-- DROP TABLE metagraph_expired_spend_transactions;

CREATE TABLE metagraph_expired_spend_transactions ( metagraph_id varchar NOT NULL, allow_spend_ref varchar NULL, snapshot_hash varchar NULL, currency_id varchar NULL, CONSTRAINT metagraph_expired_spend_transactions_pk PRIMARY KEY (hash), CONSTRAINT metagraph_expired_spend_transactions_metagraph_allow_spends_fk FOREIGN KEY (allow_spend_ref) REFERENCES metagraph_allow_spends(hash) ON DELETE CASCADE, CONSTRAINT metagraph_expired_spend_transactions_metagraphs_fk FOREIGN KEY (currency_id) REFERENCES metagraphs(id) ON DELETE CASCADE) INHERITS (public.abstract_transactions);
CREATE INDEX idx_metagraph_expired_spend_transactions_allow_spend_ref ON public.metagraph_expired_spend_transactions USING btree (allow_spend_ref);
CREATE INDEX idx_metagraph_expired_spend_transactions_currency_id ON public.metagraph_expired_spend_transactions USING btree (currency_id);
CREATE INDEX idx_metagraph_expired_spend_transactions_snapshot_hash ON public.metagraph_expired_spend_transactions USING btree (snapshot_hash);

-- Table Triggers

create trigger set_updated_at_metagraph_expired_spend_transactions before
update
    on
    public.metagraph_expired_spend_transactions for each row execute function update_updated_at_column();
create trigger trigger_insert_abstract_transactions_metagraph_expired_spend_tr after
insert
    on
    public.metagraph_expired_spend_transactions for each row execute function insert_into_parent_abstract_transactions();


-- public.metagraph_snapshots definition

-- Drop table

-- DROP TABLE metagraph_snapshots;

CREATE TABLE metagraph_snapshots ( metagraph_id varchar NOT NULL, ordinal int8 NOT NULL, global_snapshot_hash varchar NULL, hash varchar NOT NULL, height int8 NOT NULL, subheight int4 NOT NULL, last_snapshot_hash varchar NULL, fee int8 NULL, owner_address varchar NULL, staking_address varchar NULL, epoch_progress int8 NULL, "size" int8 NULL, "version" varchar NOT NULL, created_at timestamp DEFAULT now() NOT NULL, updated_at timestamp DEFAULT now() NOT NULL, CONSTRAINT metagraph_snapshot_pk PRIMARY KEY (metagraph_id, hash), CONSTRAINT metagraph_snapshot_unique UNIQUE (metagraph_id, ordinal), CONSTRAINT metagraph_id_fk FOREIGN KEY (metagraph_id) REFERENCES metagraphs(id) ON DELETE CASCADE, CONSTRAINT metagraph_snapshots_global_snapshots_fk FOREIGN KEY (global_snapshot_hash) REFERENCES global_snapshots(hash) ON DELETE CASCADE, CONSTRAINT owner_address_fk FOREIGN KEY (owner_address) REFERENCES addresses(address) ON DELETE CASCADE, CONSTRAINT staking_address_fk FOREIGN KEY (staking_address) REFERENCES addresses(address) ON DELETE CASCADE);
CREATE INDEX idx_metagraph_snapshots_global_snapshot_hash ON public.metagraph_snapshots USING btree (global_snapshot_hash);
CREATE INDEX idx_metagraph_snapshots_hash ON public.metagraph_snapshots USING btree (hash);
CREATE INDEX idx_metagraph_snapshots_metagraph_id ON public.metagraph_snapshots USING btree (metagraph_id);
CREATE INDEX idx_metagraph_snapshots_owner_address ON public.metagraph_snapshots USING btree (owner_address);
CREATE INDEX idx_metagraph_snapshots_staking_address ON public.metagraph_snapshots USING btree (staking_address);
CREATE UNIQUE INDEX metagraph_id_ordinal ON public.metagraph_snapshots USING btree (metagraph_id, ordinal);

-- Table Triggers

create trigger set_updated_at_metagraph_snapshot before
update
    on
    public.metagraph_snapshots for each row execute function update_updated_at_column();


-- public.metagraph_spend_transactions definition

-- Drop table

-- DROP TABLE metagraph_spend_transactions;

CREATE TABLE metagraph_spend_transactions ( metagraph_id varchar NOT NULL, destination_addr varchar NOT NULL, allow_spend_ref varchar NULL, snapshot_hash varchar NOT NULL, currency_id varchar NULL, CONSTRAINT metagraph_spend_transactions_pk PRIMARY KEY (hash), CONSTRAINT dag_spend_transactions_metagraph_allow_spends_fk FOREIGN KEY (allow_spend_ref) REFERENCES metagraph_allow_spends(hash), CONSTRAINT metagraph_spend_transactions_destination_addr_fk FOREIGN KEY (destination_addr) REFERENCES addresses(address) ON DELETE CASCADE, CONSTRAINT metagraph_spend_transactions_metagraph_allow_spends_fk FOREIGN KEY (allow_spend_ref) REFERENCES metagraph_allow_spends(hash) ON DELETE CASCADE, CONSTRAINT metagraph_spend_transactions_metagraph_id_fk FOREIGN KEY (metagraph_id) REFERENCES metagraphs(id) ON DELETE CASCADE, CONSTRAINT metagraph_spend_transactions_metagraphs_fk FOREIGN KEY (currency_id) REFERENCES metagraphs(id) ON DELETE CASCADE) INHERITS (public.abstract_transactions);
CREATE INDEX idx_metagraph_spend_transactions_allow_spend_ref ON public.metagraph_spend_transactions USING btree (allow_spend_ref);
CREATE INDEX idx_metagraph_spend_transactions_currency_id ON public.metagraph_spend_transactions USING btree (currency_id);
CREATE INDEX idx_metagraph_spend_transactions_destination_addr ON public.metagraph_spend_transactions USING btree (destination_addr);
CREATE INDEX idx_metagraph_spend_transactions_metagraph_id ON public.metagraph_spend_transactions USING btree (metagraph_id);

-- Table Triggers

create trigger set_updated_at_metagraph_spend_transactions before
update
    on
    public.metagraph_spend_transactions for each row execute function update_updated_at_column();
create trigger trigger_insert_abstract_transactions_metagraph_spend_transactio after
insert
    on
    public.metagraph_spend_transactions for each row execute function insert_into_parent_abstract_transactions();


-- public.metagraph_token_lock_blocks definition

-- Drop table

-- DROP TABLE metagraph_token_lock_blocks;

CREATE TABLE metagraph_token_lock_blocks ( metagraph_id varchar NOT NULL, metagraph_snapshot_hash varchar NOT NULL, round_id uuid NOT NULL, created_at timestamp DEFAULT now() NOT NULL, updated_at timestamp DEFAULT now() NOT NULL, CONSTRAINT metagraph_token_lock_blocks_pkey PRIMARY KEY (metagraph_id, metagraph_snapshot_hash), CONSTRAINT metagraph_token_lock_blocks_unique UNIQUE (metagraph_id, round_id), CONSTRAINT metagraph_token_lock_blocks_metagraph_id_fk FOREIGN KEY (metagraph_id) REFERENCES metagraphs(id) ON DELETE CASCADE, CONSTRAINT metagraph_token_lock_blocks_metagraph_snapshot_fk FOREIGN KEY (metagraph_id,metagraph_snapshot_hash) REFERENCES metagraph_snapshots(metagraph_id,hash) ON DELETE CASCADE);
CREATE INDEX idx_metagraph_token_lock_blocks_metagraph_id ON public.metagraph_token_lock_blocks USING btree (metagraph_id);
CREATE INDEX idx_metagraph_token_lock_blocks_metagraph_snapshot_hash ON public.metagraph_token_lock_blocks USING btree (metagraph_snapshot_hash);

-- Table Triggers

create trigger set_updated_at_metagraph_token_lock_blocks before
update
    on
    public.metagraph_token_lock_blocks for each row execute function update_updated_at_column();


-- public.metagraph_token_locks definition

-- Drop table

-- DROP TABLE metagraph_token_locks;

CREATE TABLE metagraph_token_locks ( metagraph_id varchar NOT NULL, ordinal int8 NOT NULL, unlock_epoch int8 NOT NULL, round_id varchar NOT NULL, parent_hash varchar NULL, snapshot_hash varchar NOT NULL, currency_id varchar NULL, CONSTRAINT metagraph_token_locks_pk PRIMARY KEY (metagraph_id, hash), CONSTRAINT metagraph_token_locks_unique UNIQUE (metagraph_id, ordinal), CONSTRAINT metagraph_id_fk FOREIGN KEY (metagraph_id) REFERENCES metagraphs(id) ON DELETE CASCADE, CONSTRAINT metagraph_token_locks_metagraphs_fk FOREIGN KEY (currency_id) REFERENCES metagraphs(id) ON DELETE CASCADE, CONSTRAINT metagraph_token_locks_source_addr_fk FOREIGN KEY (source_addr) REFERENCES addresses(address) ON DELETE CASCADE) INHERITS (public.abstract_transactions);
CREATE INDEX idx_metagraph_token_locks_currency_id ON public.metagraph_token_locks USING btree (currency_id);
CREATE INDEX idx_metagraph_token_locks_source_addr ON public.metagraph_token_locks USING btree (source_addr);

-- Table Triggers

create trigger set_updated_at_metagraph_token_locks before
update
    on
    public.metagraph_token_locks for each row execute function update_updated_at_column();
create trigger trigger_insert_abstract_transactions_metagraph_token_locks after
insert
    on
    public.metagraph_token_locks for each row execute function insert_into_parent_abstract_transactions();


-- public.metagraph_token_unlocks definition

-- Drop table

-- DROP TABLE metagraph_token_unlocks;

CREATE TABLE metagraph_token_unlocks ( metagraph_id varchar NOT NULL, lock_reference_ordinal int8 NULL, lock_reference_hash varchar NOT NULL, snapshot_hash varchar NOT NULL, parent_hash varchar NULL, currency_id varchar NULL, CONSTRAINT metagraph_token_unlocks_pk PRIMARY KEY (hash, metagraph_id), CONSTRAINT address_fk FOREIGN KEY (source_addr) REFERENCES addresses(address) ON DELETE CASCADE, CONSTRAINT metagraph_id_fk FOREIGN KEY (metagraph_id) REFERENCES metagraphs(id) ON DELETE CASCADE, CONSTRAINT metagraph_token_unlocks_metagraphs_fk FOREIGN KEY (currency_id) REFERENCES metagraphs(id) ON DELETE CASCADE, CONSTRAINT metagraph_token_unlocks_token_locks_fk FOREIGN KEY (metagraph_id,lock_reference_hash) REFERENCES metagraph_token_locks(metagraph_id,hash) ON DELETE CASCADE, CONSTRAINT metagraph_token_unlocks_token_locks_ordinal_fk FOREIGN KEY (metagraph_id,lock_reference_ordinal) REFERENCES metagraph_token_locks(metagraph_id,ordinal) ON DELETE CASCADE) INHERITS (public.abstract_transactions);
CREATE INDEX idx_metagraph_token_unlocks_currency_id ON public.metagraph_token_unlocks USING btree (currency_id);
CREATE INDEX idx_metagraph_token_unlocks_lock_reference_hash ON public.metagraph_token_unlocks USING btree (lock_reference_hash);
CREATE INDEX idx_metagraph_token_unlocks_lock_reference_ordinal ON public.metagraph_token_unlocks USING btree (lock_reference_ordinal);
CREATE INDEX idx_metagraph_token_unlocks_metagraph_id ON public.metagraph_token_unlocks USING btree (metagraph_id);
CREATE INDEX idx_metagraph_token_unlocks_source_addr ON public.metagraph_token_unlocks USING btree (source_addr);

-- Table Triggers

create trigger set_updated_at_metagraph_token_unlocks before
update
    on
    public.metagraph_token_unlocks for each row execute function update_updated_at_column();
create trigger trigger_insert_abstract_transactions_metagraph_token_unlocks after
insert
    on
    public.metagraph_token_unlocks for each row execute function insert_into_parent_abstract_transactions();


-- public.dag_allow_spend_approvers definition

-- Drop table

-- DROP TABLE dag_allow_spend_approvers;

CREATE TABLE dag_allow_spend_approvers ( allow_spend_hash varchar NOT NULL, approver_address varchar NOT NULL, CONSTRAINT dag_allow_spend_approvers_pk PRIMARY KEY (allow_spend_hash, approver_address), CONSTRAINT dag_allow_spend_approvers_address_fk FOREIGN KEY (approver_address) REFERENCES addresses(address) ON DELETE CASCADE, CONSTRAINT dag_allow_spends_fk FOREIGN KEY (allow_spend_hash) REFERENCES dag_allow_spends(hash) ON DELETE CASCADE);
CREATE INDEX idx_dag_allow_spend_approvers_approver_address ON public.dag_allow_spend_approvers USING btree (approver_address);


-- public.metagraph_allow_spend_approvers definition

-- Drop table

-- DROP TABLE metagraph_allow_spend_approvers;

CREATE TABLE metagraph_allow_spend_approvers ( allow_spend_hash varchar NOT NULL, approver_address varchar NOT NULL, CONSTRAINT metagraph_allow_spend_approvers_pk PRIMARY KEY (allow_spend_hash, approver_address), CONSTRAINT ametagraph_allow_spends_fk FOREIGN KEY (allow_spend_hash) REFERENCES metagraph_allow_spends(hash) ON DELETE CASCADE, CONSTRAINT metagraph_allow_spend_approvers_address_fk FOREIGN KEY (approver_address) REFERENCES addresses(address) ON DELETE CASCADE);
CREATE INDEX idx_metagraph_allow_spend_approvers_allow_spend_hash ON public.metagraph_allow_spend_approvers USING btree (allow_spend_hash);
CREATE INDEX idx_metagraph_allow_spend_approvers_approver_address ON public.metagraph_allow_spend_approvers USING btree (approver_address);


-- public.metagraph_allow_spend_blocks definition

-- Drop table

-- DROP TABLE metagraph_allow_spend_blocks;

CREATE TABLE metagraph_allow_spend_blocks ( metagraph_id varchar NOT NULL, round_id uuid NOT NULL, metagraph_snapshot_hash varchar NOT NULL, created_at timestamp DEFAULT now() NOT NULL, updated_at timestamp DEFAULT now() NOT NULL, CONSTRAINT metagraph_allow_spend_blocks_pkey PRIMARY KEY (round_id), CONSTRAINT metagraph_allow_spend_blocks_metagraph_snapshot_fk FOREIGN KEY (metagraph_id,metagraph_snapshot_hash) REFERENCES metagraph_snapshots(metagraph_id,hash) ON DELETE CASCADE);
CREATE INDEX idx_metagraph_allow_spend_blocks_metagraph_id ON public.metagraph_allow_spend_blocks USING btree (metagraph_id);
CREATE INDEX idx_metagraph_allow_spend_blocks_metagraph_snapshot_hash ON public.metagraph_allow_spend_blocks USING btree (metagraph_snapshot_hash);

-- Table Triggers

create trigger set_updated_at_metagraph_allow_spend_blocks before
update
    on
    public.metagraph_allow_spend_blocks for each row execute function update_updated_at_column();


-- public.metagraph_balance_changes definition

-- Drop table

-- DROP TABLE metagraph_balance_changes;

CREATE TABLE metagraph_balance_changes ( metagraph_id varchar NOT NULL, metagraph_snapshot_hash varchar NOT NULL, address varchar NOT NULL, balance int8 NOT NULL, created_at timestamp DEFAULT now() NOT NULL, updated_at timestamp DEFAULT now() NOT NULL, metagraph_snapshot_ordinal int8 NOT NULL, CONSTRAINT metagraph_balance_change_pk PRIMARY KEY (metagraph_id, address, metagraph_snapshot_ordinal), CONSTRAINT metagraph_balance_changes_unique UNIQUE (metagraph_id, metagraph_snapshot_hash, address, balance), CONSTRAINT address_fk FOREIGN KEY (address) REFERENCES addresses(address) ON DELETE CASCADE, CONSTRAINT metagraph_balance_change_metagraph_snapshot_fk FOREIGN KEY (metagraph_id,metagraph_snapshot_hash) REFERENCES metagraph_snapshots(metagraph_id,hash) ON DELETE CASCADE, CONSTRAINT metagraph_id_fk FOREIGN KEY (metagraph_id) REFERENCES metagraphs(id) ON DELETE CASCADE);
CREATE INDEX idx_metagraph_balance_changes_metagraph_id ON public.metagraph_balance_changes USING btree (metagraph_id);
CREATE INDEX idx_metagraph_balance_changes_metagraph_snapshot_hash ON public.metagraph_balance_changes USING btree (metagraph_snapshot_hash);

-- Table Triggers

create trigger set_updated_at_metagraph_balance_change before
update
    on
    public.metagraph_balance_changes for each row execute function update_updated_at_column();


-- public.metagraph_blocks definition

-- Drop table

-- DROP TABLE metagraph_blocks;

CREATE TABLE metagraph_blocks ( metagraph_id varchar NOT NULL, metagraph_snapshot_hash varchar NOT NULL, CONSTRAINT metagraph_block_pk PRIMARY KEY (metagraph_id, hash), CONSTRAINT metagraph_block_metagraph_snapshot_fk FOREIGN KEY (metagraph_id,metagraph_snapshot_hash) REFERENCES metagraph_snapshots(metagraph_id,hash) ON DELETE CASCADE, CONSTRAINT metagraph_id_fk FOREIGN KEY (metagraph_id) REFERENCES metagraphs(id) ON DELETE CASCADE) INHERITS (public.abstract_blocks);
CREATE INDEX idx_blocks_metagraph_id_snapshot_hash ON public.metagraph_blocks USING btree (metagraph_id, metagraph_snapshot_hash);
CREATE INDEX idx_metagraph_blocks_metagraph_id ON public.metagraph_blocks USING btree (metagraph_id);
CREATE INDEX idx_metagraph_blocks_metagraph_snapshot_hash ON public.metagraph_blocks USING btree (metagraph_snapshot_hash);
CREATE INDEX idx_metagraph_blocks_snap ON public.metagraph_blocks USING btree (metagraph_id, metagraph_snapshot_hash);

-- Table Triggers

create trigger set_updated_at_metagraph_blocks before
update
    on
    public.metagraph_blocks for each row execute function update_updated_at_column();
create trigger trigger_insert_abstract_blocks_metagraph after
insert
    on
    public.metagraph_blocks for each row execute function insert_into_parent_abstract_blocks();


-- public.metagraph_fee_transactions definition

-- Drop table

-- DROP TABLE metagraph_fee_transactions;

CREATE TABLE metagraph_fee_transactions ( created_at timestamp DEFAULT now() NOT NULL, metagraph_id varchar NOT NULL, metagraph_snapshot_hash varchar NOT NULL, destination_addr varchar NOT NULL, data_update_ref varchar NULL, metagraph_snapshot_ordinal int8 NULL, CONSTRAINT fee_transaction_pk PRIMARY KEY (metagraph_id, hash), CONSTRAINT fee_transaction_metagraph_snapshot_fk FOREIGN KEY (metagraph_id,metagraph_snapshot_hash) REFERENCES metagraph_snapshots(metagraph_id,hash) ON DELETE CASCADE, CONSTRAINT metagraph_fee_transactions_destination_addr_fk FOREIGN KEY (destination_addr) REFERENCES addresses(address) ON DELETE CASCADE, CONSTRAINT metagraph_fee_transactions_source_addr_fk FOREIGN KEY (source_addr) REFERENCES addresses(address) ON DELETE CASCADE, CONSTRAINT metagraph_id_fk FOREIGN KEY (metagraph_id) REFERENCES metagraphs(id) ON DELETE CASCADE) INHERITS (public.abstract_transactions);
CREATE INDEX idx_metagraph_fee_transactions_destination_addr ON public.metagraph_fee_transactions USING btree (destination_addr);
CREATE INDEX idx_metagraph_fee_transactions_metagraph_id ON public.metagraph_fee_transactions USING btree (metagraph_id);
CREATE INDEX idx_metagraph_fee_transactions_metagraph_snapshot_hash ON public.metagraph_fee_transactions USING btree (metagraph_snapshot_hash);
CREATE INDEX idx_metagraph_fee_transactions_source_addr ON public.metagraph_fee_transactions USING btree (source_addr);

-- Table Triggers

create trigger set_updated_at_metagraph_fee_transactions before
update
    on
    public.metagraph_fee_transactions for each row execute function update_updated_at_column();
create trigger trigger_insert_abstract_transactions_metagraph_fee_transactions after
insert
    on
    public.metagraph_fee_transactions for each row execute function insert_into_parent_abstract_transactions();


-- public.metagraph_reward_transactions definition

-- Drop table

-- DROP TABLE metagraph_reward_transactions;

CREATE TABLE metagraph_reward_transactions ( metagraph_id varchar NOT NULL, metagraph_snapshot_hash varchar NOT NULL, destination_addr varchar NOT NULL, amount int8 NOT NULL, created_at timestamp DEFAULT now() NOT NULL, updated_at timestamp DEFAULT now() NOT NULL, idx int4 DEFAULT '-1'::integer NOT NULL, CONSTRAINT metagraph_reward_transactions_pk PRIMARY KEY (metagraph_id, metagraph_snapshot_hash, destination_addr, idx), CONSTRAINT metagraph_reward_transaction_metagraph_reward_transaction_fk FOREIGN KEY (metagraph_id,metagraph_snapshot_hash) REFERENCES metagraph_snapshots(metagraph_id,hash) ON DELETE CASCADE, CONSTRAINT metagraph_reward_transactions_destination_addr_fk FOREIGN KEY (destination_addr) REFERENCES addresses(address) ON DELETE CASCADE, CONSTRAINT metagraph_reward_transactions_metagraph_id_fk FOREIGN KEY (metagraph_id) REFERENCES metagraphs(id) ON DELETE CASCADE);
CREATE INDEX idx_metagraph_reward_transactions_destination_addr ON public.metagraph_reward_transactions USING btree (destination_addr);
CREATE INDEX idx_metagraph_reward_transactions_metagraph_id ON public.metagraph_reward_transactions USING btree (metagraph_id);
CREATE INDEX idx_metagraph_reward_transactions_metagraph_snapshot_hash ON public.metagraph_reward_transactions USING btree (metagraph_snapshot_hash);

-- Table Triggers

create trigger set_updated_at_metagraph_reward_transaction before
update
    on
    public.metagraph_reward_transactions for each row execute function update_updated_at_column();


-- public.metagraph_transactions definition

-- Drop table

-- DROP TABLE metagraph_transactions;

CREATE TABLE metagraph_transactions ( metagraph_id varchar NOT NULL, destination_addr varchar NOT NULL, fee int8 NOT NULL, salt int8 NOT NULL, parent_ordinal int8 NOT NULL, parent_hash varchar NOT NULL, ordinal int8 NOT NULL, block_hash varchar NOT NULL, transaction_original jsonb NULL, snapshot_ordinal int8 NULL, CONSTRAINT metagraph_transaction_pk PRIMARY KEY (metagraph_id, hash), CONSTRAINT metagraph_transactions_unique_hash UNIQUE (hash), CONSTRAINT metagraph_id_fk FOREIGN KEY (metagraph_id) REFERENCES metagraphs(id) ON DELETE CASCADE, CONSTRAINT metagraph_transaction_metagraph_block_fk FOREIGN KEY (metagraph_id,block_hash) REFERENCES metagraph_blocks(metagraph_id,hash) ON DELETE CASCADE ON UPDATE CASCADE, CONSTRAINT metagraph_transactions_destination_addrfk FOREIGN KEY (destination_addr) REFERENCES addresses(address) ON DELETE CASCADE, CONSTRAINT metagraph_transactions_source_addr_fk FOREIGN KEY (source_addr) REFERENCES addresses(address) ON DELETE CASCADE) INHERITS (public.abstract_transactions);
CREATE INDEX idx_meta_txn_filter_order ON public.metagraph_transactions USING btree (metagraph_id, source_addr, snapshot_ordinal DESC, created_at DESC, hash DESC);
CREATE INDEX idx_metagraph_transactions_block_hash ON public.metagraph_transactions USING btree (block_hash);
CREATE INDEX idx_metagraph_transactions_destination_addr ON public.metagraph_transactions USING btree (destination_addr);
CREATE INDEX idx_metagraph_transactions_filter_sort ON public.metagraph_transactions USING btree (metagraph_id, snapshot_hash, created_at DESC, hash DESC);
CREATE INDEX idx_metagraph_transactions_id_hash ON public.metagraph_transactions USING btree (metagraph_id DESC, hash DESC);
CREATE INDEX idx_metagraph_transactions_metagraph_id ON public.metagraph_transactions USING btree (metagraph_id);
CREATE INDEX idx_metagraph_transactions_parent_hash ON public.metagraph_transactions USING btree (parent_hash);
CREATE INDEX idx_metagraph_transactions_snapshot_hash ON public.metagraph_transactions USING btree (snapshot_hash);
CREATE INDEX idx_metagraph_transactions_source_addr ON public.metagraph_transactions USING btree (source_addr);
CREATE INDEX idx_transactions_sorting ON public.metagraph_transactions USING btree (snapshot_hash, created_at DESC, hash DESC);

-- Table Triggers

create trigger set_updated_at_metagraph_transaction before
update
    on
    public.metagraph_transactions for each row execute function update_updated_at_column();
create trigger trigger_insert_abstract_transactions_metagraph_transactions after
insert
    on
    public.metagraph_transactions for each row execute function insert_into_parent_abstract_transactions_from_metagraph_block();
create trigger batch_trigger_set_mg_tx_snapshot_ordinal after
insert
    on
    public.metagraph_transactions for each statement execute function batch_set_mg_tx_snapshot_ordinal();


-- public.abstract_transactions_view source

CREATE OR REPLACE VIEW abstract_transactions_view
AS SELECT dag_allow_spends.hash,
    dag_allow_spends.source_addr,
    dag_allow_spends.amount,
    dag_allow_spends.created_at,
    dag_allow_spends.updated_at,
    'dag_allow_spends'::name AS table_name,
    dag_allow_spends.snapshot_hash
   FROM dag_allow_spends
UNION ALL
 SELECT dag_spend_transactions.hash,
    dag_spend_transactions.source_addr,
    dag_spend_transactions.amount,
    dag_spend_transactions.created_at,
    dag_spend_transactions.updated_at,
    'dag_spend_transactions'::name AS table_name,
    dag_spend_transactions.snapshot_hash
   FROM dag_spend_transactions
UNION ALL
 SELECT dag_expired_spend_transactions.hash,
    dag_expired_spend_transactions.source_addr,
    dag_expired_spend_transactions.amount,
    dag_expired_spend_transactions.created_at,
    dag_expired_spend_transactions.updated_at,
    'dag_expired_spend_transactions'::name AS table_name,
    dag_expired_spend_transactions.snapshot_hash
   FROM dag_expired_spend_transactions
UNION ALL
 SELECT dag_token_locks.hash,
    dag_token_locks.source_addr,
    dag_token_locks.amount,
    dag_token_locks.created_at,
    dag_token_locks.updated_at,
    'dag_token_locks'::name AS table_name,
    dag_token_locks.snapshot_hash
   FROM dag_token_locks
UNION ALL
 SELECT dag_token_unlocks.hash,
    dag_token_unlocks.source_addr,
    dag_token_unlocks.amount,
    dag_token_unlocks.created_at,
    dag_token_unlocks.updated_at,
    'dag_token_unlocks'::name AS table_name,
    dag_token_unlocks.snapshot_hash
   FROM dag_token_unlocks
UNION ALL
 SELECT dag_transactions.hash,
    dag_transactions.source_addr,
    dag_transactions.amount,
    dag_transactions.created_at,
    dag_transactions.updated_at,
    'dag_transactions'::name AS table_name,
    dag_transactions.snapshot_hash
   FROM dag_transactions
UNION ALL
 SELECT delegate_stake_create_events.hash,
    delegate_stake_create_events.source_addr,
    delegate_stake_create_events.amount,
    delegate_stake_create_events.created_at,
    delegate_stake_create_events.updated_at,
    'dag_delegate_stake_create_events'::name AS table_name,
    delegate_stake_create_events.global_snapshot_hash AS snapshot_hash
   FROM delegate_stake_create_events
UNION ALL
 SELECT delegate_stake_withdraw_events.hash,
    delegate_stake_withdraw_events.source_addr,
    NULL::bigint AS amount,
    delegate_stake_withdraw_events.created_at,
    delegate_stake_withdraw_events.updated_at,
    'dag_delegate_stake_withdraw_events'::name AS table_name,
    delegate_stake_withdraw_events.global_snapshot_hash AS snapshot_hash
   FROM delegate_stake_withdraw_events
UNION ALL
 SELECT metagraph_token_locks.hash,
    metagraph_token_locks.source_addr,
    metagraph_token_locks.amount,
    metagraph_token_locks.created_at,
    metagraph_token_locks.updated_at,
    'metagraph_token_locks'::name AS table_name,
    metagraph_token_locks.snapshot_hash
   FROM metagraph_token_locks
UNION ALL
 SELECT metagraph_token_unlocks.hash,
    metagraph_token_unlocks.source_addr,
    metagraph_token_unlocks.amount,
    metagraph_token_unlocks.created_at,
    metagraph_token_unlocks.updated_at,
    'metagraph_token_unlocks'::name AS table_name,
    metagraph_token_unlocks.snapshot_hash
   FROM metagraph_token_unlocks
UNION ALL
 SELECT metagraph_allow_spends.hash,
    metagraph_allow_spends.source_addr,
    metagraph_allow_spends.amount,
    metagraph_allow_spends.created_at,
    metagraph_allow_spends.updated_at,
    'metagraph_allow_spends'::name AS table_name,
    metagraph_allow_spends.snapshot_hash
   FROM metagraph_allow_spends
UNION ALL
 SELECT metagraph_fee_transactions.hash,
    metagraph_fee_transactions.source_addr,
    metagraph_fee_transactions.amount,
    metagraph_fee_transactions.created_at,
    metagraph_fee_transactions.updated_at,
    'metagraph_fee_transactions'::name AS table_name,
    metagraph_fee_transactions.snapshot_hash
   FROM metagraph_fee_transactions
UNION ALL
 SELECT metagraph_spend_transactions.hash,
    metagraph_spend_transactions.source_addr,
    metagraph_spend_transactions.amount,
    metagraph_spend_transactions.created_at,
    metagraph_spend_transactions.updated_at,
    'metagraph_spend_transactions'::name AS table_name,
    metagraph_spend_transactions.snapshot_hash
   FROM metagraph_spend_transactions
UNION ALL
 SELECT metagraph_expired_spend_transactions.hash,
    metagraph_expired_spend_transactions.source_addr,
    metagraph_expired_spend_transactions.amount,
    metagraph_expired_spend_transactions.created_at,
    metagraph_expired_spend_transactions.updated_at,
    'metagraph_expired_spend_transactions'::name AS table_name,
    metagraph_expired_spend_transactions.snapshot_hash
   FROM metagraph_expired_spend_transactions
UNION ALL
 SELECT metagraph_transactions.hash,
    metagraph_transactions.source_addr,
    metagraph_transactions.amount,
    metagraph_transactions.created_at,
    metagraph_transactions.updated_at,
    'metagraph_transactions'::name AS table_name,
    metagraph_transactions.snapshot_hash
   FROM metagraph_transactions;


-- public.abstract_transactions_view_2 source

CREATE OR REPLACE VIEW abstract_transactions_view_2
AS SELECT dag_spend_transactions.hash,
    dag_spend_transactions.source_addr,
    dag_spend_transactions.amount,
    dag_spend_transactions.created_at,
    dag_spend_transactions.updated_at,
    'dag_spend_transactions'::text AS table_name
   FROM ONLY dag_spend_transactions
UNION ALL
 SELECT dag_expired_spend_transactions.hash,
    dag_expired_spend_transactions.source_addr,
    dag_expired_spend_transactions.amount,
    dag_expired_spend_transactions.created_at,
    dag_expired_spend_transactions.updated_at,
    'dag_expired_spend_transactions'::text AS table_name
   FROM ONLY dag_expired_spend_transactions
UNION ALL
 SELECT dag_token_locks.hash,
    dag_token_locks.source_addr,
    dag_token_locks.amount,
    dag_token_locks.created_at,
    dag_token_locks.updated_at,
    'dag_token_locks'::text AS table_name
   FROM ONLY dag_token_locks
UNION ALL
 SELECT dag_token_unlocks.hash,
    dag_token_unlocks.source_addr,
    dag_token_unlocks.amount,
    dag_token_unlocks.created_at,
    dag_token_unlocks.updated_at,
    'dag_token_unlocks'::text AS table_name
   FROM ONLY dag_token_unlocks
UNION ALL
 SELECT dag_transactions.hash,
    dag_transactions.source_addr,
    dag_transactions.amount,
    dag_transactions.created_at,
    dag_transactions.updated_at,
    'dag_transactions'::text AS table_name
   FROM ONLY dag_transactions
UNION ALL
 SELECT metagraph_token_locks.hash,
    metagraph_token_locks.source_addr,
    metagraph_token_locks.amount,
    metagraph_token_locks.created_at,
    metagraph_token_locks.updated_at,
    'metagraph_token_locks'::text AS table_name
   FROM ONLY metagraph_token_locks
UNION ALL
 SELECT metagraph_token_unlocks.hash,
    metagraph_token_unlocks.source_addr,
    metagraph_token_unlocks.amount,
    metagraph_token_unlocks.created_at,
    metagraph_token_unlocks.updated_at,
    'metagraph_token_unlocks'::text AS table_name
   FROM ONLY metagraph_token_unlocks
UNION ALL
 SELECT metagraph_allow_spends.hash,
    metagraph_allow_spends.source_addr,
    metagraph_allow_spends.amount,
    metagraph_allow_spends.created_at,
    metagraph_allow_spends.updated_at,
    'metagraph_allow_spends'::text AS table_name
   FROM ONLY metagraph_allow_spends
UNION ALL
 SELECT metagraph_fee_transactions.hash,
    metagraph_fee_transactions.source_addr,
    metagraph_fee_transactions.amount,
    metagraph_fee_transactions.created_at,
    metagraph_fee_transactions.updated_at,
    'metagraph_fee_transactions'::text AS table_name
   FROM ONLY metagraph_fee_transactions
UNION ALL
 SELECT metagraph_spend_transactions.hash,
    metagraph_spend_transactions.source_addr,
    metagraph_spend_transactions.amount,
    metagraph_spend_transactions.created_at,
    metagraph_spend_transactions.updated_at,
    'metagraph_spend_transactions'::text AS table_name
   FROM ONLY metagraph_spend_transactions
UNION ALL
 SELECT metagraph_expired_spend_transactions.hash,
    metagraph_expired_spend_transactions.source_addr,
    metagraph_expired_spend_transactions.amount,
    metagraph_expired_spend_transactions.created_at,
    metagraph_expired_spend_transactions.updated_at,
    'metagraph_expired_spend_transactions'::text AS table_name
   FROM ONLY metagraph_expired_spend_transactions
UNION ALL
 SELECT metagraph_transactions.hash,
    metagraph_transactions.source_addr,
    metagraph_transactions.amount,
    metagraph_transactions.created_at,
    metagraph_transactions.updated_at,
    'metagraph_transactions'::text AS table_name
   FROM ONLY metagraph_transactions;


-- public.actions_view source

CREATE OR REPLACE VIEW actions_view
AS SELECT dag_allow_spends.hash,
    dag_allow_spends.source_addr,
    dag_allow_spends.amount,
    dag_allow_spends.created_at,
    dag_allow_spends.updated_at,
    'dag_allow_spends'::text AS table_name,
    dag_allow_spends.snapshot_hash,
    dag_allow_spends.last_valid_epoch_progress + 1 AS unlock_epoch,
    dag_allow_spends.parent_hash,
    dag_allow_spends.fee
   FROM dag_allow_spends
UNION ALL
 SELECT dag_spend_transactions.hash,
    dag_spend_transactions.source_addr,
    dag_spend_transactions.amount,
    dag_spend_transactions.created_at,
    dag_spend_transactions.updated_at,
    'dag_spend_transactions'::text AS table_name,
    dag_spend_transactions.snapshot_hash,
    NULL::bigint AS unlock_epoch,
    dag_spend_transactions.allow_spend_ref AS parent_hash,
    NULL::bigint AS fee
   FROM dag_spend_transactions
UNION ALL
 SELECT dag_expired_spend_transactions.hash,
    dag_expired_spend_transactions.source_addr,
    dag_expired_spend_transactions.amount,
    dag_expired_spend_transactions.created_at,
    dag_expired_spend_transactions.updated_at,
    'dag_expired_spend_transactions'::text AS table_name,
    dag_expired_spend_transactions.snapshot_hash,
    NULL::bigint AS unlock_epoch,
    dag_expired_spend_transactions.allow_spend_ref AS parent_hash,
    NULL::bigint AS fee
   FROM dag_expired_spend_transactions
UNION ALL
 SELECT dag_token_locks.hash,
    dag_token_locks.source_addr,
    dag_token_locks.amount,
    dag_token_locks.created_at,
    dag_token_locks.updated_at,
    'dag_token_locks'::text AS table_name,
    dag_token_locks.snapshot_hash,
    dag_token_locks.unlock_epoch,
    dag_token_locks.parent_hash,
    NULL::bigint AS fee
   FROM dag_token_locks
UNION ALL
 SELECT dag_token_unlocks.hash,
    dag_token_unlocks.source_addr,
    dag_token_unlocks.amount,
    dag_token_unlocks.created_at,
    dag_token_unlocks.updated_at,
    'dag_token_unlocks'::text AS table_name,
    dag_token_unlocks.snapshot_hash,
    NULL::bigint AS unlock_epoch,
    dag_token_unlocks.lock_reference_hash AS parent_hash,
    NULL::bigint AS fee
   FROM dag_token_unlocks
UNION ALL
 SELECT dag_transactions.hash,
    dag_transactions.source_addr,
    dag_transactions.amount,
    dag_transactions.created_at,
    dag_transactions.updated_at,
    'dag_transactions'::text AS table_name,
    dag_transactions.snapshot_hash,
    NULL::bigint AS unlock_epoch,
    dag_transactions.parent_hash,
    dag_transactions.fee
   FROM dag_transactions
UNION ALL
 SELECT delegate_stake_create_events.hash,
    delegate_stake_create_events.source_addr,
    delegate_stake_create_events.amount,
    delegate_stake_create_events.created_at,
    delegate_stake_create_events.updated_at,
    'dag_delegate_stake_create_events'::text AS table_name,
    delegate_stake_create_events.global_snapshot_hash AS snapshot_hash,
    NULL::bigint AS unlock_epoch,
    delegate_stake_create_events.parent_hash,
    delegate_stake_create_events.fee
   FROM delegate_stake_create_events
UNION ALL
 SELECT dswe.hash,
    dswe.source_addr,
    dsce.amount,
    dswe.created_at,
    dswe.updated_at,
    'dag_delegate_stake_withdraw_events'::text AS table_name,
    dswe.global_snapshot_hash AS snapshot_hash,
    dswe.unlock_epoch,
    dswe.stake_create_hash AS parent_hash,
    NULL::bigint AS fee
   FROM delegate_stake_withdraw_events dswe
     LEFT JOIN delegate_stake_create_events dsce ON dswe.stake_create_hash::text = dsce.hash::text
UNION ALL
 SELECT metagraph_token_locks.hash,
    metagraph_token_locks.source_addr,
    metagraph_token_locks.amount,
    metagraph_token_locks.created_at,
    metagraph_token_locks.updated_at,
    'metagraph_token_locks'::text AS table_name,
    metagraph_token_locks.snapshot_hash,
    metagraph_token_locks.unlock_epoch,
    metagraph_token_locks.parent_hash,
    NULL::bigint AS fee
   FROM metagraph_token_locks
UNION ALL
 SELECT metagraph_token_unlocks.hash,
    metagraph_token_unlocks.source_addr,
    metagraph_token_unlocks.amount,
    metagraph_token_unlocks.created_at,
    metagraph_token_unlocks.updated_at,
    'metagraph_token_unlocks'::text AS table_name,
    metagraph_token_unlocks.snapshot_hash,
    NULL::bigint AS unlock_epoch,
    metagraph_token_unlocks.lock_reference_hash AS parent_hash,
    NULL::bigint AS fee
   FROM metagraph_token_unlocks
UNION ALL
 SELECT metagraph_allow_spends.hash,
    metagraph_allow_spends.source_addr,
    metagraph_allow_spends.amount,
    metagraph_allow_spends.created_at,
    metagraph_allow_spends.updated_at,
    'metagraph_allow_spends'::text AS table_name,
    metagraph_allow_spends.snapshot_hash,
    metagraph_allow_spends.last_valid_epoch_progress + 1 AS unlock_epoch,
    metagraph_allow_spends.parent_hash,
    metagraph_allow_spends.fee
   FROM metagraph_allow_spends
UNION ALL
 SELECT metagraph_fee_transactions.hash,
    metagraph_fee_transactions.source_addr,
    metagraph_fee_transactions.amount,
    metagraph_fee_transactions.created_at,
    metagraph_fee_transactions.updated_at,
    'metagraph_fee_transactions'::text AS table_name,
    metagraph_fee_transactions.snapshot_hash,
    NULL::bigint AS unlock_epoch,
    metagraph_fee_transactions.data_update_ref AS parent_hash,
    NULL::bigint AS fee
   FROM metagraph_fee_transactions
UNION ALL
 SELECT metagraph_spend_transactions.hash,
    metagraph_spend_transactions.source_addr,
    metagraph_spend_transactions.amount,
    metagraph_spend_transactions.created_at,
    metagraph_spend_transactions.updated_at,
    'metagraph_spend_transactions'::text AS table_name,
    metagraph_spend_transactions.snapshot_hash,
    NULL::bigint AS unlock_epoch,
    metagraph_spend_transactions.allow_spend_ref AS parent_hash,
    NULL::bigint AS fee
   FROM metagraph_spend_transactions
UNION ALL
 SELECT metagraph_expired_spend_transactions.hash,
    metagraph_expired_spend_transactions.source_addr,
    metagraph_expired_spend_transactions.amount,
    metagraph_expired_spend_transactions.created_at,
    metagraph_expired_spend_transactions.updated_at,
    'metagraph_expired_spend_transactions'::text AS table_name,
    metagraph_expired_spend_transactions.snapshot_hash,
    NULL::bigint AS unlock_epoch,
    metagraph_expired_spend_transactions.allow_spend_ref AS parent_hash,
    NULL::bigint AS fee
   FROM metagraph_expired_spend_transactions
UNION ALL
 SELECT metagraph_transactions.hash,
    metagraph_transactions.source_addr,
    metagraph_transactions.amount,
    metagraph_transactions.created_at,
    metagraph_transactions.updated_at,
    'metagraph_transactions'::text AS table_name,
    metagraph_transactions.snapshot_hash,
    NULL::bigint AS unlock_epoch,
    metagraph_transactions.parent_hash,
    metagraph_transactions.fee
   FROM metagraph_transactions;


-- public.dag_actions_view source

CREATE OR REPLACE VIEW dag_actions_view
AS SELECT das.hash,
    das.source_addr,
    das.amount,
    das.created_at,
    das.updated_at,
    'AllowSpend'::text AS transaction_type,
    das.snapshot_hash AS global_snapshot_hash,
    gs.ordinal AS global_snapshot_ordinal,
    das.destination_addr,
    das.last_valid_epoch_progress AS unlock_epoch,
    das.parent_hash,
    das.fee,
    das.currency_id
   FROM dag_allow_spends das
     JOIN global_snapshots gs ON das.snapshot_hash::text = gs.hash::text
UNION ALL
 SELECT dst.hash,
    dst.source_addr,
    dst.amount,
    dst.created_at,
    dst.updated_at,
    'SpendTransaction'::text AS transaction_type,
    dst.snapshot_hash AS global_snapshot_hash,
    gs.ordinal AS global_snapshot_ordinal,
    dst.destination_addr,
    NULL::bigint AS unlock_epoch,
    dst.allow_spend_ref AS parent_hash,
    NULL::bigint AS fee,
    dst.currency_id
   FROM dag_spend_transactions dst
     JOIN global_snapshots gs ON dst.snapshot_hash::text = gs.hash::text
UNION ALL
 SELECT dest.hash,
    dest.source_addr,
    dest.amount,
    dest.created_at,
    dest.updated_at,
    'ExpiredAllowSpend'::text AS transaction_type,
    dest.snapshot_hash AS global_snapshot_hash,
    gs.ordinal AS global_snapshot_ordinal,
    NULL::character varying AS destination_addr,
    NULL::bigint AS unlock_epoch,
    dest.allow_spend_ref AS parent_hash,
    NULL::bigint AS fee,
    dest.currency_id
   FROM dag_expired_spend_transactions dest
     JOIN global_snapshots gs ON dest.snapshot_hash::text = gs.hash::text
UNION ALL
 SELECT dtl.hash,
    dtl.source_addr,
    dtl.amount,
    dtl.created_at,
    dtl.updated_at,
    'TokenLock'::text AS transaction_type,
    dtl.snapshot_hash AS global_snapshot_hash,
    gs.ordinal AS global_snapshot_ordinal,
    NULL::character varying AS destination_addr,
    dtl.unlock_epoch,
    dtl.parent_hash,
    NULL::bigint AS fee,
    dtl.currency_id
   FROM dag_token_locks dtl
     JOIN global_snapshots gs ON dtl.snapshot_hash::text = gs.hash::text
UNION ALL
 SELECT dtu.hash,
    dtu.source_addr,
    dtu.amount,
    dtu.created_at,
    dtu.updated_at,
    'TokenUnlock'::text AS transaction_type,
    dtu.snapshot_hash AS global_snapshot_hash,
    gs.ordinal AS global_snapshot_ordinal,
    NULL::character varying AS destination_addr,
    NULL::bigint AS unlock_epoch,
    dtu.lock_reference_hash AS parent_hash,
    NULL::bigint AS fee,
    dtu.currency_id
   FROM dag_token_unlocks dtu
     JOIN global_snapshots gs ON dtu.snapshot_hash::text = gs.hash::text
UNION ALL
 SELECT dsce.hash,
    dsce.source_addr,
    dsce.amount,
    dsce.created_at,
    dsce.updated_at,
    'DelegateStakeCreate'::text AS transaction_type,
    dsce.global_snapshot_hash,
    gs.ordinal AS global_snapshot_ordinal,
    NULL::character varying AS destination_addr,
    NULL::bigint AS unlock_epoch,
    dsce.parent_hash,
    dsce.fee,
    NULL::character varying AS currency_id
   FROM delegate_stake_create_events dsce
     JOIN global_snapshots gs ON dsce.global_snapshot_hash::text = gs.hash::text
UNION ALL
 SELECT dswe.hash,
    dswe.source_addr,
    dsce.amount,
    dswe.created_at,
    dswe.updated_at,
    'DelegateStakeWithdraw'::text AS transaction_type,
    dswe.global_snapshot_hash,
    gs.ordinal AS global_snapshot_ordinal,
    NULL::character varying AS destination_addr,
    dswe.unlock_epoch,
    dswe.stake_create_hash AS parent_hash,
    NULL::bigint AS fee,
    NULL::character varying AS currency_id
   FROM delegate_stake_withdraw_events dswe
     LEFT JOIN delegate_stake_create_events dsce ON dswe.stake_create_hash::text = dsce.hash::text
     JOIN global_snapshots gs ON dswe.global_snapshot_hash::text = gs.hash::text;


-- public.delegate_stake_create_events_latest_view source

CREATE OR REPLACE VIEW delegate_stake_create_events_latest_view
AS SELECT DISTINCT ON (source_addr, node_id) hash,
    ordinal,
    source_addr,
    node_id,
    amount,
    fee,
    lock_reference_hash,
    parent_hash,
    global_snapshot_hash,
    transfer_from_hash,
    created_at,
    updated_at
   FROM delegate_stake_create_events
  ORDER BY source_addr, node_id, ordinal DESC;


-- public.delegate_stake_total_rewards_view source

CREATE OR REPLACE VIEW delegate_stake_total_rewards_view
AS SELECT stake_create_hash,
    sum(rewards) AS delegate_stake_total_rewards
   FROM delegate_stake_rewards
  GROUP BY stake_create_hash;


-- public.metagraph_actions_view source

CREATE OR REPLACE VIEW metagraph_actions_view
AS SELECT mas.metagraph_id,
    mas.hash,
    mas.source_addr,
    mas.amount,
    mas.created_at,
    mas.updated_at,
    'AllowSpend'::text AS transaction_type,
    mas.snapshot_hash AS metagraph_snapshot_hash,
    ms.ordinal AS metagraph_snapshot_ordinal,
    mas.destination_addr,
    mas.last_valid_epoch_progress AS unlock_epoch,
    mas.parent_hash,
    mas.fee,
    mas.currency_id,
    gs.hash AS global_snapshot_hash,
    gs.ordinal AS global_snapshot_ordinal
   FROM metagraph_allow_spends mas
     JOIN metagraph_snapshots ms ON mas.metagraph_id::text = ms.metagraph_id::text AND mas.snapshot_hash::text = ms.hash::text
     JOIN global_snapshots gs ON ms.global_snapshot_hash::text = gs.hash::text
UNION ALL
 SELECT mst.metagraph_id,
    mst.hash,
    mst.source_addr,
    mst.amount,
    mst.created_at,
    mst.updated_at,
    'SpendTransaction'::text AS transaction_type,
    mst.snapshot_hash AS metagraph_snapshot_hash,
    ms.ordinal AS metagraph_snapshot_ordinal,
    mst.destination_addr,
    NULL::bigint AS unlock_epoch,
    mst.allow_spend_ref AS parent_hash,
    NULL::bigint AS fee,
    mst.currency_id,
    gs.hash AS global_snapshot_hash,
    gs.ordinal AS global_snapshot_ordinal
   FROM metagraph_spend_transactions mst
     JOIN metagraph_snapshots ms ON mst.metagraph_id::text = ms.metagraph_id::text AND mst.snapshot_hash::text = ms.hash::text
     JOIN global_snapshots gs ON ms.global_snapshot_hash::text = gs.hash::text
UNION ALL
 SELECT mest.metagraph_id,
    mest.hash,
    mest.source_addr,
    mest.amount,
    mest.created_at,
    mest.updated_at,
    'ExpiredAllowSpend'::text AS transaction_type,
    mest.snapshot_hash AS metagraph_snapshot_hash,
    ms.ordinal AS metagraph_snapshot_ordinal,
    NULL::character varying AS destination_addr,
    NULL::bigint AS unlock_epoch,
    mest.allow_spend_ref AS parent_hash,
    NULL::bigint AS fee,
    mest.currency_id,
    gs.hash AS global_snapshot_hash,
    gs.ordinal AS global_snapshot_ordinal
   FROM metagraph_expired_spend_transactions mest
     JOIN metagraph_snapshots ms ON mest.metagraph_id::text = ms.metagraph_id::text AND mest.snapshot_hash::text = ms.hash::text
     JOIN global_snapshots gs ON ms.global_snapshot_hash::text = gs.hash::text
UNION ALL
 SELECT mtl.metagraph_id,
    mtl.hash,
    mtl.source_addr,
    mtl.amount,
    mtl.created_at,
    mtl.updated_at,
    'TokenLock'::text AS transaction_type,
    mtl.snapshot_hash AS metagraph_snapshot_hash,
    ms.ordinal AS metagraph_snapshot_ordinal,
    NULL::character varying AS destination_addr,
    mtl.unlock_epoch,
    mtl.parent_hash,
    NULL::bigint AS fee,
    mtl.currency_id,
    gs.hash AS global_snapshot_hash,
    gs.ordinal AS global_snapshot_ordinal
   FROM metagraph_token_locks mtl
     JOIN metagraph_snapshots ms ON mtl.metagraph_id::text = ms.metagraph_id::text AND mtl.snapshot_hash::text = ms.hash::text
     JOIN global_snapshots gs ON ms.global_snapshot_hash::text = gs.hash::text
UNION ALL
 SELECT mtu.metagraph_id,
    mtu.hash,
    mtu.source_addr,
    mtu.amount,
    mtu.created_at,
    mtu.updated_at,
    'TokenUnlock'::text AS transaction_type,
    mtu.snapshot_hash AS metagraph_snapshot_hash,
    ms.ordinal AS metagraph_snapshot_ordinal,
    NULL::character varying AS destination_addr,
    NULL::bigint AS unlock_epoch,
    mtu.lock_reference_hash AS parent_hash,
    NULL::bigint AS fee,
    mtu.currency_id,
    gs.hash AS global_snapshot_hash,
    gs.ordinal AS global_snapshot_ordinal
   FROM metagraph_token_unlocks mtu
     JOIN metagraph_snapshots ms ON mtu.metagraph_id::text = ms.metagraph_id::text AND mtu.snapshot_hash::text = ms.hash::text
     JOIN global_snapshots gs ON ms.global_snapshot_hash::text = gs.hash::text
UNION ALL
 SELECT mft.metagraph_id,
    mft.hash,
    mft.source_addr,
    mft.amount,
    mft.created_at,
    mft.updated_at,
    'FeeTransaction'::text AS transaction_type,
    mft.metagraph_snapshot_hash,
    ms.ordinal AS metagraph_snapshot_ordinal,
    mft.destination_addr,
    NULL::bigint AS unlock_epoch,
    mft.data_update_ref AS parent_hash,
    NULL::bigint AS fee,
    NULL::character varying AS currency_id,
    gs.hash AS global_snapshot_hash,
    gs.ordinal AS global_snapshot_ordinal
   FROM metagraph_fee_transactions mft
     JOIN metagraph_snapshots ms ON mft.metagraph_id::text = ms.metagraph_id::text AND mft.metagraph_snapshot_hash::text = ms.hash::text
     JOIN global_snapshots gs ON ms.global_snapshot_hash::text = gs.hash::text;


-- public.token_lock_total_rewards_view source

CREATE OR REPLACE VIEW token_lock_total_rewards_view
AS SELECT dsce.lock_reference_hash,
    sum(dsr.rewards) AS total_rewards
   FROM delegate_stake_rewards dsr,
    delegate_stake_create_events dsce,
    delegate_stake_withdraw_events dswe
  WHERE dsce.hash::text = dsr.stake_create_hash::text AND dsce.hash::text = dswe.stake_create_hash::text AND dswe.is_completed
  GROUP BY dsce.lock_reference_hash;



-- DROP FUNCTION public.batch_set_dag_tx_snapshot_ordinal();

CREATE OR REPLACE FUNCTION public.batch_set_dag_tx_snapshot_ordinal()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  UPDATE dag_transactions tx
  SET snapshot_ordinal = s.ordinal
  FROM global_snapshots s
  WHERE tx.snapshot_hash = s.hash
    AND tx.snapshot_ordinal IS NULL;

  RETURN NULL;
END;
$function$
;

-- DROP FUNCTION public.batch_set_mg_tx_snapshot_ordinal();

CREATE OR REPLACE FUNCTION public.batch_set_mg_tx_snapshot_ordinal()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  UPDATE metagraph_transactions tx
  SET snapshot_ordinal = s.ordinal
  FROM metagraph_snapshots s
  WHERE tx.snapshot_hash = s.hash
    AND tx.snapshot_ordinal IS NULL;

  RETURN NULL;
END;
$function$
;

-- DROP PROCEDURE public.delete_redundant_metagraph_balances_filtered(text, text);

CREATE OR REPLACE PROCEDURE public.delete_redundant_metagraph_balances_filtered(IN in_address text DEFAULT NULL::text, IN in_metagraph_id text DEFAULT NULL::text)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  addr TEXT;
  mg_id TEXT;
  deleted_count INT;
  total_deleted INT;
BEGIN
  FOR addr IN
    SELECT address
    FROM addresses
    WHERE in_address IS NULL OR address = in_address
  LOOP
    RAISE NOTICE 'Processing address: %', addr;

    FOR mg_id IN
      SELECT id
      FROM metagraphs
      WHERE in_metagraph_id IS NULL OR id = in_metagraph_id
    LOOP
      RAISE NOTICE '  → metagraph: %', mg_id;
      total_deleted := 0;

      LOOP
        DELETE FROM metagraph_balance_changes mbc
        USING (
          SELECT metagraph_id, address, metagraph_snapshot_ordinal
          FROM (
            SELECT
              metagraph_id,
              address,
              metagraph_snapshot_ordinal,
              balance,
              LAG(balance) OVER (
                PARTITION BY metagraph_id, address
                ORDER BY metagraph_snapshot_ordinal
              ) AS previous_balance
            FROM metagraph_balance_changes
            WHERE metagraph_id = mg_id AND address = addr
          ) sub
          WHERE balance = previous_balance
          LIMIT 100000
        ) r
        WHERE mbc.metagraph_id = r.metagraph_id
          AND mbc.address = r.address
          AND mbc.metagraph_snapshot_ordinal = r.metagraph_snapshot_ordinal;

        GET DIAGNOSTICS deleted_count = ROW_COUNT;

        IF deleted_count = 0 THEN
          RAISE NOTICE '    ✔ Done for %/% (total deleted: %)', addr, mg_id, total_deleted;
          EXIT;
        END IF;

        total_deleted := total_deleted + deleted_count;
        RAISE NOTICE '    ⏳ Deleted %, running total: %', deleted_count, total_deleted;
      END LOOP;

      COMMIT;
    END LOOP;
  END LOOP;

  RAISE NOTICE 'Finished cleanup for filtered scope.';
END $procedure$
;

-- DROP FUNCTION public.insert_into_parent_abstract_blocks();

CREATE OR REPLACE FUNCTION public.insert_into_parent_abstract_blocks()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    INSERT INTO abstract_blocks (hash, height, created_at)
    VALUES (NEW.hash, NEW.height, NEW.created_at)
    ON CONFLICT (hash) DO NOTHING;
    RETURN NEW;
END;
$function$
;

-- DROP FUNCTION public.insert_into_parent_abstract_transactions();

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
$function$
;

-- DROP FUNCTION public.insert_into_parent_abstract_transactions_from_block();

CREATE OR REPLACE FUNCTION public.insert_into_parent_abstract_transactions_from_block()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    snap_hash varchar;
BEGIN

    INSERT INTO abstract_transactions (hash, source_addr, amount, created_at, snapshot_hash)
    VALUES (NEW.hash, NEW.source_addr, NEW.amount, NEW.created_at, NEW.snapshot_hash)
    ON CONFLICT (hash) DO NOTHING;

    RETURN NEW;
END;
$function$
;

-- DROP FUNCTION public.insert_into_parent_abstract_transactions_from_metagraph_block();

CREATE OR REPLACE FUNCTION public.insert_into_parent_abstract_transactions_from_metagraph_block()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    snap_hash varchar;
BEGIN

    INSERT INTO abstract_transactions (hash, source_addr, amount, created_at, snapshot_hash)
    VALUES (NEW.hash, NEW.source_addr, NEW.amount, NEW.created_at, NEW.snapshot_hash)
    ON CONFLICT (hash) DO NOTHING;

    RETURN NEW;
END;
$function$
;

-- DROP PROCEDURE public.update_dag_snapshot_ordinal_in_batches(int4);

CREATE OR REPLACE PROCEDURE public.update_dag_snapshot_ordinal_in_batches(IN batch_size integer DEFAULT 10000)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
  rows_updated INTEGER := 0;
BEGIN
  LOOP
    WITH cte AS (
      SELECT tx.ctid
      FROM dag_transactions tx
      JOIN global_snapshots s ON tx.snapshot_hash = s.hash
      WHERE tx.snapshot_ordinal IS NULL
      LIMIT batch_size
    )
    UPDATE dag_transactions tx
    SET snapshot_ordinal = s.ordinal
    FROM global_snapshots s
    WHERE tx.ctid IN (SELECT ctid FROM cte)
      AND tx.snapshot_hash = s.hash;

    GET DIAGNOSTICS rows_updated = ROW_COUNT;

    RAISE NOTICE 'Updated % rows in this batch.', rows_updated;

    COMMIT;  -- ✅ Intermediate commit after each batch

    EXIT WHEN rows_updated = 0;
    -- Optional: PERFORM pg_sleep(0.1);
  END LOOP;

  RAISE NOTICE 'Finished updating snapshot_ordinal in dag_transactions.';
END;
$procedure$
;

-- DROP FUNCTION public.update_updated_at_column();

CREATE OR REPLACE FUNCTION public.update_updated_at_column()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$function$
;