
CREATE TABLE delegate_stake_create_events (
    hash varchar PRIMARY KEY,
    ordinal int8 NOT NULL,
    source_addr varchar NOT NULL REFERENCES addresses(address) ON DELETE CASCADE,
    node_id varchar NOT NULL,
    amount int8 NOT NULL,
    fee int8 NOT NULL DEFAULT 0,
    lock_reference_hash varchar NOT NULL REFERENCES dag_token_locks(hash) ON DELETE CASCADE,
    parent_hash varchar NOT NULL,
    global_snapshot_hash varchar NOT NULL REFERENCES global_snapshots(hash) ON DELETE CASCADE,
    is_update boolean NOT NULL,
    created_at timestamp DEFAULT now() NOT NULL,
    updated_at timestamp DEFAULT now() NOT NULL,
    current_token_lock_hash varchar NULL,
    current_amount int8 NULL
);
CREATE INDEX delegate_stake_create_events_source_addr_idx ON public.delegate_stake_create_events USING btree (source_addr);
CREATE INDEX delegate_stake_create_events_lock_reference_hash_idx ON public.delegate_stake_create_events USING btree (lock_reference_hash);
CREATE INDEX delegate_stake_create_events_global_snapshot_hash_idx ON public.delegate_stake_create_events USING btree (global_snapshot_hash);
CREATE INDEX delegate_stake_create_events_current_token_lock_hash_idx ON public.delegate_stake_create_events USING btree (current_token_lock_hash);



CREATE TABLE delegate_stake_withdraw_events (
    hash varchar PRIMARY KEY,
    source_addr varchar NOT NULL REFERENCES addresses(address) ON DELETE CASCADE,
    stake_create_hash varchar NOT NULL REFERENCES delegate_stake_create_events(hash) ON DELETE CASCADE,
    global_snapshot_hash varchar NOT NULL REFERENCES global_snapshots(hash) ON DELETE CASCADE,
    created_at timestamp DEFAULT now() NOT NULL,
    updated_at timestamp DEFAULT now() NOT NULL,
    current_token_lock_hash varchar NULL,
    current_amount int8 NULL
);
CREATE INDEX delegate_stake_withdraw_events_source_addr_idx ON public.delegate_stake_withdraw_events USING btree (source_addr);
CREATE INDEX delegate_stake_withdraw_events_stake_create_hash_idx ON public.delegate_stake_withdraw_events USING btree (stake_create_hash);
CREATE INDEX delegate_stake_withdraw_events_global_snapshot_hash_idx ON public.delegate_stake_withdraw_events USING btree (global_snapshot_hash);


CREATE TABLE delegate_stake_balance_changes (
	global_snapshot_hash varchar NOT NULL REFERENCES global_snapshots(hash) ON DELETE CASCADE,
	global_snapshot_ordinal int8 NOT NULL REFERENCES global_snapshots(ordinal) ON DELETE CASCADE,
	address varchar NOT NULL REFERENCES addresses(address) ON DELETE CASCADE,
	node_id varchar NOT NULL,
	balance int8 NOT NULL,
	rewards int8 NOT NULL,
	stake_create_hash varchar NULL REFERENCES delegate_stake_create_events(hash) ON DELETE CASCADE,
	stake_withdraw_hash varchar NULL REFERENCES delegate_stake_withdraw_events(hash) ON DELETE CASCADE,
	created_at timestamp DEFAULT now() NOT NULL,
	updated_at timestamp DEFAULT now() NOT NULL
);
CREATE INDEX delegate_stake_balance_changes_global_snapshot_hash_idx ON public.delegate_stake_balance_changes USING btree (global_snapshot_hash);
CREATE INDEX delegate_stake_balance_changes_global_snapshot_ordinal_idx ON public.delegate_stake_balance_changes USING btree (global_snapshot_ordinal);
CREATE INDEX delegate_stake_balance_changes_address ON public.delegate_stake_balance_changes USING btree (address);
CREATE INDEX delegate_stake_balance_changes_node_id ON public.delegate_stake_balance_changes USING btree (node_id);
ALTER TABLE public.delegate_stake_balance_changes ADD CONSTRAINT delegate_stake_balance_changes_unique UNIQUE (global_snapshot_hash, address, node_id, balance, rewards);


CREATE TABLE delegate_stake_rewards (
	global_snapshot_hash varchar NOT NULL REFERENCES global_snapshots(hash) ON DELETE CASCADE,
	address varchar NOT NULL REFERENCES addresses(address) ON DELETE CASCADE,
	node_id varchar NOT NULL,
	rewards int8 NOT NULL,
	created_at timestamp DEFAULT now() NOT NULL,
	updated_at timestamp DEFAULT now() NOT NULL
);
CREATE INDEX delegate_stake_rewards_changes_global_snapshot_hash_idx ON public.delegate_stake_balance_changes USING btree (global_snapshot_hash);
CREATE INDEX delegate_stake_rewards_changes_address ON public.delegate_stake_balance_changes USING btree (address);
CREATE INDEX delegate_stake_rewards_changes_node_id ON public.delegate_stake_balance_changes USING btree (node_id);
ALTER TABLE public.delegate_stake_rewards ADD CONSTRAINT delegate_stake_rewards_changes_unique UNIQUE (global_snapshot_hash, address, node_id, rewards);
