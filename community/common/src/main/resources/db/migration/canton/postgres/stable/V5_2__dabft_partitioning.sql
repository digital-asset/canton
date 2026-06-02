drop view debug.ord_epochs;
drop view debug.ord_availability_batch;
drop view debug.ord_pbft_messages_in_progress;
drop view debug.ord_pbft_messages_completed;
drop view debug.ord_metadata_output_blocks;
drop view debug.ord_metadata_output_epochs;
drop view debug.ord_leader_selection_state;

alter table ord_epochs rename to ord_epochs_old;
alter table ord_availability_batch rename to ord_availability_batch_old;
alter table ord_pbft_messages_in_progress rename to ord_pbft_messages_in_progress_old;
alter table ord_pbft_messages_completed rename to ord_pbft_messages_completed_old;
alter table ord_metadata_output_blocks rename to ord_metadata_output_blocks_old;
alter table ord_metadata_output_epochs rename to ord_metadata_output_epochs_old;
alter table ord_leader_selection_state rename to ord_leader_selection_state_old;

-- Stores metadata for epochs
-- Individual blocks/transactions exist in separate table
create table ord_epochs (
  -- strictly-increasing, contiguous epoch number
  epoch_number bigint not null primary key,
  -- first block sequence number (globally) of the epoch
  start_block_number bigint not null,
  -- number of total blocks in the epoch
  epoch_length bigint not null,
  -- Sequencing instant of the topology snapshot in force for the epoch
  topology_ts bigint not null,
  -- whether the epoch is in progress
  in_progress bool not null
) partition by range (epoch_number);

create table ord_availability_batch (
  id varchar collate "C" not null,
  batch bytea not null,
  -- assigned at batch creation and used to calculate epoch expiration
  epoch_number bigint not null,
  primary key (epoch_number, id)
) partition by range (epoch_number);

-- messages stored during the progress of a block possibly across different pbft views
create table ord_pbft_messages_in_progress(
  -- global sequence number of the ordered block
  block_number bigint not null,

  -- epoch number of the block
  epoch_number bigint not null,

  -- view number
  view_number bigint not null,

  -- pbft message for the block
  message bytea not null,

  -- pbft message discriminator (0 = pre-prepare, 1 = prepare, 2 = commit)
  discriminator smallint not null,

  -- sender of the message
  from_sequencer_id varchar collate "C" not null,

  -- for each block number, we only expect one message of each kind for the same sender and view number.
  primary key (epoch_number, block_number, view_number, from_sequencer_id, discriminator)
) partition by range (epoch_number);

create table ord_pbft_messages_completed(
  -- global sequence number of the ordered block
  block_number bigint not null,

  -- epoch number of the block
  epoch_number bigint not null,

  -- pbft message for the block
  message bytea not null,

  -- pbft message discriminator (0 = pre-prepare, 2 = commit)
  discriminator smallint not null,

  -- sender of the message
  from_sequencer_id varchar collate "C" not null,

  -- for each completed block number, we only expect one message of each kind for the same sender.
  -- in the case of pre-prepare, we only expect one message for the whole block, but for simplicity
  -- we won't differentiate that at the database level.
  primary key (block_number, epoch_number, from_sequencer_id, discriminator)
) partition by range (epoch_number);

-- Stores metadata for blocks that have been assigned timestamps in the output module
create table ord_metadata_output_blocks (
  epoch_number bigint not null,
  block_number bigint not null,
  bft_ts bigint not null,
  primary key (epoch_number, block_number)
) partition by range (epoch_number);

-- Stores output metadata for epochs
create table ord_metadata_output_epochs (
  epoch_number bigint not null primary key,
  could_alter_ordering_topology bool not null
) partition by range (epoch_number);

create table ord_leader_selection_state (
    epoch_number bigint not null primary key,
    state bytea not null
) partition by range (epoch_number);

create function create_partitions_from_table(
  parent_table regclass,
  source_table regclass
) returns void as $$
declare
  partition_size bigint := ${initialBftOrdererTablesPartitionSize};
  min_val bigint;
  max_val bigint;
  current_val bigint;
  from_val bigint;
  next_val bigint;
  final_val bigint;
  partition_index bigint;
  partition_name text;
begin
  if partition_size <= 0 then
    raise exception 'partition_size must be positive';
  end if;

  execute format(
    'select min(epoch_number), max(epoch_number) from %s',
    source_table
  )
  into min_val, max_val;

  if min_val is null or max_val is null then
    min_val := 0;
    max_val := 0;
  end if;

  current_val := floor(min_val::numeric / partition_size)::bigint * partition_size;

  -- creates one extra partition after the partition containing max_val
  final_val := max_val + partition_size;

  while current_val <= final_val loop
    next_val := current_val + partition_size;
    partition_index := current_val / partition_size;
    partition_name := format(
      '%s_p%s',
      replace(parent_table::text, '.', '_'),
      partition_index
    );

    -- the first partition will start at -1
    if current_val = 0 then
      from_val := -1;
    else
      from_val := current_val;
    end if;

    execute format(
      'create table if not exists %I partition of %s for values from (%L) to (%L)',
      partition_name,
      parent_table,
      from_val,
      next_val
    );

    current_val := next_val;
  end loop;
end;
$$ language plpgsql;

select create_partitions_from_table('ord_epochs'::regclass,
                                    'ord_epochs_old'::regclass);
select create_partitions_from_table('ord_availability_batch'::regclass,
                                    'ord_availability_batch_old'::regclass);
select create_partitions_from_table('ord_pbft_messages_in_progress'::regclass,
                                    'ord_pbft_messages_in_progress_old'::regclass);
select create_partitions_from_table('ord_pbft_messages_completed'::regclass,
                                    'ord_pbft_messages_completed_old'::regclass);
select create_partitions_from_table('ord_metadata_output_blocks'::regclass,
                                    'ord_metadata_output_blocks_old'::regclass);
select create_partitions_from_table('ord_metadata_output_epochs'::regclass,
                                    'ord_metadata_output_epochs_old'::regclass);
select create_partitions_from_table('ord_leader_selection_state'::regclass,
                                    'ord_leader_selection_state_old'::regclass);
drop function create_partitions_from_table(regclass, regclass);

insert into ord_epochs (epoch_number, start_block_number, epoch_length, topology_ts, in_progress)
select epoch_number, start_block_number, epoch_length, topology_ts, in_progress
from ord_epochs_old;

insert into ord_availability_batch (id, batch, epoch_number)
select id, batch, epoch_number
from ord_availability_batch_old;

insert into ord_pbft_messages_in_progress (block_number, epoch_number, view_number, message, discriminator, from_sequencer_id)
select block_number, epoch_number, view_number, message, discriminator, from_sequencer_id
from ord_pbft_messages_in_progress_old;

insert into ord_pbft_messages_completed (block_number, epoch_number, message, discriminator, from_sequencer_id)
select block_number, epoch_number, message, discriminator, from_sequencer_id
from ord_pbft_messages_completed_old;

insert into ord_metadata_output_blocks (epoch_number, block_number, bft_ts)
select epoch_number, block_number, bft_ts
from ord_metadata_output_blocks_old;

insert into ord_metadata_output_epochs (epoch_number, could_alter_ordering_topology)
select epoch_number, could_alter_ordering_topology
from ord_metadata_output_epochs_old;

insert into ord_leader_selection_state (epoch_number, state)
select epoch_number, state
from ord_leader_selection_state_old;

drop table ord_epochs_old;
drop table ord_availability_batch_old;
drop table ord_pbft_messages_in_progress_old;
drop table ord_pbft_messages_completed_old;
drop table ord_metadata_output_blocks_old;
drop table ord_metadata_output_epochs_old;
drop table ord_leader_selection_state_old;

create table ord_partition_size_history (
    epoch_number     bigint not null primary key,
    partition_number bigint not null,
    partition_size   integer not null
);
insert into ord_partition_size_history(epoch_number, partition_number, partition_size)
    values (0, 0, ${initialBftOrdererTablesPartitionSize});

create or replace view debug.ord_epochs as
select
    epoch_number,
    start_block_number,
    epoch_length,
    debug.canton_timestamp(topology_ts) as topology_ts,
    in_progress
from ord_epochs;

create or replace view debug.ord_availability_batch as
select
    id,
    batch,
    epoch_number
from ord_availability_batch;

create or replace view debug.ord_pbft_messages_in_progress as
select
    block_number,
    epoch_number,
    view_number,
    message,
    discriminator,
    from_sequencer_id
from ord_pbft_messages_in_progress;

create or replace view debug.ord_pbft_messages_completed as
select
    block_number,
    epoch_number,
    message,
    discriminator,
    from_sequencer_id
from ord_pbft_messages_completed;

create or replace view debug.ord_metadata_output_blocks as
select
    epoch_number,
    block_number,
    debug.canton_timestamp(bft_ts) as bft_ts
from ord_metadata_output_blocks;

create or replace view debug.ord_metadata_output_epochs as
select
    epoch_number,
    could_alter_ordering_topology
from ord_metadata_output_epochs;

create or replace view debug.ord_leader_selection_state as
select
    epoch_number,
    state
from ord_leader_selection_state;

create or replace view debug.ord_partition_size_history as
select
    epoch_number,
    partition_number,
    partition_size
from ord_partition_size_history;
