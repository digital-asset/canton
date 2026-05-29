drop view debug.ord_availability_batch;
alter table ord_availability_batch rename to ord_availability_batch_old;

create table ord_availability_batch (
  id varchar collate "C" not null,
  batch bytea not null,
  -- assigned at batch creation and used to calculate epoch expiration
  epoch_number bigint not null,
  primary key (epoch_number, id)
);

insert into ord_availability_batch (id, batch, epoch_number)
select id, batch, epoch_number
from ord_availability_batch_old;

drop table ord_availability_batch_old;
create or replace view debug.ord_availability_batch as
select
    id,
    batch,
    epoch_number
from ord_availability_batch;
