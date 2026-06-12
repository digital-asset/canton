drop table ord_availability_batch;
create table ord_availability_batch(
  id varchar not null,
  batch binary large object not null,
  -- assigned at batch creation and used to calculate epoch expiration
  epoch_number bigint not null,
  primary key (epoch_number, id)
);
