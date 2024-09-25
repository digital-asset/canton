ALTER TABLE sequencer_counter_checkpoints DROP CONSTRAINT sequencer_counter_checkpoints_pkey;
ALTER TABLE sequencer_counter_checkpoints ADD PRIMARY KEY (member, counter, ts);
