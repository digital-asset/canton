// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.syntax.option._
import cats.data.NonEmptyList
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.time.NonNegativeFiniteDuration

sealed trait CommitMode {
  private[sequencer] val postgresSettings: NonEmptyList[String]
}

object CommitMode {

  /** Synchronously commit to local and replicas (in psql this means synchronous_commit='on' or 'remote_write' and that synchronous_standby_names have been appropriately set) */
  case object Synchronous extends CommitMode {
    override private[sequencer] val postgresSettings = NonEmptyList.of("on", "remote_write")
  }

  /** Synchronously commit to the local database alone (in psql this means synchronous_commit='local') */
  case object Local extends CommitMode {
    override private[sequencer] val postgresSettings = NonEmptyList.of("local")
  }

  /** The default commit mode we expect a sequencer to be run in. */
  val Default = CommitMode.Synchronous
}

/** Configuration for the database based sequencer writer
  * @param payloadQueueSize how many payloads should be held in memory while waiting for them to be flushed to the db.
  *                         if new deliver events with payloads are requested when this queue is full the send will
  *                         return a overloaded error and reject the request.
  * @param payloadWriteBatchMaxSize max payload batch size to flush to the database.
  *                                 will trigger a write when this batch size is reached.
  * @param payloadWriteBatchMaxDuration max duration to collect payloads for a batch before triggering a write if
  *                                     payloadWriteBatchMaxSize is not hit first.
  * @param payloadWriteMaxConcurrency limit how many payload batches can be written concurrently.
  * @param eventWriteBatchMaxSize max event batch size to flush to the database.
  * @param eventWriteBatchMaxDuration max duration to collect events for a batch before triggering a write.
  * @param commitMode optional commit mode that if set will be validated to ensure that the connection/db settings have been configured. Defaults to [[CommitMode.Synchronous]].
  * @param maxSqlInListSize will limit the number of items in a SQL in clause. useful for databases where this may have a low limit (e.g. Oracle).
  */
sealed trait SequencerWriterConfig {
  val payloadQueueSize: Int
  val payloadWriteBatchMaxSize: Int
  val payloadWriteBatchMaxDuration: NonNegativeFiniteDuration
  val payloadWriteMaxConcurrency: Int
  val payloadToEventBound: NonNegativeFiniteDuration
  val eventWriteBatchMaxSize: Int
  val eventWriteBatchMaxDuration: NonNegativeFiniteDuration
  val commitModeValidation: Option[CommitMode]
  val maxSqlInListSize: PositiveNumeric[Int]
}

/** Expose config as different named versions using different default values to allow easy switching for the different
  * setups we can run in (high-throughput, low-latency). However as each value is only a default so they can also be easily
  * overridden if required.
  */
object SequencerWriterConfig {
  val DefaultPayloadTimestampBound: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(60L)
  // the Oracle limit is likely 1000 however this is currently only used for payload lookups on conflicts (savePayloads)
  // so just set a bit above the default max payload batch size (50)
  val DefaultMaxSqlInListSize: PositiveNumeric[Int] = PositiveNumeric.tryCreate(250)

  /** Use to have events immediately flushed to the database. Useful for decreasing latency however at a high throughput
    * a large number of writes will be detrimental for performance.
    */
  case class LowLatency(
      override val payloadQueueSize: Int = 1000,
      override val payloadWriteBatchMaxSize: Int = 1,
      override val payloadWriteBatchMaxDuration: NonNegativeFiniteDuration =
        NonNegativeFiniteDuration.ofMillis(10),
      override val payloadWriteMaxConcurrency: Int = 2,
      override val payloadToEventBound: NonNegativeFiniteDuration = DefaultPayloadTimestampBound,
      override val eventWriteBatchMaxSize: Int = 1,
      override val eventWriteBatchMaxDuration: NonNegativeFiniteDuration =
        NonNegativeFiniteDuration.ofMillis(20),
      override val commitModeValidation: Option[CommitMode] = CommitMode.Default.some,
      override val maxSqlInListSize: PositiveNumeric[Int] = DefaultMaxSqlInListSize,
  ) extends SequencerWriterConfig

  /** Creates batches of incoming events to minimize the number of writes to the database. Useful for a high throughput
    * usecase when batches will be quickly filled and written. Will be detrimental for latency if used and a lower throughput
    * of events causes writes to always be delayed to the batch max duration.
    */
  case class HighThroughput(
      override val payloadQueueSize: Int = 1000,
      override val payloadWriteBatchMaxSize: Int = 50,
      override val payloadWriteBatchMaxDuration: NonNegativeFiniteDuration =
        NonNegativeFiniteDuration.ofMillis(50),
      override val payloadWriteMaxConcurrency: Int = 4,
      override val payloadToEventBound: NonNegativeFiniteDuration = DefaultPayloadTimestampBound,
      override val eventWriteBatchMaxSize: Int = 100,
      override val eventWriteBatchMaxDuration: NonNegativeFiniteDuration =
        NonNegativeFiniteDuration.ofMillis(50),
      override val commitModeValidation: Option[CommitMode] = CommitMode.Default.some,
      override val maxSqlInListSize: PositiveNumeric[Int] = DefaultMaxSqlInListSize,
  ) extends SequencerWriterConfig
}
