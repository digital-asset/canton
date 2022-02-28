// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import com.digitalasset.canton.time.Clock

import scala.concurrent.ExecutionContext

trait SequencerConfig

/** Unsealed trait so the database sequencer config can be reused between community and enterprise */
trait DatabaseSequencerConfig {
  this: SequencerConfig =>

  val highAvailability: SequencerHighAvailabilityConfig
  val writer: SequencerWriterConfig
  val reader: SequencerReaderConfig
  val testingInterceptor: Option[DatabaseSequencerConfig.TestingInterceptor]
}

object DatabaseSequencerConfig {

  /** The Postgres sequencer supports adding a interceptor within the sequencer itself for manipulating sequence behavior during tests.
    * This is used for delaying and/or dropping messages to verify the behavior of transaction processing in abnormal scenarios in a deterministic way.
    * It is not expected to be used at runtime in any capacity and is not possible to set through pureconfig.
    */
  type TestingInterceptor =
    Clock => Sequencer => ExecutionContext => Sequencer
}

sealed trait CommunitySequencerConfig extends SequencerConfig

object CommunitySequencerConfig {
  case class Database(
      writer: SequencerWriterConfig = SequencerWriterConfig.LowLatency(),
      reader: SequencerReaderConfig = SequencerReaderConfig(),
      highAvailability: SequencerHighAvailabilityConfig = SequencerHighAvailabilityConfig(),
      testingInterceptor: Option[DatabaseSequencerConfig.TestingInterceptor] = None,
  ) extends CommunitySequencerConfig
      with DatabaseSequencerConfig
}
