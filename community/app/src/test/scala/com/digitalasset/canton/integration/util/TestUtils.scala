// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import com.daml.ledger.javaapi
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.*
import com.digitalasset.canton.console.LocalSequencerReference
import com.digitalasset.canton.damltestsdev.java.da as DA
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.sequencing.client.{SendCallback, SendResult}
import com.digitalasset.canton.sequencing.protocol.{Batch, Deliver, TimeProof}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, FutureHelpers}
import org.scalatest.Assertion
import org.scalatest.OptionValues.*
import org.scalatest.matchers.should.Matchers.{matchPattern, *}

import scala.jdk.CollectionConverters.*

/** A collection of small utilities for tests that have no obvious home */
object TestUtils extends FutureHelpers {

  def hasPersistence(cfg: StorageConfig): Boolean = cfg match {
    case _: StorageConfig.Memory => false
    case _: DbConfig.Postgres => true
    case config: DbConfig.H2 =>
      // Check whether we're using file-based or in-memory storage
      config.config.getString("url").contains("file")
    case otherConfig =>
      throw new IllegalArgumentException(
        s"Could not automatically determine whether test uses persistence from" +
          s" storage config $otherConfig. You probably need to update this logic."
      )
  }

  def damlSet[A](scalaSet: Set[A]): DA.set.types.Set[A] =
    new DA.set.types.Set(
      scalaSet.map((_, javaapi.data.Unit.getInstance())).toMap.asJava
    )

  def waitForTargetTimeOnSequencer(
      sequencer: LocalSequencerReference,
      targetTime: CantonTimestamp,
      logger: TracedLogger,
  ): Assertion = {
    implicit val traceContext: TraceContext =
      TraceContext.createNew("wait-for-target-time-on-sequencer")
    logger.debug(s"Waiting for sequencer $sequencer to reach target time $targetTime")

    val assertion = BaseTest.eventually() {
      // send time proofs until we see a successful deliver with
      // a sequencing time greater than or equal to the target time.
      logger.debug(s"Sending time proof to sequencer $sequencer")
      val sendCallback = SendCallback.future
      sequencer.underlying.value.sequencer.client.runOnClose(sendCallback.runOnClosing)
      sequencer.underlying.value.sequencer.client
        .send(
          messageId = TimeProof.mkTimeProofRequestMessageId,
          batch = Batch(Nil, BaseTest.testedProtocolVersion),
          callback = sendCallback,
        )(
          traceContext,
          MetricsContext.Empty,
        )
        .succeedOnFutureCompleteOrShutdown

      val sendResult = sendCallback.future.futureValueUS
      logger.debug(s"Received send result for time proof $sendResult from sequencer $sequencer")
      sendResult should matchPattern {
        case SendResult.Success(d: Deliver[?]) if d.timestamp >= targetTime =>
      }
    }

    logger.debug(s"Sequencer $sequencer has reached target time $targetTime")

    assertion
  }

}
