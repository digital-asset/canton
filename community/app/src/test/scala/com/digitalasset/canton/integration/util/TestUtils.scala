// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import com.daml.ledger.javaapi
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.*
import com.digitalasset.canton.console.{
  BaseInspection,
  InstanceReference,
  LocalMediatorReference,
  LocalSequencerReference,
}
import com.digitalasset.canton.damltestslf23.java.da as DA
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.sequencing.client.{SendCallback, SendResult, SequencerClient}
import com.digitalasset.canton.sequencing.protocol.{Batch, Deliver, TimeProof}
import com.digitalasset.canton.synchronizer.mediator.MediatorNode
import com.digitalasset.canton.synchronizer.sequencer.SequencerNode
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

  def waitForTargetTimeOnSynchronizerNode(
      targetTime: CantonTimestamp,
      logger: TracedLogger,
  )(
      node: InstanceReference & BaseInspection[?]
  ): Assertion =
    waitForTargetTime(
      node,
      node.underlying.value match {
        case mediator: MediatorNode =>
          mediator.replicaManager.mediatorRuntime.value.mediator.sequencerClient
        case sequencer: SequencerNode =>
          sequencer.sequencer.client
        case other => fail(s"Unsupported node $other")
      },
      targetTime,
      logger,
    )

  def waitForTargetTimeOnSequencer(
      sequencer: LocalSequencerReference,
      targetTime: CantonTimestamp,
      logger: TracedLogger,
  ): Assertion = waitForTargetTimeOnSynchronizerNode(targetTime, logger)(sequencer)

  def waitForTargetTimeOnMediator(
      mediator: LocalMediatorReference,
      targetTime: CantonTimestamp,
      logger: TracedLogger,
  ): Assertion =
    waitForTargetTimeOnSynchronizerNode(targetTime, logger)(mediator)

  def waitForTargetTime(
      forNode: InstanceReference,
      sequencerClient: SequencerClient,
      targetTime: CantonTimestamp,
      logger: TracedLogger,
  ): Assertion = {
    implicit val traceContext: TraceContext =
      TraceContext.createNew("wait-for-target-time")
    logger.debug(s"Waiting for node $forNode to reach target time $targetTime")

    val assertion = BaseTest.eventually() {
      // send time proofs until we see a successful deliver with
      // a sequencing time greater than or equal to the target time.
      logger.debug(s"Sending time proof to node $forNode")
      val sendCallback = SendCallback.future
      sequencerClient.runOnClose(sendCallback.runOnClosing)
      sequencerClient
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
      logger.debug(s"Received send result for time proof $sendResult from node $forNode")
      sendResult should matchPattern {
        case SendResult.Success(d: Deliver[?]) if d.timestamp >= targetTime =>
      }
    }

    logger.debug(s"Node $forNode has reached target time $targetTime")

    assertion
  }
}
