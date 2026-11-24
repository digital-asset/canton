// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p

import com.daml.metrics.api.testing.InMemoryMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricsContext}
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.protocol.BftOrdererLimitsView
import com.digitalasset.canton.synchronizer.block.BlockFormat
import com.digitalasset.canton.synchronizer.metrics.{
  BftOrderingMetrics,
  SequencerHistograms,
  SequencerMetrics,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.SupportedVersions
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.{
  IgnoringUnitTestContext,
  IgnoringUnitTestEnv,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Availability,
  Consensus,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.{
  BftSequencerBaseTest,
  GeneratorsBftOrdering,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessageBody.Message
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersionValidation
import com.digitalasset.canton.version.v1.UntypedVersionedMessage
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

import java.time.DateTimeException

class P2PNetworkInModuleTest extends AnyWordSpec with BftSequencerBaseTest {

  private type AvailabilityRef = ModuleRef[Availability.Message[IgnoringUnitTestEnv]]

  implicit private val context: IgnoringUnitTestContext[v30.BftOrderingMessage] =
    IgnoringUnitTestContext()

  private val generators = new GeneratorsBftOrdering(synchronizerProtocolVersion)
  private val sender = BftNodeId("sender")
  private val outOfRange = Timestamp(seconds = Long.MaxValue, nanos = 0)

  "P2PNetworkInModule" should {
    "forward a well-formed availability message" in {
      val availability = mock[AvailabilityRef]
      val module = createModule(availability)

      storeRequestsAndBatchResponses(Timestamp(seconds = 0, nanos = 0)).foreach { message =>
        module.receive(message)
      }

      verify(availability, times(2))
        .asyncSend(any[Availability.Message[IgnoringUnitTestEnv]])(
          any[TraceContext],
          any[MetricsContext],
        )
    }

    "drop a batch whose ordering start instant is out of range" in {
      val availability = mock[AvailabilityRef]
      val module = createModule(availability)

      storeRequestsAndBatchResponses(outOfRange).foreach { message =>
        assertWarns(
          module.receive(message),
          (logEntry: LogEntry) => {
            logEntry.level shouldBe Level.WARN
            logEntry.message should include(
              s"Dropping availability message from $sender as it couldn't be parsed"
            )
          },
        )
      }

      verify(availability, never)
        .asyncSend(any[Availability.Message[IgnoringUnitTestEnv]])(
          any[TraceContext],
          any[MetricsContext],
        )
    }

    "drop a message and flag its sender when a parser throws" in {
      val metricsFactory = new InMemoryMetricsFactory
      val metrics = new SequencerMetrics(
        new SequencerHistograms(MetricName("test"))(new HistogramInventory),
        metricsFactory,
      ).bftOrdering
      val module = createModule(mock[AvailabilityRef], metrics)
      val exception = new DateTimeException("Instant exceeds minimum or maximum instant")

      assertWarns(
        module.parseUntrusted[Unit](sender, "availability")(throw exception)(_ =>
          fail("a message that failed to parse must not be forwarded")
        ),
        (logEntry: LogEntry) => {
          logEntry.level shouldBe Level.ERROR
          logEntry.message should include(
            s"exception while parsing availability message from $sender"
          )
          logEntry.throwable shouldBe Some(exception)
        },
      )

      val labels = metrics.security.noncompliant.labels
      metricsFactory.metrics.meters
        .get(metrics.security.noncompliant.behavior.info.name)
        .toList
        .flatMap(_.values)
        .flatMap(_.markers)
        .map { case (metricsContext, count) => metricsContext.labels -> count.get() } shouldBe
        List(
          Map(
            labels.Sequencer -> sender,
            labels.violationType.Key -> labels.violationType.values.UnparseableP2PMessage.toString,
          ) -> 1L
        )
    }
  }

  private def createModule(
      availability: AvailabilityRef,
      metrics: BftOrderingMetrics = SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
  ): P2PNetworkInModule[IgnoringUnitTestEnv] =
    new P2PNetworkInModule[IgnoringUnitTestEnv](
      metrics,
      ProtocolVersionValidation.PV(synchronizerProtocolVersion),
      BftOrdererLimitsView.default,
      availability,
      mock[ModuleRef[Consensus.Message[IgnoringUnitTestEnv]]],
      loggerFactory,
      timeouts,
    )

  private def storeRequestsAndBatchResponses(
      orderingStartInstant: Timestamp
  ): Seq[v30.BftOrderingMessage] = {
    val batchId = generators.batchIdArb.arbitrary.sample.value.hash.getCryptographicEvidence
    val batch = v30.Batch(
      Seq(
        v30.OrderingRequest(
          traceContext = "",
          tag = BlockFormat.SendTag,
          messageId = "aMessageId",
          payload = ByteString.copyFromUtf8("payload"),
          orderingStartInstant = Some(orderingStartInstant),
        )
      ),
      epochNumber = 0L,
    )
    Seq(
      v30.AvailabilityMessage.Message.StoreRequest(v30.StoreRequest(batchId, Some(batch))),
      v30.AvailabilityMessage.Message.BatchResponse(v30.BatchResponse(batchId, Some(batch))),
    ).map { availabilityMessage =>
      val signedMessage = v30.SignedMessage(
        message = UntypedVersionedMessage(
          UntypedVersionedMessage.Wrapper.Data(
            v30.AvailabilityMessage(availabilityMessage).toByteString
          ),
          SupportedVersions.ProtoData30.v,
        ).toByteString,
        from = sender,
        signature = Some(Signature.noSignature.toProtoV30),
      )
      v30.BftOrderingMessage(
        traceContext = "",
        body = Some(v30.BftOrderingMessageBody(Message.AvailabilityMessage(signedMessage))),
        sentBy = sender,
        sentAt = None,
      )
    }
  }
}
