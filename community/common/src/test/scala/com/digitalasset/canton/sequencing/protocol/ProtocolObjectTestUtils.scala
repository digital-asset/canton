// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.*
import cats.syntax.either.*
import com.digitalasset.canton.store.SequencedEventStore.{
  IgnoredSequencedEvent,
  OrdinarySequencedEvent,
  PossiblyIgnoredSequencedEvent,
  SequencedEventWithTraceContext,
}
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

/** Utils object to normalize envelopes (to closed, uncompressed form) and other objects using
  * envelopes.
  *
  * Needed to compare objects with different types of envelopes.
  */
object ProtocolObjectTestUtils extends Matchers {

  def assertSequencedEventEquals[Env <: Envelope[?], Env2 <: Envelope[?]](
      actual: DecompressedSequencedEvent[Env],
      expected: DecompressedSequencedEvent[Env2],
      testedProtocolVersion: ProtocolVersion,
  ): Assertion = {
    SequencedEvent.envelopesOf(actual).foreach { envelope =>
      assertEnvelopeType(envelope, testedProtocolVersion)
    }

    normalizeSequencedEvent(actual) shouldBe normalizeSequencedEvent(expected)
  }

  def assertEnvelopeType[Env <: Envelope[?]](
      envelope: Env,
      testedProtocolVersion: ProtocolVersion,
  ): Assertion =
    if (testedProtocolVersion >= ProtocolVersion.v35) {
      envelope shouldBe a[ClosedCompressedEnvelope]
    } else {
      envelope shouldBe a[ClosedUncompressedEnvelope]
    }

  private def normalizeSequencedEvent[Env <: Envelope[?]](
      sequencedEvent: DecompressedSequencedEvent[Env]
  ): DecompressedSequencedEvent[ClosedUncompressedEnvelope] = sequencedEvent match {
    case deliver: Deliver[Batch[Env]] =>
      deliver.copy(
        batch = deliver.batch.toClosedUncompressedBatchResult.valueOr(error =>
          throw new IllegalArgumentException(error.message)
        )
      )
    case deliverError: DeliverError => deliverError
  }

  private def normalizeSignedSequencedEvent[Env <: Envelope[?]](
      signedSequencedEvent: SignedContent[DecompressedSequencedEvent[Env]]
  ): SignedContent[DecompressedSequencedEvent[ClosedUncompressedEnvelope]] =
    signedSequencedEvent.traverse[Id, DecompressedSequencedEvent[ClosedUncompressedEnvelope]](
      normalizeSequencedEvent
    )

  def assertSequencedEventSeqWithTraceContextEqual[Env <: Envelope[?], Env2 <: Envelope[?]](
      actual: Seq[SequencedEventWithTraceContext[Batch[Env]]],
      expected: Seq[SequencedEventWithTraceContext[Batch[Env2]]],
      testedProtocolVersion: ProtocolVersion,
  ): Assertion = {
    for {
      actualSequencedEvent <- actual
      actualFirstEnvelope <- SequencedEvent.envelopesOf(actualSequencedEvent.signedEvent.content)
    } yield {
      assertEnvelopeType(actualFirstEnvelope, testedProtocolVersion)
    }

    actual.map(normalizeSequencedEventWithTraceContext) shouldBe expected.map(
      normalizeSequencedEventWithTraceContext
    )
  }

  private def normalizeSequencedEventWithTraceContext[Env <: Envelope[?]](
      sequencedEventWithTraceContext: SequencedEventWithTraceContext[Batch[Env]]
  ): SequencedEventWithTraceContext[Batch[ClosedUncompressedEnvelope]] =
    SequencedEventWithTraceContext(
      normalizeSignedSequencedEvent(sequencedEventWithTraceContext.signedEvent)
    )(sequencedEventWithTraceContext.traceContext)

  def assertPossiblyIgnoredSequencedEventSeqEqual[Env <: Envelope[?], Env2 <: Envelope[?]](
      actual: Seq[PossiblyIgnoredSequencedEvent[Batch[Env]]],
      expected: Seq[PossiblyIgnoredSequencedEvent[Batch[Env2]]],
      testedProtocolVersion: ProtocolVersion,
  ): Assertion = {
    actual.foreach { event =>
      assertPossiblyIgnoredSequencedEventEnvelopeType(event, testedProtocolVersion)
    }

    actual.map(normalizePossiblyIgnoredSequencedEvent) shouldBe expected.map(
      normalizePossiblyIgnoredSequencedEvent
    )
  }

  def assertPossiblyIgnoredSequencedEventEquals[Env <: Envelope[?], Env2 <: Envelope[?]](
      actual: PossiblyIgnoredSequencedEvent[Batch[Env]],
      expected: PossiblyIgnoredSequencedEvent[Batch[Env2]],
      testedProtocolVersion: ProtocolVersion,
  ): Assertion = {
    assertPossiblyIgnoredSequencedEventEnvelopeType(actual, testedProtocolVersion)

    normalizePossiblyIgnoredSequencedEvent(actual) shouldBe normalizePossiblyIgnoredSequencedEvent(
      expected
    )
  }

  private def assertPossiblyIgnoredSequencedEventEnvelopeType[Env <: Envelope[?]](
      event: PossiblyIgnoredSequencedEvent[Batch[Env]],
      testedProtocolVersion: ProtocolVersion,
  ): Unit =
    event match {
      case ignoredSequencedEvent: IgnoredSequencedEvent[Batch[Env]] =>
        ignoredSequencedEvent.underlying.toList
          .flatMap(sc => SequencedEvent.envelopesOf(sc.content))
          .foreach { envelope =>
            assertEnvelopeType(envelope, testedProtocolVersion)
          }
      case ordinarySequencedEvent: OrdinarySequencedEvent[Batch[Env]] =>
        SequencedEvent.envelopesOf(ordinarySequencedEvent.signedEvent.content).foreach { envelope =>
          assertEnvelopeType(envelope, testedProtocolVersion)
        }
    }

  private def normalizePossiblyIgnoredSequencedEvent[Env <: Envelope[?]](
      event: PossiblyIgnoredSequencedEvent[Batch[Env]]
  ): PossiblyIgnoredSequencedEvent[Batch[ClosedUncompressedEnvelope]] =
    event match {
      case ignoredSequencedEvent: IgnoredSequencedEvent[Batch[Env]] =>
        IgnoredSequencedEvent(
          ignoredSequencedEvent.timestamp,
          ignoredSequencedEvent.counter,
          ignoredSequencedEvent.underlying.map(normalizeSignedSequencedEvent),
          ignoredSequencedEvent.previousTimestamp,
        )(ignoredSequencedEvent.traceContext)
      case ordinarySequencedEvent: OrdinarySequencedEvent[Batch[Env]] =>
        OrdinarySequencedEvent(
          ordinarySequencedEvent.counter,
          normalizeSignedSequencedEvent(ordinarySequencedEvent.signedEvent),
        )(ordinarySequencedEvent.traceContext)
    }

  def normalizeSubmissionRequest[Env <: Envelope[?]](
      submissionRequest: SubmissionRequest
  ): SubmissionRequest =
    submissionRequest.copy(
      batch = submissionRequest.batch.copy(
        envelopes = submissionRequest.batch.envelopes.map(_.toClosedUncompressedEnvelopeUnsafe)
      )
    )

}
