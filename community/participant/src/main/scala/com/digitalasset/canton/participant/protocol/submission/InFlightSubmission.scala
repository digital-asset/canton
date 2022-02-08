// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.Functor
import com.digitalasset.canton.{DomainId, LedgerSubmissionId, SequencerCounter}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.store.InFlightSubmissionStore.{
  InFlightByMessageId,
  InFlightBySequencingInfo,
}
import com.digitalasset.canton.participant.store.SerializableSubmissionId
import com.digitalasset.canton.participant.sync.TimestampedEvent.TimelyRejectionEventId
import com.digitalasset.canton.sequencing.protocol.MessageId
import com.digitalasset.canton.store.db.DbSerializationException
import com.digitalasset.canton.tracing.TraceContext
import slick.jdbc.GetResult

import java.util.UUID

/** Collects information about an in-flight submission,
  * to be stored in [[com.digitalasset.canton.participant.store.InFlightSubmissionStore]].
  *
  * @param changeIdHash The identifier for the intended ledger change.
  *                     We only include the hash instead of the
  *                     [[com.daml.ledger.participant.state.v2.ChangeId]] so that
  *                     we do not need to persist and reconstruct the actual contents
  *                     of the [[com.daml.ledger.participant.state.v2.ChangeId]] when
  *                     we read an [[InFlightSubmission]] from the store.
  * @param submissionId Optional submission id.
  * @param submissionDomain The domain to which the submission is supposed to be/was sent.
  * @param messageUuid The message UUID that will be/has been used for the
  *                  [[com.digitalasset.canton.sequencing.protocol.SubmissionRequest]]
  * @param sequencingInfo Information about when the request will be/was sequenced
  * @param submissionTraceContext The [[com.digitalasset.canton.tracing.TraceContext]] of the submission.
  */
// TODO(#7348) Add submission rank
case class InFlightSubmission[+SequencingInfo <: SubmissionSequencingInfo](
    changeIdHash: ChangeIdHash,
    submissionId: Option[LedgerSubmissionId],
    submissionDomain: DomainId,
    messageUuid: UUID,
    sequencingInfo: SequencingInfo,
    submissionTraceContext: TraceContext,
) extends PrettyPrinting {

  def messageId: MessageId = MessageId.fromUuid(messageUuid)

  /** Whether the submission's sequencing has been observed */
  def isSequenced: Boolean = sequencingInfo.isSequenced

  def mapSequencingInfo[B <: SubmissionSequencingInfo](
      f: SequencingInfo => B
  ): InFlightSubmission[B] =
    setSequencingInfo(f(sequencingInfo))

  def traverseSequencingInfo[F[_], B <: SubmissionSequencingInfo](f: SequencingInfo => F[B])(
      implicit F: Functor[F]
  ): F[InFlightSubmission[B]] =
    F.map(f(sequencingInfo))(setSequencingInfo)

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def setSequencingInfo[B <: SubmissionSequencingInfo](
      newSequencingInfo: B
  ): InFlightSubmission[B] = {
    if (sequencingInfo eq newSequencingInfo) this.asInstanceOf[InFlightSubmission[B]]
    else this.copy(sequencingInfo = newSequencingInfo)
  }

  private[participant] def associatedTimestamp: CantonTimestamp =
    (sequencingInfo: @unchecked) match {
      case UnsequencedSubmission(timeout, _trackingData) => timeout
      case SequencedSubmission(_sequencerCounter, sequencingTime) => sequencingTime
    }

  override def pretty: Pretty[InFlightSubmission.this.type] = prettyOfClass(
    param("change ID hash", _.changeIdHash),
    paramIfDefined("submissionid", _.submissionId),
    param("submission domain", _.submissionDomain),
    param("message UUID", _.messageUuid),
    param("sequencing info", _.sequencingInfo),
    param("submission trace context", _.submissionTraceContext),
  )

  /** @param ev Enforces that this method is called only on unsequenced in-flight submissions
    *           as there is no point in talking about timely rejections for sequenced submissions.
    */
  def timelyRejectionEventId(implicit
      ev: SequencingInfo <:< UnsequencedSubmission
  ): TimelyRejectionEventId =
    TimelyRejectionEventId(submissionDomain, messageUuid)

  def referenceByMessageId: InFlightByMessageId = InFlightByMessageId(submissionDomain, messageId)

  def referenceBySequencingInfo(implicit
      ev: SequencingInfo <:< SequencedSubmission
  ): InFlightBySequencingInfo =
    InFlightBySequencingInfo(submissionDomain, ev(sequencingInfo))
}

object InFlightSubmission {
  implicit def getResultInFlightSubmission[SequencingInfo <: SubmissionSequencingInfo: GetResult](
      implicit getResultTraceContext: GetResult[TraceContext]
  ): GetResult[InFlightSubmission[SequencingInfo]] = { r =>
    import com.digitalasset.canton.resource.DbStorage.Implicits._
    val changeId = r.<<[ChangeIdHash]
    val submissionId = r.<<[Option[SerializableSubmissionId]].map(_.submissionId)
    val submissionDomain = r.<<[DomainId]
    val messageId = r.<<[UUID]
    val sequencingInfo = r.<<[SequencingInfo]
    val submissionTraceContext = r.<<[TraceContext]
    InFlightSubmission(
      changeId,
      submissionId,
      submissionDomain,
      messageId,
      sequencingInfo,
      submissionTraceContext,
    )
  }
}

/** Information about when an [[InFlightSubmission]] was/will be sequenced */
sealed trait SubmissionSequencingInfo extends Product with Serializable with PrettyPrinting {

  /** Whether the [[InFlightSubmission]]'s sequencing was observed. */
  def isSequenced: Boolean = asUnsequenced.isEmpty

  def asUnsequenced: Option[UnsequencedSubmission]
  def asSequenced: Option[SequencedSubmission]
}

object SubmissionSequencingInfo {
  implicit def getResultSubmissionSequencingInfo(implicit
      getResultByteArrayO: GetResult[Option[Array[Byte]]]
  ): GetResult[SubmissionSequencingInfo] = { r =>
    val timeoutO = r.<<[Option[CantonTimestamp]]
    val sequencerCounterO = r.<<[Option[SequencerCounter]]
    val sequencingTimeO = r.<<[Option[CantonTimestamp]]
    val trackingDataO = r.<<[Option[SubmissionTrackingData]]

    (timeoutO, sequencerCounterO, sequencingTimeO, trackingDataO) match {
      case (Some(timeout), None, None, Some(trackingData)) =>
        UnsequencedSubmission(timeout, trackingData)
      case (None, Some(sequencerCounter), Some(sequencingTime), None) =>
        SequencedSubmission(sequencerCounter, sequencingTime)
      case _ =>
        throw new DbSerializationException(
          s"Invalid submission sequencing info: timeout=$timeoutO, sequencer counter=$sequencerCounterO, sequencing time=$sequencingTimeO, tracking data=$trackingDataO"
        )
    }
  }
}

/** Identifies an [[InFlightSubmission]] whose sequencing has not yet been observed.
  *
  * @param timeout The point in sequencer time after which the submission cannot be sequenced any more.
  *                Typically this is the max sequencing time for the
  *                [[com.digitalasset.canton.sequencing.protocol.SubmissionRequest]].
  *                It can be earlier if the submission logic decided to not send a request at all
  *                or the sent request was rejected
  * @param trackingData The information required to produce an appropriate rejection event when the timeout has elapsed.
  */
case class UnsequencedSubmission(timeout: CantonTimestamp, trackingData: SubmissionTrackingData)
    extends SubmissionSequencingInfo {

  override def asUnsequenced: Some[UnsequencedSubmission] = Some(this)
  override def asSequenced: None.type = None

  override def pretty: Pretty[UnsequencedSubmission] = prettyOfClass(
    param("timeout", _.timeout),
    param("tracking data", _.trackingData),
  )
}

object UnsequencedSubmission {
  implicit def getResultUnsequencedSubmission(implicit
      getResultByteArray: GetResult[Array[Byte]]
  ): GetResult[UnsequencedSubmission] = GetResult { r =>
    val timeout = r.<<[CantonTimestamp]
    val trackingData = r.<<[SubmissionTrackingData]
    UnsequencedSubmission(timeout, trackingData)
  }
}

/** The observed sequencing information of an [[InFlightSubmission]]
  *
  * @param sequencerCounter The [[com.digitalasset.canton.SequencerCounter]] assigned to the
  *                         [[com.digitalasset.canton.sequencing.protocol.SubmissionRequest]]
  * @param sequencingTime The sequencer timestamp assigned to the [[com.digitalasset.canton.sequencing.protocol.SubmissionRequest]]
  */
case class SequencedSubmission(sequencerCounter: SequencerCounter, sequencingTime: CantonTimestamp)
    extends SubmissionSequencingInfo {

  override def asUnsequenced: None.type = None
  override def asSequenced: Some[SequencedSubmission] = Some(this)

  override def pretty: Pretty[SequencedSubmission] = prettyOfClass(
    param("sequencer counter", _.sequencerCounter),
    param("sequencing time", _.sequencingTime),
  )
}

object SequencedSubmission {
  implicit val getResultSequencedSubmission: GetResult[SequencedSubmission] = GetResult { r =>
    val sequencerCounter = r.<<[SequencerCounter]
    val timestamp = r.<<[CantonTimestamp]
    SequencedSubmission(sequencerCounter, timestamp)
  }
}
