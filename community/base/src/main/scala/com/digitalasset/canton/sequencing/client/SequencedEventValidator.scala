// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.Monad
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.flatMap.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{
  HashPurpose,
  SignatureCheckError,
  SyncCryptoApi,
  SyncCryptoClient,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{
  HasLoggerName,
  NamedLoggerFactory,
  NamedLogging,
  NamedLoggingContext,
  TracedLogger,
}
import com.digitalasset.canton.protocol.DynamicDomainParametersWithValidity
import com.digitalasset.canton.sequencing.protocol.{ClosedEnvelope, SequencedEvent}
import com.digitalasset.canton.sequencing.{OrdinarySerializedEvent, PossiblyIgnoredSerializedEvent}
import com.digitalasset.canton.store.SequencedEventStore.IgnoredSequencedEvent
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.version.ProtocolVersion

import java.util.ConcurrentModificationException
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

sealed trait SequencedEventValidationError extends Product with Serializable with PrettyPrinting
object SequencedEventValidationError {
  final case class BadDomainId(expected: DomainId, received: DomainId)
      extends SequencedEventValidationError {
    override def pretty: Pretty[BadDomainId] = prettyOfClass(
      param("expected", _.expected),
      param("received", _.received),
    )
  }
  final case class DecreasingSequencerCounter(
      newCounter: SequencerCounter,
      oldCounter: SequencerCounter,
  ) extends SequencedEventValidationError {
    override def pretty: Pretty[DecreasingSequencerCounter] = prettyOfClass(
      param("new counter", _.newCounter),
      param("old counter", _.oldCounter),
    )
  }
  final case class GapInSequencerCounter(newCounter: SequencerCounter, oldCounter: SequencerCounter)
      extends SequencedEventValidationError {
    override def pretty: Pretty[GapInSequencerCounter] = prettyOfClass(
      param("new counter", _.newCounter),
      param("old counter", _.oldCounter),
    )
  }
  final case class NonIncreasingTimestamp(
      newTimestamp: CantonTimestamp,
      newCounter: SequencerCounter,
      oldTimestamp: CantonTimestamp,
      oldCounter: SequencerCounter,
  ) extends SequencedEventValidationError {
    override def pretty: Pretty[NonIncreasingTimestamp] = prettyOfClass(
      param("new timestamp", _.newTimestamp),
      param("new counter", _.newCounter),
      param("old timestamp", _.oldTimestamp),
      param("old counter", _.oldCounter),
    )
  }
  final case class ForkHappened(
      counter: SequencerCounter,
      suppliedEvent: SequencedEvent[ClosedEnvelope],
      expectedEvent: Option[SequencedEvent[ClosedEnvelope]],
  ) extends SequencedEventValidationError
      with PrettyPrinting {
    override def pretty: Pretty[ForkHappened] = prettyOfClass(
      param("counter", _.counter),
      param("supplied event", _.suppliedEvent),
      paramIfDefined("expected event", _.expectedEvent),
    )
  }
  final case class SignatureInvalid(
      sequencedTimestamp: CantonTimestamp,
      usedTimestamp: CantonTimestamp,
      error: SignatureCheckError,
  ) extends SequencedEventValidationError {
    override def pretty: Pretty[SignatureInvalid] = prettyOfClass(
      unnamedParam(_.error),
      param("sequenced timestamp", _.sequencedTimestamp),
      param("used timestamp", _.usedTimestamp),
    )
  }
  final case class InvalidTimestampOfSigningKey(
      sequencedTimestamp: CantonTimestamp,
      declaredSigningKeyTimestamp: CantonTimestamp,
      reason: SequencedEventValidator.SigningTimestampVerificationError,
  ) extends SequencedEventValidationError {
    override def pretty: Pretty[InvalidTimestampOfSigningKey] = prettyOfClass(
      param("sequenced timestamp", _.sequencedTimestamp),
      param("declared signing key timestamp", _.declaredSigningKeyTimestamp),
      param("reason", _.reason),
    )
  }
  final case class TimestampOfSigningKeyNotAllowed(
      sequencedTimestamp: CantonTimestamp,
      declaredSigningKeyTimestamp: CantonTimestamp,
  ) extends SequencedEventValidationError {
    override def pretty: Pretty[TimestampOfSigningKeyNotAllowed] = prettyOfClass(
      param("sequenced timestamp", _.sequencedTimestamp),
      param("decalred signing key timestamp", _.declaredSigningKeyTimestamp),
    )
  }
}

/** Validate whether a received event is valid for processing. */
trait SequencedEventValidator extends FlagCloseable {

  /** Validates that the supplied event is suitable for processing from the prior event.
    * If the event is successfully validated it becomes the event that the event
    * in a following call will be validated against. We currently assume this is safe to do as if the event fails to be
    * handled by the application then the sequencer client will halt and will need recreating to restart event processing.
    * This method must not be called concurrently as it will corrupt the prior event state.
    */
  def validate(
      event: OrdinarySerializedEvent
  ): EitherT[FutureUnlessShutdown, SequencedEventValidationError, Unit]

  /** Validates a sequenced event when we reconnect against the prior event supplied to [[SequencedEventValidatorFactory.create]] */
  def validateOnReconnect(
      reconnectEvent: OrdinarySerializedEvent
  ): EitherT[FutureUnlessShutdown, SequencedEventValidationError, Unit]
}

object SequencedEventValidator extends HasLoggerName {

  /** Do not validate sequenced events */
  private final case class NoValidation(
      override val timeouts: ProcessingTimeout,
      logger: TracedLogger,
  ) extends SequencedEventValidator {
    override def validate(
        event: OrdinarySerializedEvent
    ): EitherT[FutureUnlessShutdown, SequencedEventValidationError, Unit] =
      EitherT(FutureUnlessShutdown.pure(Either.right(())))
    override def validateOnReconnect(
        reconnectEvent: OrdinarySerializedEvent
    ): EitherT[FutureUnlessShutdown, SequencedEventValidationError, Unit] = validate(reconnectEvent)
  }

  /** Do not validate sequenced events.
    * Only use it in case of a programming error and the need to unblock a deployment or
    * if you blindly trust the sequencer.
    *
    * @param warn whether to log a warning when used
    */
  def noValidation(
      domainId: DomainId,
      sequencerId: SequencerId,
      timeout: ProcessingTimeout,
      warn: Boolean = true,
  )(implicit
      loggingContext: NamedLoggingContext
  ): SequencedEventValidator = {
    if (warn) {
      loggingContext.warn(
        s"You have opted to skip event validation for domain $domainId using the sequencer $sequencerId. You should not do this unless you know what you are doing."
      )
    }
    NoValidation(timeout, loggingContext.tracedLogger)
  }

  /** Validates the requested signing timestamp against the sequencing timestamp and the
    * [[com.digitalasset.canton.protocol.DynamicDomainParameters.sequencerSigningTolerance]]
    * of the domain parameters valid at the requested signing timestamp.
    *
    * @param latestTopologyClientTimestamp The timestamp of an earlier event sent to the topology client
    *                                      such that no topology update has happened
    *                                      between this timestamp (exclusive) and the sequencing timestamp (exclusive).
    * @param warnIfApproximate             Whether to emit a warning if an approximate topology snapshot is used
    * @param optimistic if true, we'll try to be optimistic and validate the event with the current snapshot approximation
    *                   instead of the proper snapshot for the signing timestamp.
    *                   During sequencer key rolling or while updating the dynamic domain parameters,
    *                   an event might have been signed by a key that was just revoked or with a signing key timestamp
    *                   that exceeds the [[com.digitalasset.canton.protocol.DynamicDomainParameters.sequencerSigningTolerance]].
    *                   Optimistic validation may not catch such problems.
    * @return [[scala.Left$]] if the signing timestamp is after the sequencing timestamp or the sequencing timestamp
    *         is after the signing timestamp by more than the
    *         [[com.digitalasset.canton.protocol.DynamicDomainParameters.sequencerSigningTolerance]] valid at the signing timestamp.
    *         [[scala.Right$]] the topology snapshot that can be used for signing the event
    *         and verifying the signature on the event;
    */
  // TODO(#10040) remove optimistic validation
  def validateSigningTimestamp(
      syncCryptoApi: SyncCryptoClient[SyncCryptoApi],
      signingTimestamp: CantonTimestamp,
      sequencingTimestamp: CantonTimestamp,
      latestTopologyClientTimestamp: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
      warnIfApproximate: Boolean,
      optimistic: Boolean = false,
  )(implicit
      loggingContext: NamedLoggingContext,
      executionContext: ExecutionContext,
  ): EitherT[Future, SigningTimestampVerificationError, SyncCryptoApi] = {

    validateSigningTimestampInternal(
      syncCryptoApi,
      signingTimestamp,
      sequencingTimestamp,
      latestTopologyClientTimestamp,
      protocolVersion,
      warnIfApproximate,
      optimistic,
    )(
      SyncCryptoClient.getSnapshotForTimestamp _,
      (topology, traceContext) => topology.findDynamicDomainParameters()(traceContext),
    )
  }

  def validateSigningTimestampUS(
      syncCryptoApi: SyncCryptoClient[SyncCryptoApi],
      signingTimestamp: CantonTimestamp,
      sequencingTimestamp: CantonTimestamp,
      latestTopologyClientTimestamp: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
      warnIfApproximate: Boolean,
      optimistic: Boolean = false,
  )(implicit
      loggingContext: NamedLoggingContext,
      executionContext: ExecutionContext,
      closeContext: CloseContext,
  ): EitherT[FutureUnlessShutdown, SigningTimestampVerificationError, SyncCryptoApi] = {
    validateSigningTimestampInternal(
      syncCryptoApi,
      signingTimestamp,
      sequencingTimestamp,
      latestTopologyClientTimestamp,
      protocolVersion,
      warnIfApproximate,
      optimistic,
    )(
      SyncCryptoClient.getSnapshotForTimestampUS _,
      (topology, traceContext) =>
        closeContext.flagCloseable.performUnlessClosingF("get-dynamic-parameters")(
          topology.findDynamicDomainParameters()(traceContext)
        )(executionContext, traceContext),
    )
  }

  // Base version of validateSigningTimestamp abstracting over the effect type to allow for
  // a `Future` and `FutureUnlessShutdown` version. Once we migrate all usages to the US version, this abstraction
  // should not be needed anymore
  private def validateSigningTimestampInternal[F[_]: Monad](
      syncCryptoApi: SyncCryptoClient[SyncCryptoApi],
      signingTimestamp: CantonTimestamp,
      sequencingTimestamp: CantonTimestamp,
      latestTopologyClientTimestamp: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
      warnIfApproximate: Boolean,
      optimistic: Boolean = false,
  )(
      getSnapshotF: (
          SyncCryptoClient[SyncCryptoApi],
          CantonTimestamp,
          Option[CantonTimestamp],
          ProtocolVersion,
          Boolean,
      ) => F[SyncCryptoApi],
      getDynamicDomainParameters: (
          TopologySnapshot,
          TraceContext,
      ) => F[Either[String, DynamicDomainParametersWithValidity]],
  )(implicit
      loggingContext: NamedLoggingContext
  ): EitherT[F, SigningTimestampVerificationError, SyncCryptoApi] = {
    implicit val traceContext: TraceContext = loggingContext.traceContext

    def snapshotF: F[SyncCryptoApi] = getSnapshotF(
      syncCryptoApi,
      signingTimestamp,
      latestTopologyClientTimestamp,
      protocolVersion,
      warnIfApproximate,
    )

    def validateWithSnapshot(
        snapshot: SyncCryptoApi
    ): F[Either[SigningTimestampVerificationError, SyncCryptoApi]] = {
      getDynamicDomainParameters(snapshot.ipsSnapshot, traceContext)
        .map { dynamicDomainParametersE =>
          for {
            dynamicDomainParameters <- dynamicDomainParametersE.leftMap(NoDynamicDomainParameters)
            tolerance = dynamicDomainParameters.sequencerSigningTolerance
            withinSigningTolerance = {
              import scala.Ordered.orderingToOrdered
              tolerance.unwrap >= sequencingTimestamp - signingTimestamp
            }
            _ <- Either.cond(withinSigningTolerance, (), SigningTimestampTooOld(tolerance))
          } yield snapshot
        }
    }

    if (signingTimestamp > sequencingTimestamp) {
      EitherT.leftT[F, SyncCryptoApi](SigningTimestampAfterSequencingTime)
    } else if (optimistic) {
      val approximateSnapshot = syncCryptoApi.currentSnapshotApproximation
      val approximateSnapshotTime = approximateSnapshot.ipsSnapshot.timestamp
      // If the topology client has caught up to the signing timestamp,
      // use the right snapshot
      if (signingTimestamp <= approximateSnapshotTime) {
        EitherT(snapshotF.flatMap(validateWithSnapshot))
      } else {
        loggingContext.info(
          s"Validating event at $sequencingTimestamp optimistically with snapshot taken at $approximateSnapshotTime"
        )
        EitherT(validateWithSnapshot(approximateSnapshot))
      }
    } else if (signingTimestamp == sequencingTimestamp) {
      // If the signing timestamp is the same as the sequencing timestamp,
      // we don't need to check the tolerance because it is always non-negative.
      EitherT.right[SigningTimestampVerificationError](snapshotF)
    } else {
      EitherT(snapshotF.flatMap(validateWithSnapshot))
    }
  }

  sealed trait SigningTimestampVerificationError
      extends Product
      with Serializable
      with PrettyPrinting
  case object SigningTimestampAfterSequencingTime extends SigningTimestampVerificationError {
    override def pretty: Pretty[SigningTimestampAfterSequencingTime] =
      prettyOfObject[SigningTimestampAfterSequencingTime]
  }
  type SigningTimestampAfterSequencingTime = SigningTimestampAfterSequencingTime.type

  final case class SigningTimestampTooOld(tolerance: NonNegativeFiniteDuration)
      extends SigningTimestampVerificationError {
    override def pretty: Pretty[SigningTimestampTooOld] = prettyOfClass(
      param("tolerance", _.tolerance)
    )
  }

  final case class NoDynamicDomainParameters(error: String)
      extends SigningTimestampVerificationError {
    override def pretty: Pretty[NoDynamicDomainParameters] = prettyOfClass(
      param("error", _.error.unquoted)
    )
  }
}

trait SequencedEventValidatorFactory {

  /** Creates a new [[SequencedEventValidator]] to be used for a subscription with the given parameters.
    *
    * @param initialLastEventProcessedO
    *    The last event that the sequencer client had validated (and persisted) in case of a resubscription.
    *    The [[com.digitalasset.canton.sequencing.client.SequencerSubscription]] requests this event again.
    * @param unauthenticated Whether the subscription is unauthenticated
    */
  def create(
      initialLastEventProcessedO: Option[PossiblyIgnoredSerializedEvent],
      unauthenticated: Boolean,
  )(implicit loggingContext: NamedLoggingContext): SequencedEventValidator
}

object SequencedEventValidatorFactory {

  /** Do not validate sequenced events.
    * Only use it in case of a programming error and the need to unblock a deployment or
    * if you blindly trust the sequencer.
    *
    * @param warn whether to log a warning
    */
  def noValidation(
      domainId: DomainId,
      sequencerId: SequencerId,
      warn: Boolean = true,
      timeouts: ProcessingTimeout,
  ): SequencedEventValidatorFactory = new SequencedEventValidatorFactory {
    override def create(
        initialLastEventProcessedO: Option[PossiblyIgnoredSerializedEvent],
        unauthenticated: Boolean,
    )(implicit loggingContext: NamedLoggingContext): SequencedEventValidator =
      SequencedEventValidator.noValidation(domainId, sequencerId, timeouts, warn)
  }
}

/** Validate whether a received event is valid for processing.
  *
  * @param initialPriorEvent the preceding event of the first event to be validated (can be none on a new connection)
  * @param unauthenticated if true, then the connection is unauthenticated. in such cases, we have to skip some validations.
  * @param optimistic if true, we'll try to be optimistic and validate the event possibly with some stale data. this
  *                   means that during sequencer key rolling, a message might have been signed by a key that was just revoked.
  *                   the security impact is very marginal (and an adverse scenario only possible in the async ms of
  *                   this node validating a few inflight transactions). therefore, this parameter should be set to
  *                   true due to performance reasons.
  */
// TODO(#10040) remove optimistic validation
class SequencedEventValidatorImpl(
    initialPriorEvent: Option[PossiblyIgnoredSerializedEvent],
    unauthenticated: Boolean,
    optimistic: Boolean,
    domainId: DomainId,
    sequencerId: SequencerId,
    protocolVersion: ProtocolVersion,
    syncCryptoApi: SyncCryptoClient[SyncCryptoApi],
    protected val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit executionContext: ExecutionContext)
    extends SequencedEventValidator
    with HasCloseContext
    with NamedLogging {

  import SequencedEventValidationError.*
  import SequencedEventValidatorImpl.*

  private val priorEventRef: AtomicReference[Option[PossiblyIgnoredSerializedEvent]] =
    new AtomicReference[Option[PossiblyIgnoredSerializedEvent]](initialPriorEvent)

  /** Validates that the supplied event is suitable for processing from the prior event.
    * Currently the signature not being valid is not considered an error but its validity is returned to the caller
    * to allow them to choose what to do with the event.
    * If the event is successfully validated (regardless of the signature check) it becomes the event that the event
    * in a following call will be validated against. We currently assume this is safe to do as if the event fails to be
    * handled by the application then the sequencer client will halt and will need recreating to restart event processing.
    * This method must not be called concurrently as it will corrupt the prior event state.
    */
  override def validate(
      event: OrdinarySerializedEvent
  ): EitherT[FutureUnlessShutdown, SequencedEventValidationError, Unit] = {
    val priorEventO = priorEventRef.get()
    val oldCounter = priorEventO.fold(SequencerCounter.Genesis - 1L)(_.counter)
    val newCounter = event.counter

    def checkCounterIncreases: ValidationResult =
      Either.cond(
        newCounter == oldCounter + 1,
        (),
        if (newCounter < oldCounter) DecreasingSequencerCounter(newCounter, oldCounter)
        else GapInSequencerCounter(newCounter, oldCounter),
      )

    def checkTimestampIncreases: ValidationResult =
      priorEventO.traverse_ { prior =>
        val oldTimestamp = prior.timestamp
        val newTimestamp = event.timestamp
        Either.cond(
          newTimestamp > oldTimestamp,
          (),
          NonIncreasingTimestamp(newTimestamp, newCounter, oldTimestamp, oldCounter),
        )
      }

    // TODO(M99): dishonest sequencer: Check that the node is listed as a recipient on all envelopes in the batch

    for {
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        Seq(
          checkCounterIncreases,
          checkDomainId(event),
          checkTimestampIncreases,
        ).sequence_
      )
      // Verify the signature only if we know of a prior event.
      // Otherwise, this is a fresh subscription and we will get the topology state with the first transaction
      // TODO(#4933) Upon a fresh subscription, retrieve the keys via the topology API and validate immediately or
      //  validate the signature after processing the initial event
      _ <- verifySignature(priorEventO, event, protocolVersion)
    } yield updatePriorEvent(priorEventO, event)
  }

  private def updatePriorEvent(
      old: Option[PossiblyIgnoredSerializedEvent],
      newEvent: OrdinarySerializedEvent,
  ): Unit = {
    implicit val traceContext: TraceContext = newEvent.traceContext
    val replaced = priorEventRef.compareAndSet(old, Some(newEvent))
    if (!replaced) {
      // shouldn't happen but likely implies a bug in the caller if it does
      ErrorUtil.internalError(
        new ConcurrentModificationException(
          "The prior event has been unexpectedly changed. Multiple events may be incorrectly being validated concurrently."
        )
      )
    }
  }

  override def validateOnReconnect(
      reconnectEvent: OrdinarySerializedEvent
  ): EitherT[FutureUnlessShutdown, SequencedEventValidationError, Unit] = {
    implicit val traceContext: TraceContext = reconnectEvent.traceContext
    val priorEvent = priorEventRef.get.getOrElse(
      ErrorUtil.internalError(
        new IllegalStateException(
          "No prior event known even though the sequencer client reconnects"
        )
      )
    )
    val checkFork: Either[SequencedEventValidationError, Unit] = priorEvent match {
      case ordinaryPrior: OrdinarySerializedEvent =>
        val oldSequencedEvent = ordinaryPrior.signedEvent.content
        val newSequencedEvent = reconnectEvent.signedEvent.content
        // We compare the contents of the `SequencedEvent` rather than their serialization
        // because the SequencerReader serializes the `SequencedEvent` afresh upon each resubscription
        // and the serialization may therefore differ from time to time. This is fine for auditability
        // because the sequencer also delivers a new signature on the new serialization.
        Either.cond(
          oldSequencedEvent == newSequencedEvent,
          (),
          ForkHappened(oldSequencedEvent.counter, newSequencedEvent, Some(oldSequencedEvent)),
        )
      case ignored: IgnoredSequencedEvent[ClosedEnvelope] =>
        // If the event should be ignored, we nevertheless check the counter
        // We merely check timestamp monotonicity, but not the exact timestamp
        // because when we ignore unsequenced events, we assign them the least possible timestamp.
        Either.cond(
          ignored.counter == reconnectEvent.counter && ignored.timestamp <= reconnectEvent.timestamp,
          (),
          ForkHappened(
            ignored.counter,
            reconnectEvent.signedEvent.content,
            ignored.underlying.map(_.content),
          ),
        )
    }

    for {
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        Seq(
          checkDomainId(reconnectEvent),
          checkFork,
        ).sequence_
      )
      _ <- verifySignature(Some(priorEvent), reconnectEvent, protocolVersion)
    } yield ()
    // do not update the priorEvent because if it was ignored, then it was ignored for a reason.
  }

  private def checkDomainId(event: OrdinarySerializedEvent): ValidationResult = {
    val receivedDomainId = event.signedEvent.content.domainId
    Either.cond(receivedDomainId == domainId, (), BadDomainId(domainId, receivedDomainId))
  }

  private def verifySignature(
      priorEventO: Option[PossiblyIgnoredSerializedEvent],
      event: OrdinarySerializedEvent,
      protocolVersion: ProtocolVersion,
  ): EitherT[FutureUnlessShutdown, SequencedEventValidationError, Unit] = {
    implicit val traceContext: TraceContext = event.traceContext
    if (unauthenticated) {
      // TODO(i4933) once we have topology data on the sequencer api, we might fetch the domain keys
      //  and use the domain keys to validate anything here if we are unauthenticated
      logger.debug(
        s"Skipping sequenced event validation for counter ${event.counter} and timestamp ${event.timestamp} in unauthenticated subscription"
      )
      EitherT.fromEither[FutureUnlessShutdown](checkNoTimestampOfSigningKey(event))
    } else if (event.counter == SequencerCounter.Genesis) {
      // TODO(#4933) This is a fresh subscription. Either fetch the domain keys via a future sequencer API and validate the signature
      //  or wait until the topology processor has processed the topology information in the first message and then validate the signature.
      logger.info(
        s"Skipping signature verification of the first sequenced event due to a fresh subscription"
      )
      // The first sequenced event addressed to a member must not specify a signing key timestamp because
      // the member will only be able to compute snapshots for the current topology state and later.
      EitherT.fromEither[FutureUnlessShutdown](checkNoTimestampOfSigningKey(event))
    } else {
      val signingTs = event.signedEvent.timestampOfSigningKey.getOrElse(event.timestamp)

      def doValidate(
          optimistic: Boolean
      ): EitherT[FutureUnlessShutdown, SequencedEventValidationError, Unit] =
        for {
          snapshot <- SequencedEventValidator
            .validateSigningTimestampUS(
              syncCryptoApi,
              signingTs,
              event.timestamp,
              lastTopologyClientTimestamp(priorEventO),
              protocolVersion,
              warnIfApproximate = true,
              optimistic,
            )
            .leftMap(InvalidTimestampOfSigningKey(event.timestamp, signingTs, _))
          _ <- event.signedEvent
            .verifySignature(snapshot, sequencerId, HashPurpose.SequencedEventSignature)
            .leftMap[SequencedEventValidationError](
              SignatureInvalid(event.timestamp, signingTs, _)
            )
            .mapK(FutureUnlessShutdown.outcomeK)
        } yield ()

      doValidate(optimistic).leftFlatMap { err =>
        // When optimistic validation fails, retry with the right snapshot
        if (optimistic) {
          logger.debug(
            s"Optimistic event validation failed with $err. Falling back to validation with the proper topology state."
          )
          doValidate(optimistic = false)
        } else EitherT.leftT(err)
      }
    }
  }

  private def checkNoTimestampOfSigningKey(event: OrdinarySerializedEvent): ValidationResult = {
    event.signedEvent.timestampOfSigningKey.traverse_(tsOfSigningKey =>
      // Batches addressed to unauthenticated members must not specify a signing key timestamp.
      // As some sequencer implementations in some protocol versions set the timestampOfSigningKey field
      // always to the sequencing timestamp if no timestamp was requested,
      // we tolerate equality.
      Either.cond(
        tsOfSigningKey == event.timestamp,
        (),
        TimestampOfSigningKeyNotAllowed(event.timestamp, tsOfSigningKey),
      )
    )
  }
}

object SequencedEventValidatorImpl {
  private[SequencedEventValidatorImpl] type ValidationResult =
    Either[SequencedEventValidationError, Unit]

  /** The sequencer client assumes that the topology processor is ticked for every event proecessed,
    * even if the event is a [[com.digitalasset.canton.store.SequencedEventStore.IgnoredSequencedEvent]].
    * This is why [[com.digitalasset.canton.sequencing.handlers.DiscardIgnoredEvents]]
    * must not be used in application handlers on nodes that support ignoring events.
    */
  private[SequencedEventValidatorImpl] def lastTopologyClientTimestamp(
      priorEvent: Option[PossiblyIgnoredSerializedEvent]
  ): Option[CantonTimestamp] =
    priorEvent.map(_.timestamp)
}
