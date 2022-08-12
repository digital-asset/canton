// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import cats.syntax.either._
import com.digitalasset.canton.crypto.{Hash, SignatureCheckError, SyncCryptoApi, SyncCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{
  HasLoggerName,
  NamedLoggerFactory,
  NamedLogging,
  NamedLoggingContext,
}
import com.digitalasset.canton.sequencing.protocol.{ClosedEnvelope, SequencedEvent, SignedContent}
import com.digitalasset.canton.sequencing.{OrdinarySerializedEvent, PossiblyIgnoredSerializedEvent}
import com.digitalasset.canton.store.SequencedEventStore.{
  IgnoredSequencedEvent,
  OrdinarySequencedEvent,
}
import com.digitalasset.canton.topology.{DomainId, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.{GenesisSequencerCounter, SequencerCounter}

import java.time.Duration
import java.util.ConcurrentModificationException
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

sealed trait SequencedEventValidationError extends Product with Serializable
object SequencedEventValidationError {
  case class BadDomainId(expected: DomainId, received: DomainId)
      extends SequencedEventValidationError
  case class DecreasingSequencerCounter(newCounter: SequencerCounter, oldCounter: SequencerCounter)
      extends SequencedEventValidationError
  case class GapInSequencerCounter(newCounter: SequencerCounter, oldCounter: SequencerCounter)
      extends SequencedEventValidationError
  case class NonIncreasingTimestamp(
      newTimestamp: CantonTimestamp,
      newCounter: SequencerCounter,
      oldTimestamp: CantonTimestamp,
      oldCounter: SequencerCounter,
  ) extends SequencedEventValidationError
  case class ForkHappened(
      counter: SequencerCounter,
      suppliedEvent: SequencedEvent[ClosedEnvelope],
      expectedEvent: SequencedEvent[ClosedEnvelope],
  ) extends SequencedEventValidationError
      with PrettyPrinting {
    override def pretty: Pretty[ForkHappened] = prettyOfClass(
      param("counter", _.counter),
      param("supplied event", _.suppliedEvent),
      param("expected event", _.expectedEvent),
    )
  }
  case class SignatureInvalid(
      sequencedTimestamp: CantonTimestamp,
      usedTimestamp: CantonTimestamp,
      error: SignatureCheckError,
  ) extends SequencedEventValidationError
}

/** Validate whether a received event is valid for processing. */
trait SequencedEventValidator {

  /** Validates a sequenced event against the current state of the sequencer client. */
  def validate(event: OrdinarySerializedEvent): EitherT[Future, SequencedEventValidationError, Unit]
}

object SequencedEventValidator extends HasLoggerName {

  /** Do not validate sequenced events */
  private case object NoValidation extends SequencedEventValidator {
    override def validate(
        event: OrdinarySerializedEvent
    ): EitherT[Future, SequencedEventValidationError, Unit] =
      EitherT(Future.successful(Either.right(())))
  }

  /** Do not validate sequenced events.
    * Only use it in case of a programming error and the need to unblock a deployment or
    * if you blindly trust the sequencer.
    *
    * @param warn whether to log a warning when used
    */
  def noValidation(domainId: DomainId, sequencerId: SequencerId, warn: Boolean = true)(implicit
      loggingContext: NamedLoggingContext
  ): SequencedEventValidator = {
    if (warn) {
      loggingContext.warn(
        s"You have opted to skip event validation for domain $domainId using the sequencer $sequencerId. You should not do this unless you know what you are doing."
      )
    }
    NoValidation
  }
}

trait SequencedEventValidatorFactory {
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
  ): SequencedEventValidatorFactory = new SequencedEventValidatorFactory {
    override def create(
        initialLastEventProcessedO: Option[PossiblyIgnoredSerializedEvent],
        unauthenticated: Boolean,
    )(implicit loggingContext: NamedLoggingContext): SequencedEventValidator =
      SequencedEventValidator.noValidation(domainId, sequencerId, warn)
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
class SequencedEventValidatorImpl(
    initialPriorEvent: Option[PossiblyIgnoredSerializedEvent],
    unauthenticated: Boolean,
    optimistic: Boolean,
    domainId: DomainId,
    sequencerId: SequencerId,
    syncCryptoApi: SyncCryptoClient[SyncCryptoApi],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SequencedEventValidator
    with NamedLogging {

  import SequencedEventValidationError._
  import SequencedEventValidatorImpl._

  private def invalid(error: SequencedEventValidationError): ValidationResult = Left(error)
  private val valid: ValidationResult = Right(())

  private def topologyChangeDelayAt(
      ts: CantonTimestamp,
      context: => String,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Duration] =
    syncCryptoApi
      .awaitIpsSnapshotUSSupervised(context)(ts)
      .flatMap { snapshot =>
        FutureUnlessShutdown.outcomeF {
          snapshot
            .findDynamicDomainParametersOrDefault(warnOnUsingDefault = false)
            .map(_.topologyChangeDelay.duration)
        }
      }

  private val priorEventRef: AtomicReference[Option[PriorEvent]] =
    new AtomicReference[Option[PriorEvent]](
      initialPriorEvent.map(x => PriorEvent(x, true))
    )

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
  ): EitherT[Future, SequencedEventValidationError, Unit] = {
    implicit val traceContext: TraceContext = event.traceContext
    val priorEventRefO = priorEventRef.get()
    val priorEventO = priorEventRefO.map(_.event)
    val oldCounter = priorEventO.fold(GenesisSequencerCounter - 1L)(_.counter)
    val oldTimestamp = priorEventO.fold(CantonTimestamp.MinValue)(_.timestamp)
    val oldSequencedEventO = priorEventO.flatMap {
      case OrdinarySequencedEvent(signedContent) => Some(signedContent.content)
      case _: IgnoredSequencedEvent[_] => None
    }

    val newSequencedEvent = event.signedEvent.content
    val newCounter = newSequencedEvent.counter
    val newTimestamp = newSequencedEvent.timestamp

    def checkCounter: ValidationResult =
      if (newCounter < oldCounter) invalid(DecreasingSequencerCounter(newCounter, oldCounter))
      else if (newCounter > oldCounter + 1) invalid(GapInSequencerCounter(newCounter, oldCounter))
      else valid

    def checkForFork: ValidationResult = oldSequencedEventO match {
      case Some(oldSequencedEvent) if oldCounter == newCounter =>
        Either.cond(
          // We compare the contents of the `SequencedEvent` rather than their serialization
          // because the SequencerReader serializes the `SequencedEvent` afresh upon each resubscription
          // and the serialization may therefore differ from time to time. This is fine for auditability
          // because the sequencer also delivers a new signature on the new serialization.
          oldSequencedEvent == newSequencedEvent,
          (),
          ForkHappened(oldCounter, newSequencedEvent, oldSequencedEvent),
        )
      case _ => valid
    }

    def checkDomainId: ValidationResult = {
      val receivedDomainId = event.signedEvent.content.domainId
      Either.cond(receivedDomainId == domainId, (), BadDomainId(domainId, receivedDomainId))
    }

    def checkTimestamp: ValidationResult = {
      if (newCounter > oldCounter && newTimestamp <= oldTimestamp)
        invalid(NonIncreasingTimestamp(newTimestamp, newCounter, oldTimestamp, oldCounter))
      else valid
    }

    // TODO(M99): dishonest sequencer: Check that the node is listed as a recipient on all envelopes in the batch

    def validateSync: ValidationResult =
      for {
        _ <- checkCounter
        _ <- checkForFork
        _ <- checkDomainId
        _ <- checkTimestamp
      } yield ()

    // ignore events might wipe a few events and then restart a connection. this in turn will reset our "known timestamp until"
    // until before the timestamp of the previous (possibly ignored) event. therefore, we do have to skip signature
    // verification, as otherwise, we might hang forever until the "previous event" was successfully processed (but it won't)
    // TODO(i7997) check if we can do better than this or explain it. also, does it work with multiple skipped events
    val skipNext = newCounter == oldCounter
    val priorTimestamp =
      priorEventRefO.filterNot(_.skipNextSignatureValidation).map(_.event.timestamp)
    for {
      _ <- EitherT.fromEither[Future](validateSync)
      newHash = SignedContent.hashContent(syncCryptoApi.pureCrypto, newSequencedEvent)
      _ <- verifySignature(priorTimestamp, event, newHash)
      _ =
        if (
          !priorEventRef.compareAndSet(
            priorEventRefO,
            Some(PriorEvent(event, skipNext)),
          )
        ) {
          // shouldn't happen but likely implies a bug in the caller if it does
          ErrorUtil.internalError(
            new ConcurrentModificationException(
              "The prior event has been unexpectedly changed. Multiple events may be incorrectly being validated concurrently."
            )
          )
        }
    } yield ()
  }

  private def verifySignature(
      previousTsO: Option[CantonTimestamp],
      event: OrdinarySerializedEvent,
      hash: Hash,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[Future, SequencedEventValidationError, Unit] =
    (unauthenticated, previousTsO) match {
      // we can not check if we are unauthenticated or if we don't have a previous timestamp (because we haven't received
      // any topology information, so we don't know ... )
      case (true, _) | (_, None) =>
        // TODO(i4933) once we have topology data on the sequencer api, we might fetch the domain keys
        //  and use the domain keys to validate anything here if we are unauthenticated
        logger.debug(
          s"Skipping sequenced event validation for counter ${event.counter} and timestamp ${event.timestamp}"
        )
        EitherT.pure(())
      case (_, Some(previousTs)) =>
        val topologyStateKnownUntil = syncCryptoApi.topologyKnownUntilTimestamp
        import com.digitalasset.canton.lifecycle.FutureUnlessShutdown.syntax._

        def performValidation(
            timestamp: CantonTimestamp
        ): EitherT[FutureUnlessShutdown, SequencedEventValidationError, Unit] = {
          logger.debug(s"Wait for topology snapshot at $timestamp")
          for {
            snapshot <- EitherT.right(
              syncCryptoApi.awaitSnapshotUSSupervised(
                s"await topology ts $timestamp for event at ${event.timestamp}(tsOfSign=${event.signedEvent.timestampOfSigningKey}) with previous=$previousTsO and known=$topologyStateKnownUntil"
              )(timestamp)
            )
            _ <- snapshot
              .verifySignature(hash, sequencerId, event.signedEvent.signature)
              .leftMap[SequencedEventValidationError](
                SignatureInvalid(event.timestamp, timestamp, _)
              )
              .mapK(FutureUnlessShutdown.outcomeK)
          } yield ()
        }

        def determineTimestamp(): FutureUnlessShutdown[CantonTimestamp] = {
          // determine timestamp against which we are going to evaluate this snapshot
          val evaluationTs = event.signedEvent.timestampOfSigningKey match {
            case None => event.timestamp
            case Some(ts) if ts <= event.timestamp =>
              // FIXME(i4639): We need to check that timestamp is not too old, because otherwise a rollover of a compromised key does not revoke the key.
              //  An attacker could still use the compromised key to impersonate the sequencer.
              //  Also, add a lower threshold to domain parameters for what is a reasonable signing timestamps
              ts
            case Some(ts) =>
              // TODO(M40) dishonest submitter / sequencer
              logger.warn(
                s"Supplied timestamp of signing key ${ts} for message at sc=${event.counter} is after event time=${event.timestamp}"
              )
              event.timestamp
          }

          // if the timestamp is less or equal than the topologyStateKnownUntil, then we use that
          // this will be correct also during replays where we already know the topology state of the future
          // in most cases, this should work directly, as the "topology state" is known ahead, so we don't need
          // to wait until previous processing has finished before we can check the signatures here
          if (evaluationTs <= topologyStateKnownUntil) {
            logger.debug(
              s"Evaluation timestamp $evaluationTs is before or at known timestamp $topologyStateKnownUntil"
            )
            FutureUnlessShutdown.pure(evaluationTs)
          } else {
            // the evaluationTs is newer than we know. this can be due to three reasons:
            // (1) we do not have a last update
            // (2) the last update has been before (timestamp - epsilon)
            // (3) the last update has not yet been processed
            //
            // we'll deal with this situation in the following way:
            // (1) we just use the latest state we have.
            // (2) and (3) we determine the epsilon at the last update.
            //     and compute thresholdTs = (previousTs + epsilon(lastTs)).
            //     if our timestamp > thresholdTs, we use thresholdTs.immediateSuccessor for our ts,
            //     as this is the topologyStateKnownUntil time resulting from the preceding message
            //     otherwise, we use the timestamp and wait for the successful processing of the previous event
            topologyChangeDelayAt(
              previousTs,
              s"searching for topology-change-delay at ts $previousTs for event at ${event.timestamp}(tsOfSign=${event.signedEvent.timestampOfSigningKey}) with previous=$previousTsO and known=$topologyStateKnownUntil",
            ).map { topologyChangeDelay =>
              val thresholdTs = previousTs.plus(topologyChangeDelay)
              logger.debug(
                s"Found topology change delay $topologyChangeDelay for $previousTs. Threshold timestamp for event validation is $thresholdTs."
              )
              if (evaluationTs > thresholdTs)
                thresholdTs.immediateSuccessor
              else
                evaluationTs
            }
          }
        }

        // if we are doing optimistic validation, we'll first try to evaluate with an approximate
        // snapshot and only if that one fails, we'll wait for the "real" one.
        // we do this in order to not require topology management to have finished processing
        // the previous event before we start with the remaining ones
        {
          if (optimistic) {
            performValidation(syncCryptoApi.approximateTimestamp).recoverWith { _ =>
              EitherT.right(determineTimestamp()).flatMap(performValidation)
            }
          } else {
            EitherT.right(determineTimestamp()).flatMap(performValidation)
          }
        }.onShutdown {
          // TODO(i4933) on shutdown, we would skip the validation step. rather, we should propagate the shutdown
          logger.debug("Aborting sequenced event validation due to shutdown")
          Right(())
        }
    }
}

object SequencedEventValidatorImpl {
  private[SequencedEventValidatorImpl] type ValidationResult =
    Either[SequencedEventValidationError, Unit]

  private[SequencedEventValidatorImpl] case class PriorEvent(
      event: PossiblyIgnoredSerializedEvent,
      skipNextSignatureValidation: Boolean,
  )
}
