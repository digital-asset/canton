// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.{Hash, SignatureCheckError, SyncCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.{DomainId, GenesisSequencerCounter, SequencerCounter}

import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

sealed trait SequencedEventValidationError
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
      suppliedTimestamp: CantonTimestamp,
      expectedTimestamp: CantonTimestamp,
      suppliedHash: Hash,
      expectedHash: Hash,
  ) extends SequencedEventValidationError
  case class SignatureInvalid(
      sequencedTimestamp: CantonTimestamp,
      usedTimestamp: CantonTimestamp,
      error: SignatureCheckError,
  ) extends SequencedEventValidationError
}

/** Validate whether a received event is valid for processing. */
trait ValidateSequencedEvent {

  /** Validates a sequenced event against the current state of the sequencer client. */
  def validate(event: OrdinarySerializedEvent): EitherT[Future, SequencedEventValidationError, Unit]
}

/** Validate whether a received event is valid for processing.
  *
  * @param initialPriorEventMetadata the preceding event of the first event to be validated (can be none on a new connection)
  * @param unauthenticated if true, then the connection is unauthenticated. in such cases, we have to skip some validations.
  * @param optimistic if true, we'll try to be optimistic and validate the event possibly with some stale data. this
  *                   means that during sequencer key rolling, a message might have been signed by a key that was just revoked.
  *                   the security impact is very marginal (and an adverse scenario only possible in the async ms of
  *                   this node validating a few inflight transactions). therefore, this parameter should be set to
  *                   true due to performance reasons.
  * @param skipValidation if this flag is set to true, then the sequenced event validation will be skipped entirely.
  *                       only use it in case of a programming error and the need to unblock a deployment.
  */
class SequencedEventValidator(
    initialPriorEventMetadata: Option[SequencedEventMetadata],
    unauthenticated: Boolean,
    optimistic: Boolean,
    skipValidation: Boolean,
    domainId: DomainId,
    sequencerId: SequencerId,
    syncCryptoApi: SyncCryptoClient,
    timely: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends ValidateSequencedEvent
    with NamedLogging {

  if (skipValidation) {
    noTracingLogger.warn(
      "You have opted to skip event validation. You should not do this unless you know what you are doing."
    )
  }

  import SequencedEventValidationError._

  private type ValidationResult = Either[SequencedEventValidationError, Unit]
  private case class PriorEventMetadata(
      event: SequencedEventMetadata,
      skipNextSignatureValidation: Boolean,
  )
  private def invalid(error: SequencedEventValidationError): ValidationResult = Left(error)
  private val valid: ValidationResult = Right(())

  private def topologyChangeDelayAt(
      ts: CantonTimestamp,
      context: => String,
  )(implicit traceContext: TraceContext): Future[Duration] =
    timely.supervised(context)(
      syncCryptoApi
        .awaitIpsSnapshot(ts)
        .flatMap(
          _.findDynamicDomainParametersOrDefault(warnOnUsingDefault = false)
            .map(_.topologyChangeDelay.duration)
        )
    )

  private val priorEventMetadataRef: AtomicReference[Option[PriorEventMetadata]] =
    new AtomicReference[Option[PriorEventMetadata]](
      initialPriorEventMetadata.map(x => PriorEventMetadata(x, true))
    )

  /** Validates that the supplied event is suitable for processing from the prior event.
    * Currently the signature not being valid is not considered an error but its validity is returned to the caller
    * to allow them to choose what to do with the event.
    * If the event is successfully validated (regardless of the signature check) it becomes the event that the event
    * in a following call will be validated against. We currently assume this is safe to do as if the event fails to be
    * handled by the application then the sequencer client will halt and will need recreating to restart event processing.
    * This method should not be called concurrently as it will corrupt the prior event state.
    */
  override def validate(
      event: OrdinarySerializedEvent
  ): EitherT[Future, SequencedEventValidationError, Unit] = if (skipValidation) EitherT.pure(())
  else {
    implicit val traceContext: TraceContext = event.traceContext
    val priorEventRefO = priorEventMetadataRef.get()
    val priorEventMetadataO = priorEventRefO.map(_.event)
    val oldCounter = priorEventMetadataO.fold(GenesisSequencerCounter - 1L)(_.counter)
    val oldTimestamp = priorEventMetadataO.fold(CantonTimestamp.MinValue)(_.timestamp)
    val oldHashO = priorEventMetadataO.flatMap(_.hash)

    val newEventMetadata @ SequencedEventMetadata(newCounter, newTimestamp, newHashO) =
      SequencedEventMetadata.fromPossiblyIgnoredSequencedEvent(syncCryptoApi.pureCrypto, event)
    val newHash = newHashO.getOrElse(
      ErrorUtil.internalError(
        new IllegalStateException("Undefined hash for ordinary sequenced event!")
      )
    )

    def checkCounter: ValidationResult =
      if (newCounter < oldCounter) invalid(DecreasingSequencerCounter(newCounter, oldCounter))
      else if (newCounter > oldCounter + 1) invalid(GapInSequencerCounter(newCounter, oldCounter))
      else valid

    def checkForFork: ValidationResult = oldHashO match {
      case Some(oldHash) if oldCounter == newCounter =>
        Either.cond(
          newTimestamp == oldTimestamp && oldHash == newHash,
          (),
          ForkHappened(oldCounter, newTimestamp, oldTimestamp, newHash, oldHash),
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
      _ <- verifySignature(priorTimestamp, event, newHash)
      _ =
        if (
          !priorEventMetadataRef.compareAndSet(
            priorEventRefO,
            Some(PriorEventMetadata(newEventMetadata, skipNext)),
          )
        ) {
          // shouldn't happen but likely implies a bug in the caller if it does
          sys.error(
            "The prior event metadata has been unexpectedly changed. Multiple events may be incorrectly being validated concurrently."
          )
        }
    } yield ()
  }

  private def verifySignature(
      previousTsO: Option[CantonTimestamp],
      event: OrdinarySerializedEvent,
      hash: Hash,
  )(implicit traceContext: TraceContext): EitherT[Future, SequencedEventValidationError, Unit] =
    (unauthenticated, previousTsO) match {
      // we can not check if we are unauthenticated or if we don't have a previous timestamp (because we haven't received
      // any topology information, so we don't know ... )
      case (true, _) | (_, None) =>
        // TODO(i4933) once we have topology data on the sequencer api, we might fetch the domain keys
        //  and use the domain keys to validate anything here if we are unauthenticated
        EitherT.pure(())
      case (_, Some(previousTs)) =>
        val topologyStateKnownUntil = syncCryptoApi.topologyKnownUntilTimestamp

        def performValidation(timestamp: CantonTimestamp) = {
          for {
            snapshot <- EitherT.right(
              timely.supervised(
                s"await topology ts $timestamp for event at ${event.timestamp}(tsOfSign=${event.signedEvent.timestampOfSigningKey}) with previous=$previousTsO and known=$topologyStateKnownUntil"
              )(
                syncCryptoApi.awaitSnapshot(timestamp)
              )
            )
            _ <- snapshot
              .verifySignature(hash, sequencerId, event.signedEvent.signature)
              .leftMap[SequencedEventValidationError](
                SignatureInvalid(event.timestamp, timestamp, _)
              )
          } yield ()
        }

        def determineTimestamp(): Future[CantonTimestamp] = {
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
          if (evaluationTs <= topologyStateKnownUntil)
            Future.successful(evaluationTs)
          else {
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
        if (optimistic) {
          performValidation(syncCryptoApi.approximateTimestamp).recoverWith { _ =>
            EitherT.right(determineTimestamp()).flatMap(performValidation)
          }
        } else {
          EitherT.right(determineTimestamp()).flatMap(performValidation)
        }
    }
}
