// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.client.SequencerClient.{
  SequencerTransportContainer,
  SequencerTransports,
}
import com.digitalasset.canton.sequencing.client.SequencerClientSubscriptionError.{
  ApplicationHandlerPassive,
  ApplicationHandlerShutdown,
}
import com.digitalasset.canton.sequencing.client.transports.SequencerClientTransport
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.util.{Failure, Success, Try}

class SequencersTransportState(
    initialSequencerTransports: SequencerTransports,
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  private val closeReasonPromise = Promise[SequencerClient.CloseReason]()

  def completion: Future[SequencerClient.CloseReason] = closeReasonPromise.future

  private val lock = new Object()

  private val state = new mutable.HashMap[SequencerId, SequencerTransportState]()

  blocking(lock.synchronized {
    val sequencerIdToTransportStateMap = initialSequencerTransports.sequencerIdToTransportMap.map {
      case (sequencerId, transport) =>
        (sequencerId, SequencerTransportState(transport))
    }
    state.addAll(sequencerIdToTransportStateMap).discard
  })

  private def transportState(
      sequencerId: SequencerId
  )(implicit traceContext: TraceContext): SequencerTransportState =
    blocking(lock.synchronized {
      state.getOrElse(
        sequencerId,
        ErrorUtil.internalError(
          new IllegalArgumentException(s"sequencerId=$sequencerId is unknown")
        ),
      )
    })

  private def updateTransport(
      sequencerId: SequencerId,
      updatedTransport: SequencerTransportContainer,
  )(implicit traceContext: TraceContext): SequencerTransportState = blocking(lock.synchronized {
    val transportStateBefore = transportState(sequencerId)
    state
      .put(sequencerId, transportStateBefore.withTransport(updatedTransport))
      .discard
    transportStateBefore
  })

  def transport(implicit traceContext: TraceContext): SequencerClientTransport = blocking(
    lock.synchronized {
      val (_, sequencerTransportState) = state.view
        .filterKeys(subscriptionIsHealthy)
        .headOption // TODO (i12377): Introduce round robin
        // TODO (i12377): Can we fallback to first sequencer transport here or should we
        //                introduce EitherT and propagate error handling?
        .orElse(state.headOption)
        .getOrElse(
          sys.error(
            "No healthy subscription at the moment. Try again later."
          ) // TODO (i12377): Error handling
        )
      sequencerTransportState.transport.clientTransport
    }
  )

  def subscriptionIsHealthy(
      sequencerId: SequencerId
  )(implicit traceContext: TraceContext): Boolean =
    transportState(sequencerId).subscription
      .exists(!_.resilientSequencerSubscription.isFailed)

  def transport(sequencerId: SequencerId)(implicit
      traceContext: TraceContext
  ): SequencerClientTransport =
    transportState(sequencerId).transport.clientTransport

  def addSubscription(
      sequencerId: SequencerId,
      subscription: ResilientSequencerSubscription[
        SequencerClientSubscriptionError
      ],
      eventValidator: SequencedEventValidator,
  )(implicit traceContext: TraceContext): Unit = blocking(lock.synchronized {
    val currentSequencerTransportStateForAlias: SequencerTransportState =
      transportState(sequencerId)
    if (currentSequencerTransportStateForAlias.subscription.nonEmpty) {
      // there's an existing subscription!
      logger.warn(
        "Cannot create additional subscriptions to the sequencer from the same client"
      )
      sys.error(
        s"The sequencer client already has a running subscription for sequencerAlias=$sequencerId"
      )
    }
    subscription.closeReason.onComplete(closeWithSubscriptionReason(sequencerId))

    state
      .put(
        sequencerId,
        currentSequencerTransportStateForAlias.withSubscription(
          subscription,
          eventValidator,
        ),
      )
      .discard
  })

  def changeTransport(
      sequencerTransports: SequencerTransports
  )(implicit traceContext: TraceContext): Future[Unit] = blocking(lock.synchronized {
    val oldSequencerIds = state.keySet.toSet
    val newSequencerIds = sequencerTransports.sequencerIdToTransportMap.keySet

    val newValues: Set[SequencerId] = newSequencerIds.diff(oldSequencerIds)
    val removedValues: Set[SequencerId] = oldSequencerIds.diff(newSequencerIds)
    val keptValues: Set[SequencerId] = oldSequencerIds.intersect(newSequencerIds)

    if (newValues.nonEmpty || removedValues.nonEmpty) {
      ErrorUtil.internalErrorAsync(
        new IllegalArgumentException(
          "Adding or removing sequencer subscriptions is not supported at the moment"
        )
      )
    } else
      MonadUtil
        .sequentialTraverse(keptValues.toSeq) { sequencerId =>
          val transportStateBefore =
            updateTransport(sequencerId, sequencerTransports.sequencerIdToTransportMap(sequencerId))
          transportStateBefore.subscription
            .map(_.resilientSequencerSubscription.resubscribeOnTransportChange())
            .getOrElse(Future.unit)
            .thereafter { _ => transportStateBefore.transport.clientTransport.close() }
        }
        .map(_ => ())
  })

  private def closeSubscription(
      sequencerId: SequencerId,
      sequencerState: SequencerTransportState,
  )(implicit traceContext: TraceContext): Unit = {
    logger.debug(s"Closing sequencer subscription $sequencerId...")
    sequencerState.subscription.foreach(_.close())
    sequencerState.transport.clientTransport.close()
    val closeReason = sequencerState.subscription
      .map(_.resilientSequencerSubscription.closeReason)
      .getOrElse(Future.unit)
    logger.trace(s"Wait for the subscription $sequencerId to complete")
    timeouts.shutdownNetwork
      .await_(s"closing resilient sequencer client subscription $sequencerId")(closeReason)
  }

  def closeAllSubscriptions(): Unit = blocking(lock.synchronized {
    import TraceContext.Implicits.Empty.*

    state.toList.foreach { case (sequencerId, subscription) =>
      closeSubscription(sequencerId, subscription)
    }

    closeReasonPromise
      .tryComplete(Success(SequencerClient.CloseReason.ClientShutdown))
      .discard
  })

  private def closeWithSubscriptionReason(sequencerId: SequencerId)(
      subscriptionCloseReason: Try[SubscriptionCloseReason[SequencerClientSubscriptionError]]
  ): Unit = {
    // TODO(i12076): Consider aggregating the current situation about other sequencers and
    //               close the sequencer client only in case of not enough healthy sequencers

    val maybeCloseReason: Try[Either[SequencerClient.CloseReason, Unit]] =
      subscriptionCloseReason.map[Either[SequencerClient.CloseReason, Unit]] {
        case SubscriptionCloseReason.HandlerException(ex) =>
          Left(SequencerClient.CloseReason.UnrecoverableException(ex))
        case SubscriptionCloseReason.HandlerError(ApplicationHandlerPassive(_reason)) =>
          Left(SequencerClient.CloseReason.BecamePassive)
        case SubscriptionCloseReason.HandlerError(ApplicationHandlerShutdown) =>
          Left(SequencerClient.CloseReason.ClientShutdown)
        case SubscriptionCloseReason.HandlerError(err) =>
          Left(SequencerClient.CloseReason.UnrecoverableError(s"handler returned error: $err"))
        case permissionDenied: SubscriptionCloseReason.PermissionDeniedError =>
          // TODO(i12076) This reason should not be closing sequencer client, but should depend on aggregation logic
          Left(SequencerClient.CloseReason.PermissionDenied(s"$permissionDenied"))
        case subscriptionError: SubscriptionCloseReason.SubscriptionError =>
          // TODO(i12076) This reason should not be closing sequencer client, but should depend on aggregation logic
          Left(
            SequencerClient.CloseReason.UnrecoverableError(
              s"subscription implementation failed: $subscriptionError"
            )
          )
        case SubscriptionCloseReason.Closed =>
          // TODO(i12076) This reason should not be closing sequencer client, but should depend on aggregation logic
          Left(SequencerClient.CloseReason.ClientShutdown)
        case SubscriptionCloseReason.Shutdown => Left(SequencerClient.CloseReason.ClientShutdown)
        case SubscriptionCloseReason.TransportChange =>
          Right(()) // we don't want to close the sequencer client when changing transport
      }

    def complete(reason: Try[SequencerClient.CloseReason]): Unit =
      closeReasonPromise.tryComplete(reason).discard

    lazy val closeReason: Try[SequencerClient.CloseReason] = maybeCloseReason.collect {
      case Left(error) =>
        error
    }
    maybeCloseReason match {
      case Failure(_) => complete(closeReason)
      case Success(Left(_)) => complete(closeReason)
      case Success(Right(())) =>
    }
  }
}

final case class SequencerTransportState(
    transport: SequencerTransportContainer,
    subscription: Option[SequencerTransportState.Subscription] = None,
) {

  def withTransport(
      newTransport: SequencerTransportContainer
  ): SequencerTransportState = {
    require(
      newTransport.sequencerId == transport.sequencerId,
      "SequencerId of the new transport must match",
    )
    copy(transport = newTransport)
  }

  def withSubscription(
      resilientSequencerSubscription: ResilientSequencerSubscription[
        SequencerClientSubscriptionError
      ],
      eventValidator: SequencedEventValidator,
  ): SequencerTransportState = {
    require(
      subscription.isEmpty,
      "Cannot create additional subscriptions to the sequencer from the same client",
    )
    copy(subscription =
      Some(
        SequencerTransportState.Subscription(
          resilientSequencerSubscription,
          eventValidator,
        )
      )
    )
  }

}

object SequencerTransportState {
  final case class Subscription(
      resilientSequencerSubscription: ResilientSequencerSubscription[
        SequencerClientSubscriptionError
      ],
      eventValidator: SequencedEventValidator,
  ) {
    def close(): Unit = {
      eventValidator.close()
      resilientSequencerSubscription.close()
    }
  }
}
