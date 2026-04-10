// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.HealthComponent.AlwaysHealthyComponent
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  HasRunOnClosing,
  OnShutdownRunner,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencing.SequencedEventHandler
import com.digitalasset.canton.sequencing.client.SendAsyncClientError.SendAsyncClientResponseError
import com.digitalasset.canton.sequencing.client.pool.Connection.ConnectionConfig
import com.digitalasset.canton.sequencing.client.pool.InternalSequencerConnection.{
  ConnectionAttributes,
  SequencerConnectionHealth,
}
import com.digitalasset.canton.sequencing.client.pool.SequencerConnectionWithPekkoSubscribe
import com.digitalasset.canton.sequencing.client.{
  SendAsyncClientError,
  SequencerSubscription,
  SequencerSubscriptionError,
  SequencerSubscriptionPekko,
  SubscriptionErrorRetryPolicy,
  SubscriptionErrorRetryPolicyPekko,
}
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  GetTrafficStateForMemberRequest,
  GetTrafficStateForMemberResponse,
  SendAsyncError,
  SignedContent,
  SubmissionRequest,
  SubscriptionRequest,
  TopologyStateForInitHashResponse,
  TopologyStateForInitRequest,
  TopologyStateForInitResponse,
}
import com.digitalasset.canton.synchronizer.sequencer.DirectSequencerConnection.{
  SequencedEventError,
  SubscriptionCreationError,
}
import com.digitalasset.canton.synchronizer.sequencer.errors.CreateSubscriptionError
import com.digitalasset.canton.synchronizer.sequencer.errors.CreateSubscriptionError.ShutdownError
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil.DelayedKillSwitch
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, PekkoUtil}
import io.grpc.Status
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.{Done, NotUsed}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.Duration

/** Sequencer connection meant to be used with [[DirectSequencerConnectionPool]].
  */
class DirectSequencerConnection(
    override val config: ConnectionConfig,
    sequencer: Sequencer,
    synchronizerId: PhysicalSynchronizerId,
    sequencerId: SequencerId,
    staticParameters: StaticSynchronizerParameters,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContextExecutor)
    extends SequencerConnectionWithPekkoSubscribe
    with PrettyPrinting {

  override def name: String = config.name

  override val health: SequencerConnectionHealth =
    new SequencerConnectionHealth.AlwaysValidated(s"$name-health", logger)

  override def fail(reason: String)(implicit traceContext: TraceContext): Unit = ()

  override def fatal(reason: String)(implicit traceContext: TraceContext): Unit = ()

  override def sendAsync(
      request: SignedContent[SubmissionRequest],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SendAsyncClientResponseError, Unit] =
    sequencer
      .sendAsyncSigned(request)
      .leftMap(err =>
        SendAsyncClientError.RequestRefused(SendAsyncError.SendAsyncErrorDirect(err.cause))
      )

  override def acknowledgeSigned(
      signedRequest: SignedContent[AcknowledgeRequest],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Boolean] =
    sequencer.acknowledgeSigned(signedRequest).map(_ => true)

  override def getTrafficStateForMember(
      request: GetTrafficStateForMemberRequest,
      timeout: Duration,
      logPolicy: CantonGrpcUtil.GrpcLogPolicy,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, GetTrafficStateForMemberResponse] =
    sequencer
      .getTrafficStateAt(request.member, request.timestamp)
      .map { trafficStateO =>
        GetTrafficStateForMemberResponse(trafficStateO, staticParameters.protocolVersion)
      }
      .leftMap(_.toString)

  override def downloadTopologyStateForInit(
      request: TopologyStateForInitRequest,
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, TopologyStateForInitResponse] =
    ErrorUtil.internalError(
      new UnsupportedOperationException(
        s"$functionFullName is not implemented for DirectSequencerConnection"
      )
    )

  override def logout()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Status, Unit] =
    // In-process connection is not authenticated
    EitherTUtil.unitUS

  override def getTime(timeout: Duration)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Option[CantonTimestamp]] =
    EitherT.right[String](sequencer.sequencingTime)

  override def subscribe[E](
      request: SubscriptionRequest,
      handler: SequencedEventHandler[E],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): Either[String, SequencerSubscription[E]] = ???

  override protected def pretty: Pretty[DirectSequencerConnection] =
    prettyOfClass(param("name", _.name.singleQuoted))

  override def attributes: ConnectionAttributes =
    ConnectionAttributes(synchronizerId, sequencerId, staticParameters)

  override def subscriptionRetryPolicy: SubscriptionErrorRetryPolicy =
    SubscriptionErrorRetryPolicy.never

  override def downloadTopologyStateForInitHash(
      request: TopologyStateForInitRequest,
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, TopologyStateForInitHashResponse] =
    ErrorUtil.internalError(
      new UnsupportedOperationException(
        s"$functionFullName is not implemented for DirectSequencerConnection"
      )
    )

  override type SubscriptionError = DirectSequencerConnection.SubscriptionError

  override def subscribe(request: SubscriptionRequest)(implicit
      traceContext: TraceContext
  ): SequencerSubscriptionPekko[SubscriptionError] = {
    val sourceF = sequencer
      .read(request.member, request.timestamp)
      .value
      .unwrap
      .map {
        case UnlessShutdown.AbortedDueToShutdown =>
          Source
            .single(Left(SubscriptionCreationError(ShutdownError)))
            .mapMaterializedValue((_: NotUsed) =>
              (PekkoUtil.noOpKillSwitch, FutureUnlessShutdown.pure(Done))
            )
        case UnlessShutdown.Outcome(Left(creationError)) =>
          Source
            .single(Left(SubscriptionCreationError(creationError)))
            .mapMaterializedValue((_: NotUsed) =>
              (PekkoUtil.noOpKillSwitch, FutureUnlessShutdown.pure(Done))
            )
        case UnlessShutdown.Outcome(Right(source)) =>
          source.map(_.leftMap(SequencedEventError.apply))
      }
    import com.digitalasset.canton.util.Thereafter.syntax.*
    import com.digitalasset.canton.util.PekkoUtil.syntax.*

    val health = new AlwaysHealthyComponent(s"direct-sequencer-subscription", logger) {
      override lazy val associatedHasRunOnClosing: AutoCloseable & HasRunOnClosing =
        new OnShutdownRunner.PureOnShutdownRunner(logger)
    }

    val source = Source
      .futureSource(sourceF)
      .watchTermination() { (matF, terminationF) =>
        val directExecutionContext = DirectExecutionContext(noTracingLogger)
        val killSwitchF = matF.map { case (killSwitch, _) => killSwitch }(directExecutionContext)
        val killSwitch = new DelayedKillSwitch(killSwitchF, noTracingLogger)
        val doneF = matF
          .flatMap { case (_, doneF) => doneF.unwrap }(directExecutionContext)
          .flatMap(_ => terminationF)(directExecutionContext)
          .thereafter { _ =>
            logger.debug("Closing direct sequencer subscription transport")
            health.associatedHasRunOnClosing.close()
          }
        (killSwitch, doneF)
      }
      .injectKillSwitch { case (killSwitch, _) => killSwitch }

    SequencerSubscriptionPekko(source, health)
  }

  override def subscriptionRetryPolicyPekko: SubscriptionErrorRetryPolicyPekko[SubscriptionError] =
    // unlikely there will be any errors with this direct transport implementation
    SubscriptionErrorRetryPolicyPekko.never

}

object DirectSequencerConnection {
  sealed trait SubscriptionError extends Product with Serializable with PrettyPrinting {
    override protected def pretty: Pretty[SubscriptionError.this.type] = adHocPrettyInstance
  }
  final case class SubscriptionCreationError(error: CreateSubscriptionError)
      extends SubscriptionError
  final case class SequencedEventError(error: SequencerSubscriptionError.SequencedEventError)
      extends SubscriptionError
}
