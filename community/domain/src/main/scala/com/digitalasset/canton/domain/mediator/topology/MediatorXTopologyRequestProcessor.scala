// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.topology

import cats.data.EitherT
import cats.syntax.functor.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.client.{SequencerClient, SequencerClientSend}
import com.digitalasset.canton.sequencing.protocol.{
  AggregationRule,
  Batch,
  Deliver,
  OpenEnvelope,
  Recipients,
}
import com.digitalasset.canton.time.DomainTimeTracker
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.transaction.{
  DomainParametersStateX,
  DomainTrustCertificateX,
  MediatorDomainStateX,
  SequencerDomainStateX,
}
import com.digitalasset.canton.topology.{DomainId, TopologyStateProcessorX}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

class MediatorXTopologyRequestProcessor(
    domainId: DomainId,
    client: SequencerClientSend,
    protocolVersion: ProtocolVersion,
    processor: TopologyStateProcessorX,
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends UnsignedProtocolEventHandler
    with NamedLogging
    with FlagCloseable {

  {
    import TraceContext.Implicits.Empty.*
    ErrorUtil.requireState(
      protocolVersion == ProtocolVersion.dev,
      "only supported for dev version!",
    )
  }
  override def name: String = "mediator-x-topology-request-processor"

  override def subscriptionStartsAt(start: SubscriptionStart, domainTimeTracker: DomainTimeTracker)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.unit

  override def apply(
      events: BoxedEnvelope[UnsignedEnvelopeBox, DefaultOpenEnvelope]
  ): HandlerResult = performUnlessClosingF(functionFullName)(Future {
    val requests: Seq[Traced[(CantonTimestamp, RegisterTopologyTransactionRequestX)]] = {
      events.value.collect { case t @ Traced(Deliver(_sc, ts, _, _, batch)) =>
        batch.envelopes.map(_.protocolMessage).collect {
          case request: RegisterTopologyTransactionRequestX => Traced((ts, request))(t.traceContext)
        }
      }.flatten
    }
    // needs to be sequential now as we are applying the changes to the state
    AsyncResult(
      MonadUtil.sequentialTraverse(requests)(process).map(_ => ())
    )
  })(ec, TraceContext.empty)

  private def process(
      event: Traced[(CantonTimestamp, RegisterTopologyTransactionRequestX)]
  ): FutureUnlessShutdown[Unit] = event.withTraceContext { implicit traceContext => event =>
    {
      val (ts, request) = event
      def genResponse(
          res: RegisterTopologyTransactionResponseResult.State,
          aggregationRule: Option[AggregationRule],
          maxSequencingTime: CantonTimestamp,
      ) = {
        val response = OpenEnvelope(
          RegisterTopologyTransactionResponseX.create(
            requestedBy = request.requestedBy,
            requestedFor = request.requestedFor,
            requestId = request.requestId,
            results = request.transactions.map(_ => res),
            domainId = domainId,
            protocolVersion = protocolVersion,
          ),
          Recipients.cc(request.requestedBy),
        )(protocolVersion)
        logger.info(s"Sending ${res} for ${request.requestId}")
        send(response, "responding to request", aggregationRule, maxSequencingTime)
      }

      performUnlessClosingEitherU(functionFullName)(
        processor
          .validateAndApplyAuthorization(
            SequencedTime(ts),
            EffectiveTime(ts),
            request.transactions,
            abortIfCascading = false,
            abortOnError = true,
            expectFullAuthorization = true,
          )
      ).leftMap(x => s"Txs failed ${x}")
        .biflatMap(
          failed => {
            logger.warn(s"Topology evaluation failed with ${failed}")
            EitherT
              .right[String](
                buildSubmissionParameters(ts)
                  .flatMap { case (aggregationRule, maxSequencingTime) =>
                    genResponse(
                      RegisterTopologyTransactionResponseResult.State.Failed,
                      aggregationRule,
                      maxSequencingTime,
                    )
                  }
              )
          },
          _success => {
            EitherT.right[String](for {
              params <- buildSubmissionParameters(ts)
              (aggregationRule, maxSequencingTime) = params
              _ <- acceptTransactions(ts, request, aggregationRule, maxSequencingTime)
              _ <- genResponse(
                RegisterTopologyTransactionResponseResult.State.Accepted,
                aggregationRule,
                maxSequencingTime,
              )
            } yield ())
          },
        )
        .fold(_ => (), _ => ())
    }
  }

  private def buildSubmissionParameters(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext) =
    performUnlessClosingF(functionFullName) {
      processor.store
        .findPositiveTransactions(
          asOf = timestamp,
          asOfInclusive = false,
          types = Seq(DomainParametersStateX.code, MediatorDomainStateX.code),
          isProposal = false,
          filterUid = None,
          filterNamespace = None,
        )
        .map { stored =>
          val aggregationRule = stored
            .collectOfMapping[MediatorDomainStateX]
            .collectLatestByUniqueKey
            .result
            .collectFirst {
              case storedTx
                  if storedTx.transaction.transaction.mapping.group == NonNegativeInt.zero =>
                storedTx.transaction.transaction.mapping
            }
            .map(mds => Some(AggregationRule(mds.active, mds.threshold, protocolVersion)))
            .getOrElse(ErrorUtil.invalidState(s"Unable to find mediator group 0 at $timestamp"))

          val maxSequencingTime = stored
            .collectOfMapping[DomainParametersStateX]
            .collectLatestByUniqueKey
            .result
            .headOption
            .map(_.transaction.transaction.mapping.parameters.mediatorReactionTimeout)
            .map(timeout => timestamp + timeout)
            .getOrElse(
              ErrorUtil.invalidState(s"Unable to find dynamic domain parameters at $timestamp")
            )

          (aggregationRule, maxSequencingTime)
        }
    }

  private def acceptTransactions(
      ts: CantonTimestamp,
      request: RegisterTopologyTransactionRequestX,
      aggregationRule: Option[AggregationRule],
      maxSequencingTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val msg = AcceptedTopologyTransactionsX.create(
      accepted = List(
        AcceptedTopologyTransactionsX.AcceptedRequest(
          requestId = request.requestId,
          transactions = request.transactions,
        )
      ),
      domainId = domainId,
      protocolVersion = protocolVersion,
    )
    performUnlessClosingF(functionFullName)(
      processor.store.findPositiveTransactions(
        asOf = ts,
        asOfInclusive =
          false, // must be exclusive as we don't want to include folks that we just added
        isProposal = false,
        types = Seq(
          DomainTrustCertificateX.code,
          MediatorDomainStateX.code,
          SequencerDomainStateX.code,
        ),
        filterUid = None,
        filterNamespace = None,
      )
    ).flatMap { res =>
      val participants = res
        .collectOfMapping[DomainTrustCertificateX]
        .result
        .map(_.transaction.transaction.mapping.participantId)
      val mediators = res
        .collectOfMapping[MediatorDomainStateX]
        .result
        .map(_.transaction.transaction.mapping)
        .flatMap(mds => mds.active ++ mds.observers)
      val sequencers = res
        .collectOfMapping[SequencerDomainStateX]
        .result
        .map(_.transaction.transaction.mapping)
        .flatMap(sds => sds.active ++ sds.observers)
      val members = participants ++ mediators ++ sequencers
      Recipients.ofSet(members.toSet) match {
        case Some(recipients) =>
          send(
            OpenEnvelope(msg, recipients)(protocolVersion),
            "sending accepted txs to members",
            aggregationRule,
            maxSequencingTime,
          )
        case None =>
          logger.error("Didn't find recipients")
          FutureUnlessShutdown.unit
      }
    }
  }

  private def send(
      envelope: OpenEnvelope[ProtocolMessage],
      description: String,
      aggregationRule: Option[AggregationRule],
      maxSequencingTimestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    SequencerClient
      .sendWithRetries(
        callback =>
          client.sendAsync(
            Batch(List(envelope), protocolVersion),
            callback = callback,
            aggregationRule = aggregationRule,
            maxSequencingTime = maxSequencingTimestamp,
          ),
        maxRetries = timeouts.unbounded.retries(1.second),
        delay = 1.second,
        sendDescription = description,
        errMsg = s"Failed to send: ${description}",
        flagCloseable = this,
      )
      .recover { case NonFatal(e) =>
        performUnlessClosing("recover")(
          logger.error(
            s"After many attempts, failed to send $description",
            e,
          )
        )
      }

}
