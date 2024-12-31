// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.implicits.{toBifunctorOps, toTraverseOps}
import cats.syntax.either.*
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.participant.v30.*
import com.digitalasset.canton.admin.participant.v30.InspectionServiceGrpc.InspectionService
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.InspectionServiceErrorGroup
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.{GrpcFUSExtended, wrapErrUS}
import com.digitalasset.canton.participant.admin.inspection.SyncStateInspection
import com.digitalasset.canton.participant.admin.inspection.SyncStateInspection.InFlightCount
import com.digitalasset.canton.participant.domain.DomainAliasManager
import com.digitalasset.canton.participant.pruning.{
  CommitmentContractMetadata,
  CommitmentInspectContract,
}
import com.digitalasset.canton.protocol.messages.{
  CommitmentPeriodState,
  DomainSearchCommitmentPeriod,
  ReceivedAcsCommitment,
  SentAcsCommitment,
}
import com.digitalasset.canton.protocol.{DomainParametersLookup, LfContractId}
import com.digitalasset.canton.pruning.{ConfigForDomainThresholds, ConfigForSlowCounterParticipants}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore}
import com.digitalasset.canton.topology.client.IdentityProvidingServiceClient
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.{EitherTUtil, GrpcStreamingUtils}
import com.digitalasset.daml.lf.data.Bytes
import io.grpc.stub.StreamObserver

import java.io.OutputStream
import scala.concurrent.{ExecutionContext, Future}

class GrpcInspectionService(
    syncStateInspection: SyncStateInspection,
    ips: IdentityProvidingServiceClient,
    indexedStringStore: IndexedStringStore,
    domainAliasManager: DomainAliasManager,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends InspectionService
    with NamedLogging {

  override def lookupOffsetByTime(
      request: LookupOffsetByTime.Request
  ): Future[LookupOffsetByTime.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    request.timestamp
      .fold[FutureUnlessShutdown[LookupOffsetByTime.Response]](
        FutureUnlessShutdown.failed(new IllegalArgumentException(s"""Timestamp not specified"""))
      ) { ts =>
        CantonTimestamp.fromProtoTimestamp(ts) match {
          case Right(cantonTimestamp) =>
            syncStateInspection
              .getOffsetByTime(cantonTimestamp)
              .map(ledgerOffset => LookupOffsetByTime.Response(ledgerOffset))
          case Left(err) =>
            FutureUnlessShutdown.failed(
              new IllegalArgumentException(s"""Failed to parse timestamp: $err""")
            )
        }
      }
      .asGrpcResponse
  }

  /** Configure metrics for slow counter-participants (i.e., that are behind in sending commitments) and
    * configure thresholds for when a counter-participant is deemed slow.
    *
    * returns error if synchronizer ids re not distinct.
    */
  override def setConfigForSlowCounterParticipants(
      request: SetConfigForSlowCounterParticipants.Request
  ): Future[SetConfigForSlowCounterParticipants.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val result = for {
      mapped <- wrapErrUS(
        request.configs
          .flatMap(ConfigForSlowCounterParticipants.fromProto30)
          .sequence
      )
      allsynchronizerIds = request.configs.flatMap(_.synchronizerIds)

      mappedDistinct <- EitherT.cond[FutureUnlessShutdown](
        allsynchronizerIds.distinct.lengthIs == allsynchronizerIds.length,
        mapped,
        InspectionServiceError.IllegalArgumentError.Error(
          "synchronizerIds are not distinct"
        ),
      )

      mappedThreshold <- wrapErrUS(
        request.configs
          .flatMap(ConfigForDomainThresholds.fromProto30)
          .sequence
      )

      _ <- EitherTUtil
        .fromFuture(
          syncStateInspection.addOrUpdateConfigsForSlowCounterParticipants(
            mappedDistinct,
            mappedThreshold,
          ),
          err => InspectionServiceError.InternalServerError.Error(err.toString),
        )
        .leftWiden[CantonError]
    } yield SetConfigForSlowCounterParticipants.Response()

    CantonGrpcUtil.mapErrNewEUS(result)
  }

  /** Get the current configuration for metrics for slow counter-participants.
    */
  override def getConfigForSlowCounterParticipants(
      request: GetConfigForSlowCounterParticipants.Request
  ): Future[GetConfigForSlowCounterParticipants.Response] =
    TraceContextGrpc.withGrpcTraceContext { implicit traceContext =>
      {
        for {
          allConfigs <- syncStateInspection.getConfigsForSlowCounterParticipants()
          (slowCounterParticipants, domainThresholds) = allConfigs
          filtered =
            if (request.synchronizerIds.isEmpty) slowCounterParticipants
            else
              slowCounterParticipants.filter(config =>
                request.synchronizerIds.contains(config.synchronizerId.toProtoPrimitive)
              )
          filteredWithThreshold = filtered
            .map(cfg =>
              (
                cfg,
                domainThresholds
                  .find(dThresh => dThresh.synchronizerId == cfg.synchronizerId),
              )
            )
            .flatMap {
              case (cfg, Some(thresh)) => Some(cfg.toProto30(thresh))
              case (_, None) => None
            }
            .groupBy(_.synchronizerIds)
            .map { case (domains, configs) =>
              configs.foldLeft(
                new SlowCounterParticipantDomainConfig(Seq.empty, Seq.empty, -1, -1, Seq.empty)
              ) { (next, previous) =>
                new SlowCounterParticipantDomainConfig(
                  domains, // since we have them grouped by synchronizerId, these are the same
                  next.distinguishedParticipantUids ++ previous.distinguishedParticipantUids,
                  Math.max(
                    next.thresholdDistinguished,
                    previous.thresholdDistinguished,
                  ), // we use max since they will have th same value (unless it uses the default from fold)
                  Math.max(
                    next.thresholdDefault,
                    previous.thresholdDefault,
                  ), // we use max since they will have th same value (unless it uses the default from fold)
                  next.participantUidsMetrics ++ previous.participantUidsMetrics,
                )
              }
            }
            .toSeq

        } yield GetConfigForSlowCounterParticipants.Response(
          filteredWithThreshold
        )
      }.asGrpcResponse
    }

  /** Get the number of intervals that counter-participants are behind in sending commitments.
    * Can be used to decide whether to ignore slow counter-participants w.r.t. pruning.
    */
  override def getIntervalsBehindForCounterParticipants(
      request: GetIntervalsBehindForCounterParticipants.Request
  ): Future[GetIntervalsBehindForCounterParticipants.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val result = for {
      domains <- wrapErrUS(
        request.synchronizerIds.traverse(SynchronizerId.fromProtoPrimitive(_, "synchronizer_id"))
      )
      participants <- wrapErrUS(
        request.counterParticipantUids.traverse(
          ParticipantId.fromProtoPrimitive(_, "counter_participant_uid")
        )
      )
      intervals <- EitherTUtil
        .fromFuture(
          syncStateInspection
            .getIntervalsBehindForParticipants(domains, participants),
          err => InspectionServiceError.InternalServerError.Error(err.toString),
        )
        .leftWiden[CantonError]

      mapped = intervals
        .filter(interval =>
          participants.contains(
            interval.participantId
          ) || (participants.isEmpty && request.threshold
            .fold(true)(interval.intervalsBehind.value > _))
        )
        .map(_.toProtoV30)
    } yield {
      GetIntervalsBehindForCounterParticipants.Response(mapped.toSeq)
    }

    CantonGrpcUtil.mapErrNewEUS(result)
  }

  /** Look up the ACS commitments computed and sent by a participant
    */
  override def lookupSentAcsCommitments(
      request: LookupSentAcsCommitments.Request
  ): Future[LookupSentAcsCommitments.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val result = for {

      domainSearchPeriodsF <-
        if (request.timeRanges.isEmpty) fetchDefaultDomainTimeRanges()
        else
          request.timeRanges
            .traverse(dtr => validateDomainTimeRange(dtr))
      result <- for {
        counterParticipantIds <- request.counterParticipantUids.traverse(pId =>
          ParticipantId
            .fromProtoPrimitive(pId, s"counter_participant_uids")
            .leftMap(err => err.message)
        )
        states <- request.commitmentState
          .map(CommitmentPeriodState.fromProtoV30)
          .sequence
          .leftMap(err => err.message)
      } yield {
        for {
          domainSearchPeriods <- Future.sequence(domainSearchPeriodsF)
        } yield syncStateInspection
          .crossDomainSentCommitmentMessages(
            domainSearchPeriods,
            counterParticipantIds,
            states,
            request.verbose,
          )
      }
    } yield result

    result
      .fold(
        string => Future.failed(new IllegalArgumentException(string)),
        _.map {
          case Left(string) => Future.failed(new IllegalArgumentException(string))
          case Right(value) =>
            Future.successful(
              LookupSentAcsCommitments.Response(SentAcsCommitment.toProtoV30(value))
            )
        },
      )
      .flatten
  }

  /** List the counter-participants of a participant and their ACS commitments together with the match status
    * TODO(#18749) R1 Can also be used for R1, to fetch commitments that a counter participant received from myself
    */
  override def lookupReceivedAcsCommitments(
      request: LookupReceivedAcsCommitments.Request
  ): Future[LookupReceivedAcsCommitments.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val result = for {
      domainSearchPeriodsF <-
        if (request.timeRanges.isEmpty) fetchDefaultDomainTimeRanges()
        else
          request.timeRanges
            .traverse(dtr => validateDomainTimeRange(dtr))

      result <- for {
        counterParticipantIds <- request.counterParticipantUids.traverse(pId =>
          ParticipantId
            .fromProtoPrimitive(pId, s"counter_participant_uids")
            .leftMap(err => err.message)
        )
        states <- request.commitmentState
          .map(CommitmentPeriodState.fromProtoV30)
          .sequence
          .leftMap(err => err.message)
      } yield {
        for {
          domainSearchPeriods <- Future.sequence(domainSearchPeriodsF)
        } yield syncStateInspection
          .crossDomainReceivedCommitmentMessages(
            domainSearchPeriods,
            counterParticipantIds,
            states,
            request.verbose,
          )
      }
    } yield result
    result match {
      case Left(string) => Future.failed(new IllegalArgumentException(string))
      case Right(value) =>
        value.map {
          case Left(string) => Future.failed(new IllegalArgumentException(string))
          case Right(value) =>
            Future.successful(
              LookupReceivedAcsCommitments.Response(ReceivedAcsCommitment.toProtoV30(value))
            )
        }.flatten
    }
  }

  private def fetchDefaultDomainTimeRanges()(implicit
      traceContext: TraceContext
  ): Either[String, Seq[Future[DomainSearchCommitmentPeriod]]] = {
    val searchPeriods = domainAliasManager.ids.map(synchronizerId =>
      for {
        domainAlias <- domainAliasManager
          .aliasForSynchronizerId(synchronizerId)
          .toRight(s"No domain alias found for $synchronizerId")
        lastComputed <- syncStateInspection
          .findLastComputedAndSent(domainAlias)
          .toRight(s"No computations done for $synchronizerId")
      } yield {
        for {
          indexedDomain <- IndexedDomain
            .indexed(indexedStringStore)(synchronizerId)
            .failOnShutdownToAbortException("fetchDefaultDomainTimeRanges")
        } yield DomainSearchCommitmentPeriod(
          indexedDomain,
          lastComputed.forgetRefinement,
          lastComputed.forgetRefinement,
        )
      }
    )

    val (lefts, rights) = searchPeriods.partitionMap(identity)

    NonEmpty.from(lefts) match {
      case Some(leftsNe) => Left(leftsNe.head1)
      case None => Right(rights.toSeq)
    }

  }

  private def validateDomainTimeRange(
      timeRange: DomainTimeRange
  )(implicit
      traceContext: TraceContext
  ): Either[String, Future[DomainSearchCommitmentPeriod]] =
    for {
      synchronizerId <- SynchronizerId.fromString(timeRange.synchronizerId)

      domainAlias <- domainAliasManager
        .aliasForSynchronizerId(synchronizerId)
        .toRight(s"No domain alias found for $synchronizerId")
      lastComputed <- syncStateInspection
        .findLastComputedAndSent(domainAlias)
        .toRight(s"No computations done for $synchronizerId")

      times <- timeRange.interval
        // when no timestamp is given we make a TimeRange of (lastComputedAndSentTs,lastComputedAndSentTs), this should give the latest elements since the comparison later on is inclusive.
        .fold(
          Right(TimeRange(Some(lastComputed.toProtoTimestamp), Some(lastComputed.toProtoTimestamp)))
        )(Right(_))
        .flatMap(interval =>
          for {
            min <- interval.fromExclusive
              .toRight(s"Missing start timestamp for ${timeRange.synchronizerId}")
              .flatMap(ts => CantonTimestamp.fromProtoTimestamp(ts).left.map(_.message))
            max <- interval.toInclusive
              .toRight(s"Missing end timestamp for ${timeRange.synchronizerId}")
              .flatMap(ts => CantonTimestamp.fromProtoTimestamp(ts).left.map(_.message))
          } yield (min, max)
        )
      (start, end) = times

    } yield {
      for {
        indexedDomain <- IndexedDomain
          .indexed(indexedStringStore)(synchronizerId)
          .failOnShutdownToAbortException("validateDomainTimeRange")
      } yield DomainSearchCommitmentPeriod(indexedDomain, start, end)
    }

  /** Request metadata about shared contracts used in commitment computation at a specific time
    * Subject to the data still being available on the participant
    * TODO(#9557) R2
    */
  override def openCommitment(
      request: OpenCommitment.Request,
      responseObserver: StreamObserver[OpenCommitment.Response],
  ): Unit =
    GrpcStreamingUtils.streamToClient(
      (out: OutputStream) => openCommitment(request, out),
      responseObserver,
      byteString => OpenCommitment.Response(byteString),
    )

  private def openCommitment(
      request: OpenCommitment.Request,
      out: OutputStream,
  ): Future[Unit] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val result =
      for {
        // 1. Check that the commitment to open matches a sent local commitment
        synchronizerId <- CantonGrpcUtil.wrapErrUS(
          SynchronizerId.fromProtoPrimitive(request.synchronizerId, "synchronizerId")
        )

        domainAlias <- EitherT
          .fromOption[FutureUnlessShutdown](
            domainAliasManager.aliasForSynchronizerId(synchronizerId),
            InspectionServiceError.IllegalArgumentError
              .Error(s"Unknown synchronizer id $synchronizerId"),
          )
          .leftWiden[CantonError]

        pv <- EitherTUtil
          .fromFuture(
            FutureUnlessShutdown.outcomeF(syncStateInspection.getProtocolVersion(domainAlias)),
            err => InspectionServiceError.InternalServerError.Error(err.toString),
          )
          .leftWiden[CantonError]

        cantonTickTs <- CantonGrpcUtil.wrapErrUS(
          ProtoConverter.parseRequired(
            CantonTimestamp.fromProtoTimestamp,
            "periodEndTick",
            request.periodEndTick,
          )
        )

        counterParticipant <- CantonGrpcUtil.wrapErrUS(
          ParticipantId
            .fromProtoPrimitive(
              request.computedForCounterParticipantUid,
              s"computedForCounterParticipantUid",
            )
        )

        computedCmts = syncStateInspection.findComputedCommitments(
          domainAlias,
          cantonTickTs,
          cantonTickTs,
          Some(counterParticipant),
        )

        // 2. Retrieve the contracts for the domain and the time of the commitment

        topologySnapshot <- EitherT.fromOption[FutureUnlessShutdown](
          ips.forDomain(synchronizerId),
          InspectionServiceError.InternalServerError.Error(
            s"Failed to retrieve ips for domain: $synchronizerId"
          ),
        )

        snapshot <- EitherTUtil
          .fromFuture(
            topologySnapshot.awaitSnapshotUS(cantonTickTs),
            err => InspectionServiceError.InternalServerError.Error(err.toString),
          )
          .leftWiden[CantonError]

        // Check that the given timestamp is a valid tick. We cannot move this check up, because .get(cantonTickTs)
        // would wait if the timestamp is in the future. Here, we already validated that the timestamp is in the past
        // by checking it corresponds to an already computed commitment, and already obtained a snapshot at the
        // given timestamp.
        domainParamsF <- EitherTUtil
          .fromFuture(
            DomainParametersLookup
              .forAcsCommitmentDomainParameters(
                pv,
                topologySnapshot,
                loggerFactory,
              )
              .get(cantonTickTs, warnOnUsingDefaults = false),
            err => InspectionServiceError.InternalServerError.Error(err.toString),
          )
          .leftWiden[CantonError]

        _ <- EitherT.fromEither[FutureUnlessShutdown](
          CantonTimestampSecond
            .fromCantonTimestamp(cantonTickTs)
            .flatMap(ts =>
              Either.cond(
                ts.getEpochSecond % domainParamsF.reconciliationInterval.duration.getSeconds == 0,
                (),
                "",
              )
            )
            .leftMap[CantonError](_ =>
              InspectionServiceError.IllegalArgumentError.Error(
                s"""The participant cannot open commitment ${request.commitment} for participant
                   | ${request.computedForCounterParticipantUid} on domain ${request.synchronizerId} because the given
                   | period end tick ${request.periodEndTick} is not a valid reconciliation interval tick""".stripMargin
              )
            )
        )

        _ <- EitherT.cond[FutureUnlessShutdown](
          computedCmts.exists { case (period, participant, cmt) =>
            period.fromExclusive < cantonTickTs && period.toInclusive >= cantonTickTs && participant == counterParticipant && cmt == request.commitment
          },
          (),
          InspectionServiceError.IllegalArgumentError.Error(
            s"""The participant cannot open commitment ${request.commitment} for participant
            ${request.computedForCounterParticipantUid} on domain ${request.synchronizerId} and period end
            ${request.periodEndTick} because the participant has not computed such a commitment at the given tick timestamp for the given counter participant""".stripMargin
          ): CantonError,
        )

        counterParticipantParties <- EitherTUtil
          .fromFuture(
            snapshot
              .inspectKnownParties(
                filterParty = "",
                filterParticipant = counterParticipant.filterString,
              ),
            err => InspectionServiceError.InternalServerError.Error(err.toString),
          )
          .leftWiden[CantonError]

        contractsAndTransferCounter <- EitherTUtil
          .fromFuture(
            syncStateInspection.activeContractsStakeholdersFilter(
              synchronizerId,
              cantonTickTs,
              counterParticipantParties.map(_.toLf),
            ),
            err => InspectionServiceError.InternalServerError.Error(err.toString),
          )
          .leftWiden[CantonError]

        commitmentContractsMetadata = contractsAndTransferCounter.map {
          case (cid, transferCounter) =>
            CommitmentContractMetadata.create(cid, transferCounter)(pv)
        }

      } yield {
        commitmentContractsMetadata.foreach(c => c.writeDelimitedTo(out).foreach(_ => out.flush()))
      }

    CantonGrpcUtil.mapErrNewEUS(result)
  }

  /** TODO(#9557) R2
    */
  override def inspectCommitmentContracts(
      request: InspectCommitmentContracts.Request,
      responseObserver: StreamObserver[InspectCommitmentContracts.Response],
  ): Unit =
    GrpcStreamingUtils.streamToClient(
      (out: OutputStream) => inspectCommitmentContracts(request, out),
      responseObserver,
      byteString => InspectCommitmentContracts.Response(byteString),
    )

  private def inspectCommitmentContracts(
      request: InspectCommitmentContracts.Request,
      out: OutputStream,
  ): Future[Unit] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val result =
      for {
        expectedSynchronizerId <- CantonGrpcUtil.wrapErrUS(
          SynchronizerId.fromProtoPrimitive(request.expectedSynchronizerId, "synchronizerId")
        )

        expectedDomainAlias <- EitherT
          .fromOption[FutureUnlessShutdown](
            domainAliasManager.aliasForSynchronizerId(expectedSynchronizerId),
            InspectionServiceError.IllegalArgumentError
              .Error(s"Domain alias not found for $expectedSynchronizerId"),
          )
          .leftWiden[CantonError]

        cantonTs <- CantonGrpcUtil.wrapErrUS(
          ProtoConverter.parseRequired(
            CantonTimestamp.fromProtoTimestamp,
            "timestamp",
            request.timestamp,
          )
        )

        pv <- EitherTUtil
          .fromFuture(
            FutureUnlessShutdown.outcomeF(
              syncStateInspection.getProtocolVersion(expectedDomainAlias)
            ),
            err => InspectionServiceError.InternalServerError.Error(err.toString),
          )
          .leftWiden[CantonError]

        cids <-
          EitherT.fromEither[FutureUnlessShutdown](
            request.cids
              .traverse(cid => LfContractId.fromBytes(Bytes.fromByteString(cid)))
              .leftMap(InspectionServiceError.IllegalArgumentError.Error(_))
              .leftWiden[CantonError]
          )

        contractStates <- EitherTUtil
          .fromFuture(
            CommitmentInspectContract
              .inspectContractState(
                cids,
                expectedSynchronizerId,
                cantonTs,
                request.downloadPayload,
                syncStateInspection,
                indexedStringStore,
                pv,
              ),
            err => InspectionServiceError.InternalServerError.Error(err.toString),
          )
          .leftWiden[CantonError]

      } yield {
        contractStates.foreach(c => c.writeDelimitedTo(out).foreach(_ => out.flush()))
      }
    CantonGrpcUtil.mapErrNewEUS(result)
  }

  override def countInFlight(
      request: CountInFlight.Request
  ): Future[CountInFlight.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val inFlightCount: EitherT[FutureUnlessShutdown, String, InFlightCount] = for {
      synchronizerId <- EitherT.fromEither[FutureUnlessShutdown](
        SynchronizerId.fromString(request.synchronizerId)
      )
      domainAlias <- EitherT.fromEither[FutureUnlessShutdown](
        domainAliasManager
          .aliasForSynchronizerId(synchronizerId)
          .toRight(s"Not able to find domain alias for ${synchronizerId.toString}")
      )

      count <- syncStateInspection.countInFlight(domainAlias)
    } yield {
      count
    }

    inFlightCount
      .fold(
        err => throw new IllegalArgumentException(err),
        count =>
          CountInFlight.Response(
            count.pendingSubmissions.unwrap,
            count.pendingTransactions.unwrap,
          ),
      )
      .asGrpcResponse

  }
}

object InspectionServiceError extends InspectionServiceErrorGroup {
  sealed trait InspectionServiceError extends CantonError

  @Explanation("""Inspection has failed because of an internal server error.""")
  @Resolution("Identify the error in the server log.")
  object InternalServerError
      extends ErrorCode(
        id = "INTERNAL_INSPECTION_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "An error occurred in the inspection service: " + reason
        )
        with InspectionServiceError
  }

  @Explanation("""Inspection has failed because of an illegal argument.""")
  @Resolution(
    "Identify the illegal argument in the error details of the gRPC status message that the call returned."
  )
  object IllegalArgumentError
      extends ErrorCode(
        id = "ILLEGAL_ARGUMENT_INSPECTION_ERROR",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "The inspection service received an illegal argument: " + reason
        )
        with InspectionServiceError
  }
}
