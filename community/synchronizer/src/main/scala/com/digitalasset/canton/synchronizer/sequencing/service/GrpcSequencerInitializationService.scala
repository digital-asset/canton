// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functorFilter.*
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.{
  FieldNotSet,
  InvariantViolation,
  OtherError,
  ProtoDeserializationFailure,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencer.admin.v30.SequencerInitializationServiceGrpc.SequencerInitializationService
import com.digitalasset.canton.sequencer.admin.v30.{
  InitializeSequencerFromGenesisStateRequest,
  InitializeSequencerFromGenesisStateResponse,
  InitializeSequencerFromGenesisStateV2Request,
  InitializeSequencerFromGenesisStateV2Response,
  InitializeSequencerFromLsuPredecessorRequest,
  InitializeSequencerFromLsuPredecessorResponse,
  InitializeSequencerFromOnboardingStateV2Request,
  InitializeSequencerFromOnboardingStateV2Response,
}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.Synchronizer.FailedToInitialiseSynchronizerNode
import com.digitalasset.canton.synchronizer.sequencer.admin.grpc.{
  InitializeSequencerRequest,
  InitializeSequencerResponse,
}
import com.digitalasset.canton.synchronizer.sequencer.{
  OnboardingStateForSequencerV2,
  SequencerSnapshot,
}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
}
import com.digitalasset.canton.topology.transaction.{
  LsuAnnouncement,
  SequencerSynchronizerState,
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyMapping,
}
import com.digitalasset.canton.topology.{
  PhysicalSynchronizerId,
  SynchronizerId,
  TopologyManagerError,
}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.{EitherTUtil, GrpcStreamingUtils}
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class GrpcSequencerInitializationService(
    handler: GrpcSequencerInitializationService.Callback,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext,
    materializer: Materializer,
) extends SequencerInitializationService
    with NamedLogging {

  override def initializeSequencerFromGenesisState(
      responseObserver: StreamObserver[InitializeSequencerFromGenesisStateResponse]
  ): StreamObserver[InitializeSequencerFromGenesisStateRequest] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    GrpcStreamingUtils.streamFromClient(
      _.topologySnapshot,
      _.parameters,
      (
          topologySnapshot: ByteString,
          parameters: InitializeSequencerFromGenesisStateRequest.Parameters,
      ) => {
        val synchronizerParametersE = parameters match {
          case InitializeSequencerFromGenesisStateRequest.Parameters.V31(ssp) =>
            StaticSynchronizerParameters.fromProtoV31(ssp)
          case InitializeSequencerFromGenesisStateRequest.Parameters.V32(ssp) =>
            StaticSynchronizerParameters.fromProtoV32(ssp)
          case InitializeSequencerFromGenesisStateRequest.Parameters.V33(ssp) =>
            StaticSynchronizerParameters.fromProtoV33(ssp)
          case InitializeSequencerFromGenesisStateRequest.Parameters.Empty =>
            Left(FieldNotSet("InitializeSequencerFromGenesisStateRequest.parameters"))
        }

        initializeSequencerFromState(
          topologySnapshot,
          synchronizerParametersE,
          doResetTimes = true,
          ignoreLsuPsidCheck = false,
        ).map(InitializeSequencerFromGenesisStateResponse(_))
      },
      responseObserver,
    )
  }

  override def initializeSequencerFromLsuPredecessor(
      responseObserver: StreamObserver[InitializeSequencerFromLsuPredecessorResponse]
  ): StreamObserver[InitializeSequencerFromLsuPredecessorRequest] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    GrpcStreamingUtils.streamChunkedFromClient[
      InitializeSequencerFromLsuPredecessorRequest,
      InitializeSequencerFromLsuPredecessorResponse,
      (String, InitializeSequencerFromLsuPredecessorRequest.Parameters, Boolean),
      GenericStoredTopologyTransaction,
    ](
      responseObserver,
      emptyStreamError,
      getBytes = _.topologySnapshot,
      parseMessage = StoredTopologyTransaction.parseDelimitedFromTrusted(_),
    )(contextFromFirstRequest =
      req => Success((req.synchronizerId, req.parameters, req.ignorePsidCheck))
    ) { case ((synchronizerIdProto, parameters, ignoreLsuPsidCheck), source) =>
      val synchronizerParametersE = parameters match {
        case InitializeSequencerFromLsuPredecessorRequest.Parameters.V31(ssp) =>
          StaticSynchronizerParameters.fromProtoV31(ssp)
        case InitializeSequencerFromLsuPredecessorRequest.Parameters.V32(ssp) =>
          StaticSynchronizerParameters.fromProtoV32(ssp)
        case InitializeSequencerFromLsuPredecessorRequest.Parameters.V33(ssp) =>
          StaticSynchronizerParameters.fromProtoV33(ssp)
        case InitializeSequencerFromLsuPredecessorRequest.Parameters.Empty =>
          Left(FieldNotSet("InitializeSequencerFromLsuPredecessorRequest.parameters"))
      }

      for {
        synchronizerId <- SynchronizerId
          .fromProtoPrimitive(synchronizerIdProto, "synchronizer_id")
          .map(FutureUnlessShutdown.pure)
          .valueOr(err =>
            FutureUnlessShutdown.failed(ProtoDeserializationFailure.WrapNoLogging(err).toGrpcError)
          )
        _ <- FutureUnlessShutdown.outcomeF(
          initializeSequencerFromLsuPredecessorInternalStreaming(
            synchronizerId,
            source,
            synchronizerParametersE,
            doResetTimes = false,
            ignoreLsuPsidCheck = ignoreLsuPsidCheck,
          )
        )
      } yield InitializeSequencerFromLsuPredecessorResponse()
    }
  }

  /** Initializes the sequencer from a topology state snapshot.
    *
    * Used for major upgrades, logical upgrades, and initializing from an LSU predecessor.
    *
    * @param doResetTimes
    *   Determines whether sequenced and effective times should be reset to MinValue. Use `true` for
    *   genesis states (brand-new networks) to start a new timeline. Use `false` when initializing
    *   from an LSU predecessor to preserve historical timestamps and maintain continuity.
    *
    * @param ignoreLsuPsidCheck
    *   Only matters after LSU. If true, it will not be checked that that current PSId matches the
    *   successor in the topology state. Should be used *only* in disaster recovery scenarios (roll
    *   forward).
    * @return
    *   True if the sequencer is replicated.
    */
  // TODO(#37930): Broken implementation that requires the topology state to fit into a single bytestring of max 2GB
  private def initializeSequencerFromState(
      topologySnapshot: ByteString,
      synchronizerParametersE: ParsingResult[StaticSynchronizerParameters],
      doResetTimes: Boolean,
      ignoreLsuPsidCheck: Boolean,
  )(implicit traceContext: TraceContext): Future[Boolean] = {
    val res: EitherT[Future, RpcError, Boolean] = for {
      topologyState <- EitherT.fromEither[Future](
        StoredTopologyTransactions
          .fromTrustedByteString(topologySnapshot)
          .leftMap(ProtoDeserializationFailure.Wrap(_))
      )
      replicated <- initializeSequencerFromGenesisStateInternal(
        topologyState,
        synchronizerParametersE,
        doResetTimes = doResetTimes,
        ignoreLsuPsidCheck = ignoreLsuPsidCheck,
      )
    } yield replicated
    mapErrNew(res)
  }

  override def initializeSequencerFromGenesisStateV2(
      responseObserver: StreamObserver[InitializeSequencerFromGenesisStateV2Response]
  ): StreamObserver[InitializeSequencerFromGenesisStateV2Request] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    GrpcStreamingUtils.streamChunkedFromClient[
      InitializeSequencerFromGenesisStateV2Request,
      InitializeSequencerFromGenesisStateV2Response,
      InitializeSequencerFromGenesisStateV2Request.Parameters,
      GenericStoredTopologyTransaction,
    ](
      responseObserver,
      emptyStreamError,
      getBytes = _.topologySnapshot,
      parseMessage = StoredTopologyTransaction.parseDelimitedFromTrusted(_),
    )(contextFromFirstRequest = req => Success(req.parameters)) { (parameters, source) =>
      val synchronizerParametersE = parameters match {
        case InitializeSequencerFromGenesisStateV2Request.Parameters.SynchronizerParametersV31(
              ssp
            ) =>
          StaticSynchronizerParameters.fromProtoV31(ssp)
        case InitializeSequencerFromGenesisStateV2Request.Parameters.SynchronizerParametersV32(
              ssp
            ) =>
          StaticSynchronizerParameters.fromProtoV32(ssp)
        case InitializeSequencerFromGenesisStateV2Request.Parameters.V33(
              ssp
            ) =>
          StaticSynchronizerParameters.fromProtoV33(ssp)
        case InitializeSequencerFromGenesisStateV2Request.Parameters.Empty =>
          Left(FieldNotSet("InitializeSequencerFromGenesisStateV2Request.parameters"))
      }

      FutureUnlessShutdown.outcomeF(
        source
          .runWith(Sink.seq)
          .flatMap(
            initializeSequencerFromGenesisStateV2(
              _,
              synchronizerParametersE,
              doResetTimes = true,
              ignoreLsuPsidCheck = false,
            )
          )
          .map(InitializeSequencerFromGenesisStateV2Response(_))
      )
    }
  }

  private[service] def initializeSequencerFromLsuPredecessorInternalStreaming(
      synchronizerId: SynchronizerId,
      topologyStateSource: Source[GenericStoredTopologyTransaction, NotUsed],
      synchronizerParametersE: ParsingResult[StaticSynchronizerParameters],
      doResetTimes: Boolean,
      ignoreLsuPsidCheck: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Future[Boolean] = {
    val res = for {
      synchronizerParameters <- EitherT
        .fromEither[Future](synchronizerParametersE)
        .leftMap(ProtoDeserializationFailure.Wrap(_))
      // reset effective time and sequenced time if we are initializing the sequencer from the beginning
      genesisState = if (doResetTimes) resetTimes(topologyStateSource) else topologyStateSource

      initializeRequest = InitializeSequencerRequest(
        synchronizerId,
        genesisState,
        synchronizerParameters,
        None,
        ignoreLsuPsidCheck = ignoreLsuPsidCheck,
        isLsuSuccessorInit = true,
      )
      result <- handler
        .initialize(initializeRequest)
        .leftMap(FailedToInitialiseSynchronizerNode.Failure(_))
        .onShutdown(Left(FailedToInitialiseSynchronizerNode.Shutdown())): EitherT[
        Future,
        RpcError,
        InitializeSequencerResponse,
      ]
    } yield result.replicated

    mapErrNew(res)
  }

  // TODO(#37930): Broken implementation that requires the entire topology state to fit into memory
  private def initializeSequencerFromGenesisStateV2(
      topologySnapshot: Seq[GenericStoredTopologyTransaction],
      synchronizerParametersE: ParsingResult[StaticSynchronizerParameters],
      doResetTimes: Boolean,
      ignoreLsuPsidCheck: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Future[Boolean] =
    mapErrNew(
      initializeSequencerFromGenesisStateInternal(
        StoredTopologyTransactions(topologySnapshot),
        synchronizerParametersE,
        doResetTimes = doResetTimes,
        ignoreLsuPsidCheck = ignoreLsuPsidCheck,
      )
    )

  /** @param topologyState
    *   Topology state the sequencer should be initialized with.
    * @param synchronizerParametersE
    *   Static synchronizer parameters
    * @param doResetTimes
    *   Determines whether sequenced and effective times should be reset to MinValue. Use `true` for
    *   genesis states (brand-new networks) to start a new timeline. Use `false` when initializing
    *   from an LSU predecessor to preserve historical timestamps and maintain continuity.
    * @param ignoreLsuPsidCheck
    *   Only matters after LSU. If true, it will not be checked that that current PSId matches the
    *   successor in the topology state. Should be used *only* in disaster recovery scenarios (roll
    *   forward).
    */
  // TODO(#37930): Broken implementation that requires the entire topology state to fit into memory
  private def initializeSequencerFromGenesisStateInternal(
      topologyState: GenericStoredTopologyTransactions,
      synchronizerParametersE: ParsingResult[StaticSynchronizerParameters],
      doResetTimes: Boolean,
      ignoreLsuPsidCheck: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, RpcError, Boolean] =
    for {
      synchronizerParameters <- EitherT
        .fromEither[Future](synchronizerParametersE)
        .leftMap(ProtoDeserializationFailure.Wrap(_))
      // reset effective time and sequenced time if we are initializing the sequencer from the beginning
      genesisState: StoredTopologyTransactions[TopologyChangeOp, TopologyMapping] =
        if (doResetTimes) resetTimes(topologyState) else topologyState

      synchronizerIds = genesisState
        .collectOfMapping[SequencerSynchronizerState]
        .collectLatestByUniqueKey
        .result
        .map(_.mapping.synchronizerId)
        .toSet

      _ <- EitherTUtil.condUnitET[Future](
        synchronizerIds.sizeIs <= 1,
        TopologyManagerError.InconsistentTopologySnapshot.MultipleLogicalSynchronizerIds(
          synchronizerIds
        ),
      )

      synchronizerId <- EitherT.fromEither[Future](
        synchronizerIds.headOption.toRight[RpcError](
          TopologyManagerError.InconsistentTopologySnapshot.MissingSynchronizerSequencerState()
        )
      )

      physicalSynchronizerId = PhysicalSynchronizerId(synchronizerId, synchronizerParameters)
      expectedUpgradePsidO = genesisState
        .collectOfMapping[LsuAnnouncement]
        .collectLatestByUniqueKey
        .collectOfType[
          TopologyChangeOp.Replace
        ] // must happen after collectLatestByUniqueKey, not to miss a possible LSU cancellation
        .result
        .map(_.mapping.successorSynchronizerId)
        .headOption

      // If there's an announcement in the topology snapshot, we expect that the sequencer being initialized is the successor
      _ <- EitherT.fromEither[Future](
        expectedUpgradePsidO.fold(Right(()): Either[RpcError, Unit])(expectedUpgradePsid =>
          Either.cond(
            ignoreLsuPsidCheck || expectedUpgradePsid == physicalSynchronizerId.opaque,
            (),
            TopologyManagerError.InconsistentTopologySnapshot.UnexpectedPhysicalSynchronizerId(
              physicalSynchronizerId,
              expectedUpgradePsid,
            ),
          )
        )
      )

      // check that the snapshot is consistent with respect to effective proposals and effective fully authorized transactions
      multipleEffectivePerUniqueKey = genesisState.result
        .groupBy(_.transaction.mapping.uniqueKey)
        // only retain effective transactions
        .values
        .toSeq
        .flatMap { topoTxs =>
          val (proposals, fullyAuthorized) =
            topoTxs.filter(tx => tx.validUntil.isEmpty).partition(_.transaction.isProposal)

          val maxFullyAuthorizedSerialO = fullyAuthorized.view.map(_.serial).maxOption

          val allProposalsLessThanOrEqualMaxSerial = {
            val unexpectedEffectiveProposals = maxFullyAuthorizedSerialO.toList.flatMap {
              maxFullyAuthorizedSerial => proposals.filter(_.serial <= maxFullyAuthorizedSerial)
            }
            if (unexpectedEffectiveProposals.nonEmpty) {
              Seq(
                "effective proposals with serial less than or equal to the highest fully authorized transaction" -> unexpectedEffectiveProposals
              )
            } else Seq.empty
          }

          val multipleEffectiveProposalsWithDifferentSerials =
            if (proposals.map(_.serial).distinct.sizeIs > 1) {
              Seq("multiple effective proposals with different serials" -> proposals)
            } else Seq.empty

          val multipleEffectiveProposalsForSameTransactionHash =
            proposals
              .groupBy(_.hash)
              .filter(_._2.sizeIs > 1)
              .values
              .map(
                "multiple effective proposals for the same transaction hash" -> _
              )

          val multipleFullyAuthorized = if (fullyAuthorized.sizeIs > 1) {
            Seq("concurrently effective transactions with the same unique key" -> fullyAuthorized)
          } else Seq.empty

          multipleFullyAuthorized ++ multipleEffectiveProposalsForSameTransactionHash ++ allProposalsLessThanOrEqualMaxSerial ++ multipleEffectiveProposalsWithDifferentSerials
        }

      _ <- EitherTUtil.condUnitET[Future](
        multipleEffectivePerUniqueKey.isEmpty,
        TopologyManagerError.InconsistentTopologySnapshot.MultipleEffectiveMappingsPerUniqueKey(
          multipleEffectivePerUniqueKey
        ),
      )

      initializeRequest = InitializeSequencerRequest(
        synchronizerId,
        Source(genesisState.result),
        synchronizerParameters,
        None,
      )
      result <- handler
        .initialize(initializeRequest)
        .leftMap(FailedToInitialiseSynchronizerNode.Failure(_))
        .onShutdown(Left(FailedToInitialiseSynchronizerNode.Shutdown())): EitherT[
        Future,
        RpcError,
        InitializeSequencerResponse,
      ]
    } yield result.replicated

  /** Sets all sequencing and effective timestamps to
    * [[com.digitalasset.canton.data.CantonTimestamp.MinValue]].
    *
    * Filters out all ineffective proposals, too, because time resetting is incompatible with
    * proposals in the preprocessing of the topology snapshot in
    * [[com.digitalasset.canton.topology.processing.InitialTopologySnapshotValidator.validateAndApplyInitialTopologySnapshot]]
    * Concretely, `topologyState` might contain a proposal at timestamp `ts1` and the same topology
    * transaction as a fully authorized transaction at timestamp `ts2 > ts1` with the same serial.
    * This can happen if the authorization conditions have changed between `ts1` and `ts2`, e.g., by
    * reducing the threshold of a decentralized namespace. When all timestamps are reset to the same
    * value, preprocessing attempts to merge the validity periods of the now identical topology
    * transactions (except for the proposal flag) because we can store only one of them due to
    * uniqueness constraints. Yet, we cannot really merge them: If we retain the proposal flag on
    * the resulting transaction, then the fully authorized transaction is gone from the topology
    * state. Conversely, if we clear the proposal flag, then topology snapshot validation will fail
    * on the first transaction because the first transaction has not yet been fully authorized at
    * this point. Instead, we drop all expired proposals from the genesis state.
    *
    * Note: The same problem of varying authorization requirements can also appear within a single
    * sequenced topology transaction. However, compaction
    * [[com.digitalasset.canton.topology.transaction.SignedTopologyTransactions.compact]] prevents
    * the uniqueness violation as it removes the fully authorized topology transaction from the
    * sequence. For the genesis snapshot, however, we cannot enable compaction.
    */
  private def resetTime(
      stored: StoredTopologyTransaction[TopologyChangeOp, TopologyMapping]
  ): Option[StoredTopologyTransaction[TopologyChangeOp, TopologyMapping]] =
    Option.when(!stored.transaction.isProposal || stored.validUntil.isEmpty)(
      StoredTopologyTransaction(
        SequencedTime(SignedTopologyTransaction.InitialTopologySequencingTime),
        EffectiveTime(SignedTopologyTransaction.InitialTopologySequencingTime),
        stored.validUntil.map(_ =>
          EffectiveTime(SignedTopologyTransaction.InitialTopologySequencingTime)
        ),
        stored.transaction,
        stored.rejectionReason,
      )
    )

  private def resetTimes(
      topologyState: GenericStoredTopologyTransactions
  ): StoredTopologyTransactions[TopologyChangeOp, TopologyMapping] =
    StoredTopologyTransactions[TopologyChangeOp, TopologyMapping](
      topologyState.result.mapFilter(resetTime)
    )

  private def resetTimes[A](
      topologyStateSource: Source[GenericStoredTopologyTransaction, A]
  ): Source[StoredTopologyTransaction[TopologyChangeOp, TopologyMapping], A] =
    topologyStateSource.mapConcat(resetTime(_).toList)

  override def initializeSequencerFromOnboardingStateV2(
      responseObserver: StreamObserver[InitializeSequencerFromOnboardingStateV2Response]
  ): StreamObserver[InitializeSequencerFromOnboardingStateV2Request] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    def contextFromFirstMessage(
        state: OnboardingStateForSequencerV2
    ): Try[(StaticSynchronizerParameters, SequencerSnapshot, Option[SynchronizerId])] =
      for {
        snapshot <- state.sequencerSnapshot
          .toRight(
            ProtoDeserializationFailure
              .Wrap(ProtoDeserializationError.FieldNotSet("sequencer_snapshot"))
              .toGrpcError
          )
          .toTry
        params <- state.staticSynchronizerParameters
          .toRight(
            ProtoDeserializationFailure
              .Wrap(ProtoDeserializationError.FieldNotSet("static_synchronizer_parameters"))
              .toGrpcError
          )
          .toTry
        // the synchronizerId is only set if the onboarding state is exported on a sequencer running
        // for a synchronizer with PV=37. before that, the synchronizerId might not be set.
        // TODO(#37616): require that synchronizerId is set in the first message when removing PV36
      } yield (params, snapshot, state.synchronizerId)
    GrpcStreamingUtils.streamChunkedParsedFromClient[
      InitializeSequencerFromOnboardingStateV2Request,
      InitializeSequencerFromOnboardingStateV2Response,
      (StaticSynchronizerParameters, SequencerSnapshot, Option[SynchronizerId]),
      OnboardingStateForSequencerV2,
    ](
      responseObserver,
      emptyStreamError,
      getBytes = _.onboardingState,
      parseMessage = OnboardingStateForSequencerV2.parseDelimitedFromTrusted(_),
    )(contextFromFirstMessage = contextFromFirstMessage) {
      case ((staticParams, sequencerSnapshot, synchronizerIdO), source) =>
        val checkedSource = source
          .mapConcat { state =>
            val checkedForEqual = for {
              _ <- checkForEqual(
                "static_synchronizer_parameters",
                Some(staticParams),
                state.staticSynchronizerParameters,
              )
              _ <- checkForEqual(
                "sequencer_snapshot",
                Some(sequencerSnapshot),
                state.sequencerSnapshot,
              )
              _ <- checkForEqual(
                "synchronizerId",
                synchronizerIdO,
                state.synchronizerId,
              )
            } yield state.topologySnapshot

            checkedForEqual.valueOr(err => throw ProtoDeserializationFailure.Wrap(err).toGrpcError)
          }

        // if the first request specifies a synchronizerId, the topology state can be streamed.
        // otherwise the entire topology must be loaded into memory to extract the synchronizerId from a topology transaction.
        synchronizerIdO
          .map(
            initializeSequencerFromOnboardingStateV2Streaming(
              staticParams,
              sequencerSnapshot,
              _,
              checkedSource,
            )
          )
          .getOrElse(
            initializeSequencerFromOnboardingStateV2InMemory(
              staticParams,
              sequencerSnapshot,
              checkedSource,
            )
          )
    }
  }

  /** Loads the entire state into memory to extract the synchronizer id.
    */
  // TODO(#37616): Remove when removing support for PV=36
  private def initializeSequencerFromOnboardingStateV2InMemory(
      staticParams: StaticSynchronizerParameters,
      sequencerSnapshot: SequencerSnapshot,
      onboardingState: Source[GenericStoredTopologyTransaction, NotUsed],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[InitializeSequencerFromOnboardingStateV2Response] = {
    val res = for {
      fullTopologyState <- EitherT
        .right(onboardingState.runWith(Sink.seq))
      synchronizerId <- EitherT.fromEither[Future](
        fullTopologyState.iterator
          .flatMap(_.selectMapping[SequencerSynchronizerState])
          .map(_.mapping.synchronizerId)
          .nextOption()
          .toRight(
            TopologyManagerError.InconsistentTopologySnapshot
              .MissingSynchronizerSequencerState(): RpcError
          )
      )
      isReplicated <- initializeSequencerFromOnboardingStateInternal(
        Source(fullTopologyState),
        staticParams,
        sequencerSnapshot,
        synchronizerId,
      )
    } yield InitializeSequencerFromOnboardingStateV2Response(isReplicated)

    FutureUnlessShutdown.outcomeF(mapErrNew(res))
  }

  private def initializeSequencerFromOnboardingStateV2Streaming(
      staticParams: StaticSynchronizerParameters,
      sequencerSnapshot: SequencerSnapshot,
      synchronizerId: SynchronizerId,
      onboardingState: Source[GenericStoredTopologyTransaction, NotUsed],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[InitializeSequencerFromOnboardingStateV2Response] = {
    val res = initializeSequencerFromOnboardingStateInternal(
      onboardingState,
      staticParams,
      sequencerSnapshot,
      synchronizerId,
    ).map(InitializeSequencerFromOnboardingStateV2Response(_))

    FutureUnlessShutdown.outcomeF(mapErrNew(res))
  }

  private def emptyStreamError[A](implicit traceContext: TraceContext): Try[A] =
    Failure(
      ProtoDeserializationFailure
        .Wrap(OtherError("No elements were received in stream"))
        .asGrpcError
    )

  private def checkForEqual[A](
      field: String,
      x: Option[A],
      y: Option[A],
  ): Either[ProtoDeserializationError, Option[A]] = (x, y) match {
    case (None, None) => Right(None)
    case (Some(a), Some(b)) =>
      Either.cond(
        a == b,
        Some(b),
        InvariantViolation(field, s"Multiple provided values didn't match: a=[$a], b=[$b]"),
      )
    case (a, b) => Right(a.orElse(b))
  }

  private def initializeSequencerFromOnboardingStateInternal(
      topologySource: Source[GenericStoredTopologyTransaction, NotUsed],
      staticSynchronizerParameters: StaticSynchronizerParameters,
      sequencerSnapshot: SequencerSnapshot,
      synchronizerId: SynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, RpcError, Boolean] = {

    val initializeRequest = InitializeSequencerRequest(
      synchronizerId,
      topologySource,
      staticSynchronizerParameters,
      Some(sequencerSnapshot),
    )
    handler
      .initialize(initializeRequest)
      .bimap(
        FailedToInitialiseSynchronizerNode.Failure(_),
        result => result.replicated,
      )
      .onShutdown(Left(FailedToInitialiseSynchronizerNode.Shutdown()))
  }
}

object GrpcSequencerInitializationService {
  trait Callback {
    def initialize(request: InitializeSequencerRequest)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, InitializeSequencerResponse]
  }
}
