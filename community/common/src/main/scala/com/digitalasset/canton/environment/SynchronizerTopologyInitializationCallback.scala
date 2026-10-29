// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.data.EitherT
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.UnlessShutdown.Outcome
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.topology.client.SynchronizerTopologyClientWithInit
import com.digitalasset.canton.topology.processing.InitialTopologySnapshotValidator
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.util.retry
import com.digitalasset.canton.util.retry.AllExceptionRetryPolicy
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.StatusRuntimeException
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

trait SynchronizerTopologyInitializationCallback {
  def callback(
      topologyStoreInitialization: InitialTopologySnapshotValidator,
      topologyClient: SynchronizerTopologyClientWithInit,
      sequencerClient: SequencerClient,
      protocolVersion: ProtocolVersion,
  )(implicit
      closeContext: CloseContext,
      executionContext: ExecutionContext,
      elc: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, String, Unit]
}

class StoreBasedSynchronizerTopologyInitializationCallback
    extends SynchronizerTopologyInitializationCallback {
  override def callback(
      topologyStoreInitialization: InitialTopologySnapshotValidator,
      topologyClient: SynchronizerTopologyClientWithInit,
      sequencerClient: SequencerClient,
      protocolVersion: ProtocolVersion,
  )(implicit
      closeContext: CloseContext,
      executionContext: ExecutionContext,
      elc: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    implicit val traceContext = elc.traceContext

    downloadWithRetries(
      topologyStoreInitialization,
      topologyClient,
      () =>
        sequencerClient
          .streamTopologyStateForInit(maxRetries = retry.Forever, retryLogLevel = None),
    )
  }

  private[environment] def downloadWithRetries(
      topologyStoreInitialization: InitialTopologySnapshotValidator,
      topologyClient: SynchronizerTopologyClientWithInit,
      mkTopologyTransactionSource: () => EitherT[
        FutureUnlessShutdown,
        String,
        Source[GenericStoredTopologyTransaction, NotUsed],
      ],
  )(implicit
      closeContext: CloseContext,
      executionContext: ExecutionContext,
      elc: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    implicit val traceContext = elc.traceContext

    def attemptDownload: EitherT[FutureUnlessShutdown, String, Unit] = for {
      topologyTransactionsSource <- mkTopologyTransactionSource()
      _ <- topologyStoreInitialization.validateAndApplyInitialTopologySnapshot(
        topologyTransactionsSource
      )
      _ <- EitherT.right[String](topologyClient.updateKnownTimestampsDuringStartup())
    } yield ()

    // Since the source only actually sends the download request upon materialization and running the graph
    // the retry loop has to wrap the full run of the pipeline (as opposed to closer to the actual gRPC call further down the stack)
    val overallResult = retry
      .Pause(
        logger = elc.logger,
        hasSynchronizeWithClosing = closeContext.context,
        maxRetries = retry.Forever,
        delay = 1.second,
        operationName = "Stream the initial topology state",
        retryLogLevel = None,
      )
      .unlessShutdown(
        attemptDownload.value.recover { case grpcExc: StatusRuntimeException =>
          elc.logger.debug(
            s"Streaming the topology state for initialization failed with gRPC exception",
            grpcExc,
          )
          val grpcError =
            GrpcError(
              "streaming-topology-state-for-init",
              "some sequencer connection",
              grpcExc,
            )
          Outcome(Left(grpcError.toString))
        },
        AllExceptionRetryPolicy,
      )
    EitherT(overallResult)
  }
}
