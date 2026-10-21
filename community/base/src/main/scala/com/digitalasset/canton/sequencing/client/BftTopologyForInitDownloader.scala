// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import com.digitalasset.canton.crypto.topology.TopologyStateHash
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.{
  TopologyStateForInitHashResponse,
  TopologyStateForInitRequest,
}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.retry.NoExceptionRetryPolicy
import com.digitalasset.canton.util.{ErrorUtil, retry}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import org.slf4j.event.Level

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

trait TopologyStateStream {
  def getBftInitTopologyStateHash(
      request: TopologyStateForInitRequest,
      acceptableSequencersO: Option[Set[SequencerId]],
  )(implicit traceContext: TraceContext): EitherT[
    FutureUnlessShutdown,
    String,
    TopologyStateForInitHashResponse,
  ]

  def streamTopologyState(
      request: TopologyStateForInitRequest,
      acceptableSequencersO: Option[Set[SequencerId]],
  )(implicit traceContext: TraceContext): Either[String, Source[
    GenericStoredTopologyTransaction,
    NotUsed,
  ]]
}

class BftTopologyForInitDownloader(protected override val loggerFactory: NamedLoggerFactory)
    extends NamedLogging {
  def streamAndVerifyTopologyTxs(
      topologyStateStream: TopologyStateStream,
      maxRetries: Int,
      retryLogLevel: Option[Level],
      retryDelay: FiniteDuration,
      request: TopologyStateForInitRequest,
      acceptableSequencersO: Option[Set[SequencerId]],
  )(implicit
      closeContext: CloseContext,
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Source[GenericStoredTopologyTransaction, NotUsed]] =
    for {
      expectedBftHash <- {
        EitherT(
          retry
            .Pause(
              logger = logger,
              hasSynchronizeWithClosing = closeContext.context,
              maxRetries = maxRetries,
              delay = retryDelay,
              operationName = "Get hash for init topology state",
              retryLogLevel = retryLogLevel,
            )
            .unlessShutdown(
              topologyStateStream.getBftInitTopologyStateHash(request, acceptableSequencersO).value,
              NoExceptionRetryPolicy,
            )
        )
      }
      _ = logger.info(s"Expecting topology state for init with hash $expectedBftHash")

      source <- EitherT.fromEither(
        topologyStateStream.streamTopologyState(request, acceptableSequencersO)
      )
    } yield {
      // compute the hash on the fly and fail the source at the end, in case the computed hash does not match
      // the expected hash that was retrieved from the quorum of sequencers
      source.foldConcat(init = (TopologyStateHash.build(), 0 /* numTransactions */ ))(update = {
        case ((hashBuilder, numTransactions), transaction) =>
          (hashBuilder.add(transaction), numTransactions + 1)
      })(
        continue = { case (hashBuilder, numTransactions) =>
          val computedHash = hashBuilder.finish().hash
          if (computedHash == expectedBftHash.topologyStateHash) {
            logger.info(
              s"Successfully downloaded topology state of $numTransactions for init with hash matching expected $computedHash"
            )
            Source.empty[GenericStoredTopologyTransaction]
          } else {
            // Not using Source.failed for consistency with other invalidState calls and to make sure we log in the error
            Source.future(
              ErrorUtil.invalidStateAsync(
                s"Bft hash mismatch for downloaded topology state for init (hash = $computedHash, expected hash = ${expectedBftHash.topologyStateHash})"
              )
            )
          }
        }
      )
    }

}

/** This class encapsulates the logic to download topology txs and verify them using a hash obtained
  * via a separate procedure (e.g. a threshold read from multiple sequencers).
  */
object BftTopologyForInitDownloader {
  def downloadAndVerifyTopologyTxs(
      maxRetries: Int,
      retryLogLevel: Option[Level],
      retryDelay: FiniteDuration,
      request: TopologyStateForInitRequest,
      loggerFactory: NamedLoggerFactory,
      getBftInitTopologyStateHash: (
          TopologyStateForInitRequest,
          Option[Set[SequencerId]],
      ) => EitherT[
        FutureUnlessShutdown,
        String,
        TopologyStateForInitHashResponse,
      ],
      downloadSnapshot: (TopologyStateForInitRequest, Option[Set[SequencerId]]) => EitherT[
        FutureUnlessShutdown,
        String,
        GenericStoredTopologyTransactions,
      ],
      acceptableSequencersO: Option[Set[SequencerId]],
  )(implicit
      closeContext: CloseContext,
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, GenericStoredTopologyTransactions] = {
    val logger = loggerFactory.getTracedLogger(this.getClass)
    for {
      expectedBftHash <- {
        EitherT(
          retry
            .Pause(
              logger = logger,
              hasSynchronizeWithClosing = closeContext.context,
              maxRetries = maxRetries,
              delay = retryDelay,
              operationName = "Get hash for init topology state",
              retryLogLevel = retryLogLevel,
            )
            .unlessShutdown(
              getBftInitTopologyStateHash(request, acceptableSequencersO).value,
              NoExceptionRetryPolicy,
            )
        )
      }
      _ = logger.info(s"Expecting topology state for init with hash $expectedBftHash")
      topologyTransactions <- {
        EitherT(
          retry
            .Pause(
              logger = logger,
              hasSynchronizeWithClosing = closeContext.context,
              maxRetries = maxRetries,
              delay = retryDelay,
              operationName = "Download topology state for init",
              retryLogLevel = retryLogLevel,
            )
            .unlessShutdown(
              {
                val hashBuilder = TopologyStateHash.build()
                downloadSnapshot(request, acceptableSequencersO).value.map {
                  case Right(topologyTransactions) =>
                    topologyTransactions.result.foreach { tx =>
                      hashBuilder.add(tx).discard
                    }
                    val computedHash = hashBuilder.finish().hash
                    if (computedHash == expectedBftHash.topologyStateHash) {
                      logger.info(
                        s"Successfully downloaded topology state of ${topologyTransactions.result.size} for init with hash matching expected $computedHash"
                      )
                      Right(topologyTransactions)
                    } else {
                      Left(
                        s"Bft hash mismatch for downloaded topology state for init (hash = $computedHash, expected hash = ${expectedBftHash.topologyStateHash})"
                      )
                    }
                  case err @ Left(_) => err
                }
              },
              NoExceptionRetryPolicy,
            )
        )
      }
    } yield topologyTransactions
  }
}
