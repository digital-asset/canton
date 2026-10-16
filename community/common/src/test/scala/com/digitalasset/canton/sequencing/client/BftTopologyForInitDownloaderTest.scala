// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import com.digitalasset.canton.crypto.topology.TopologyStateHash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
}
import com.digitalasset.canton.sequencing.protocol.{
  TopologyStateForInitHashResponse,
  TopologyStateForInitRequest,
}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
}
import com.digitalasset.canton.topology.{DefaultTestIdentities, SequencerId, TestingOwnerWithKeys}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

class BftTopologyForInitDownloaderTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with HasCloseContext
    with FlagCloseable {

  private val testMember = DefaultTestIdentities.mediatorId
  private val testRequest = TopologyStateForInitRequest(testMember)(
    TopologyStateForInitRequest.protocolVersionRepresentativeFor(testedProtocolVersion)
  )
  private val testingTransactions =
    new TestingOwnerWithKeys(testMember, loggerFactory, executorService).TestingTransactions
  private val tx1 = StoredTopologyTransaction(
    sequenced = SequencedTime(CantonTimestamp.Epoch),
    validFrom = EffectiveTime(CantonTimestamp.Epoch),
    validUntil = None,
    transaction = testingTransactions.dpc1,
    rejectionReason = None,
  )
  private val tx2 = StoredTopologyTransaction(
    sequenced = SequencedTime(CantonTimestamp.Epoch),
    validFrom = EffectiveTime(CantonTimestamp.Epoch),
    validUntil = None,
    transaction = testingTransactions.ns1k2,
    rejectionReason = None,
  )
  private val validHash = {
    val hashBuilder = TopologyStateHash.build()
    hashBuilder.add(tx1)
    hashBuilder.add(tx2)

    TopologyStateForInitHashResponse(
      hashBuilder.finish().hash
    )
  }
  private val invalidHash = {
    val hashBuilder = TopologyStateHash.build()
    hashBuilder.add(tx1)
    TopologyStateForInitHashResponse(
      hashBuilder.finish().hash
    )
  }

  private val txs = Seq(
    tx1,
    tx2,
  )

  val testDownloadResponse: GenericStoredTopologyTransactions =
    StoredTopologyTransactions(txs)

  "downloadAndVerifyTopologyTxs" should {
    implicit val closeContext: CloseContext = CloseContext(this)

    "fail on download mismatch with expected bft hash" in {

      val errorMessage = BftTopologyForInitDownloader
        .downloadAndVerifyTopologyTxs(
          maxRetries = 0,
          retryLogLevel = None,
          retryDelay = 1.second,
          request = testRequest,
          loggerFactory = loggerFactory,
          getBftInitTopologyStateHash =
            (_, _) => EitherT.pure[FutureUnlessShutdown, String](invalidHash),
          downloadSnapshot =
            (_, _) => EitherT.pure[FutureUnlessShutdown, String](testDownloadResponse),
          acceptableSequencersO = None,
        )
        .futureValueUS
        .leftOrFail("Expected a hash disagreement")

      errorMessage should include("Bft hash mismatch for downloaded topology state for init")
    }

    "succeed on download matching expected bft hash" in {

      val response = BftTopologyForInitDownloader
        .downloadAndVerifyTopologyTxs(
          maxRetries = 0,
          retryLogLevel = None,
          retryDelay = 1.second,
          request = testRequest,
          loggerFactory = loggerFactory,
          getBftInitTopologyStateHash =
            (_, _) => EitherT.pure[FutureUnlessShutdown, String](validHash),
          downloadSnapshot =
            (_, _) => EitherT.pure[FutureUnlessShutdown, String](testDownloadResponse),
          acceptableSequencersO = None,
        )
        .futureValueUS
        .valueOrFail("Expected a successful download")

      response shouldEqual testDownloadResponse
    }
  }

  "streamAndVerifyTopologyTxs" should {
    implicit val actorSystem: ActorSystem =
      PekkoUtil.createActorSystem(loggerFactory.threadName)

    val downloader = new BftTopologyForInitDownloader(loggerFactory)

    def makeFailingTopologyStateStream() = new TopologyStateStream {
      private val callCount = new AtomicInteger(0)

      override def getBftInitTopologyStateHash(
          request: TopologyStateForInitRequest,
          acceptableSequencersO: Option[Set[SequencerId]],
      )(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, String, TopologyStateForInitHashResponse] =
        if (callCount.getAndAdd(1) < 3) {
          EitherT.leftT[FutureUnlessShutdown, TopologyStateForInitHashResponse](
            "No connection available to get initial topology state hash"
          )
        } else {
          EitherT.pure[FutureUnlessShutdown, String](validHash)
        }

      override def streamTopologyState(
          request: TopologyStateForInitRequest,
          acceptableSequencersO: Option[Set[SequencerId]],
      )(implicit
          traceContext: TraceContext
      ): Either[String, Source[GenericStoredTopologyTransaction, NotUsed]] =
        Right(Source(txs))
    }

    def makeTransactionSource(
        topologyStateStream: TopologyStateStream,
        maxRetries: Int,
    ): Either[String, Source[GenericStoredTopologyTransaction, NotUsed]] =
      downloader
        .streamAndVerifyTopologyTxs(
          topologyStateStream = topologyStateStream,
          maxRetries = maxRetries,
          retryLogLevel = None,
          retryDelay = 1.second,
          request = testRequest,
          acceptableSequencersO = None,
        )
        .futureValueUS

    "succeed in a simple case" in {
      val topologyStateStream = new TopologyStateStream {
        override def getBftInitTopologyStateHash(
            request: TopologyStateForInitRequest,
            acceptableSequencersO: Option[Set[SequencerId]],
        )(implicit
            traceContext: TraceContext
        ): EitherT[FutureUnlessShutdown, String, TopologyStateForInitHashResponse] =
          EitherT.pure[FutureUnlessShutdown, String](validHash)

        override def streamTopologyState(
            request: TopologyStateForInitRequest,
            acceptableSequencersO: Option[Set[SequencerId]],
        )(implicit
            traceContext: TraceContext
        ): Either[String, Source[GenericStoredTopologyTransaction, NotUsed]] =
          Right(Source(txs))
      }

      val source = makeTransactionSource(topologyStateStream, maxRetries = 0)

      source
        .getOrElse(throw new IllegalStateException())
        .runWith(Sink.seq)
        .futureValue shouldBe txs
    }

    "succeed eventually after several failed attempts" in {
      val topologyStateStream = makeFailingTopologyStateStream()

      val source = makeTransactionSource(topologyStateStream, maxRetries = 3)

      source
        .getOrElse(throw new IllegalStateException())
        .runWith(Sink.seq)
        .futureValue shouldBe txs
    }

    "fail if it runs out of the retries" in {
      val topologyStateStream = makeFailingTopologyStateStream()

      val source = makeTransactionSource(topologyStateStream, maxRetries = 2)

      source shouldBe Left("No connection available to get initial topology state hash")
    }
  }
}
