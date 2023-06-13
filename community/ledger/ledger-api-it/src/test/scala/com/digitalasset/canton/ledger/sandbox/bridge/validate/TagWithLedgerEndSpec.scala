// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.sandbox.bridge.validate

import com.daml.lf.data.Ref
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexService
import com.digitalasset.canton.ledger.sandbox.bridge.BridgeMetrics
import com.digitalasset.canton.ledger.sandbox.domain.{Rejection, Submission}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining.*

class TagWithLedgerEndSpec extends AnyFlatSpec with Matchers with MockitoSugar {
  behavior of classOf[TagWithLedgerEndImpl].getSimpleName

  // TODO(#13019) Avoid the global execution context
  @SuppressWarnings(Array("com.digitalasset.canton.GlobalExecutionContext"))
  private implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
  private implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.ForTesting

  private val indexServiceMock = mock[IndexService]
  private val tagWithLedgerEnd = new TagWithLedgerEndImpl(
    indexService = indexServiceMock,
    bridgeMetrics = new BridgeMetrics(NoOpMetricsFactory),
  )

  "tagWithLedgerEnd" should "tag the incoming submissions with the index service ledger end" in {
    val offsetString = Ref.HexString.assertFromString("ab")
    val expectedOffset = Offset.fromHexString(offsetString)
    val indexServiceProvidedOffset = domain.LedgerOffset.Absolute(offsetString)

    val somePreparedSubmission =
      mock[PreparedSubmission]
        .tap { preparedSubmission =>
          val someSubmission = mock[Submission].tap { submission =>
            when(submission.loggingContext).thenReturn(loggingContext)
          }
          when(preparedSubmission.submission).thenReturn(someSubmission)
        }

    when(indexServiceMock.currentLedgerEnd())
      .thenReturn(Future.successful(indexServiceProvidedOffset))

    tagWithLedgerEnd(Right(somePreparedSubmission))
      .map(_ shouldBe Right(expectedOffset -> somePreparedSubmission))
  }

  "tagWithLedgerEnd" should "propagate a rejection" in {
    val rejectionIn = Left(mock[Rejection])
    tagWithLedgerEnd(rejectionIn).map(_ shouldBe rejectionIn)
  }
}
