// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.Eval
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.{BatchAggregatorConfig, DefaultProcessingTimeouts}
import com.digitalasset.canton.participant.store.memory.InMemoryAcsDigestStore
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.ExecutionContext

class BatchingAcsDigestStoreTest extends AsyncWordSpec with BaseTest with AcsDigestStoreTest {
  private def mkStore(executionContext: ExecutionContext): AcsDigestStore = {
    val underlying =
      InMemoryAcsDigestStore.create(Eval.now(mockStringInterning), loggerFactory)(executionContext)
    new BatchingAcsDigestStore(
      underlying,
      BatchAggregatorConfig.defaultsForTesting,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    )(executionContext)
  }

  "BatchingAcsDigestStoreTest" should {
    behave like acsDigestSingleStoreTests(mkStore)

    behave like acsDigestMultiStoresTests((ec, _) => mkStore(ec))
  }
}
