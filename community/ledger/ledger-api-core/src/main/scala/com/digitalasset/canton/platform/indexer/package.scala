// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.digitalasset.canton.ledger.participant.state.Update
import com.digitalasset.canton.util.PekkoUtil.{Commit, FutureQueueConsumer}

import scala.concurrent.Future

package object indexer {

  /** Indexer is a factory for indexing. The outer `Future` completes when initialization is done up
    * to the point where the indexer is wirable for shutdown. The inner `Future` completes when
    * initialization finishes completely and yields the ready FutureQueueConsumer.
    */
  type Indexer = indexer.IndexerParams => Future[Future[FutureQueueConsumer[Update]]]

}

package indexer {

  import com.digitalasset.canton.util.PekkoUtil.ShutdownInProgress

  /** Parameters for creating an indexer instance.
    *
    * @param repairMode
    *   whether the indexer is running in repair mode
    * @param commit
    *   callback to confirm successful processing of an element
    * @param shutdownRequested
    *   probe to check whether shutdown has been requested
    */
  final case class IndexerParams(
      repairMode: Boolean,
      commit: Commit,
      shutdownRequested: ShutdownInProgress,
  )
}
