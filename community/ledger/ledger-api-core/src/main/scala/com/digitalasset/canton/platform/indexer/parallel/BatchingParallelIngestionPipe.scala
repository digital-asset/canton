// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.digitalasset.canton.util.BatchN
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import scala.concurrent.Future

object BatchingParallelIngestionPipe {

  def apply[In, InBatch, DbElem, DbBatch, OutBatch](
      submissionBatchSize: Int,
      inputMappingParallelism: Int,
      contractReInsertion: Iterable[In] => Future[Iterable[In]],
      inputMapper: Iterable[In] => Future[InBatch],
      seqMapperZero: InBatch,
      seqMapper: (InBatch, InBatch) => InBatch,
      dbPrepareParallelism: Int,
      dbPrepare: InBatch => Future[Vector[DbElem]],
      ingestionParallelism: Int,
      ingestionMaxBatchSize: Int,
      ingestionMinBatchSize: Int,
      batcher: Iterable[DbElem] => Future[DbBatch],
      batcherCostFn: DbElem => Long,
      ingester: DbBatch => Future[DbBatch],
      tailConflateFlow: Flow[DbBatch, OutBatch, NotUsed],
      ingestTail: OutBatch => Future[OutBatch],
  ): Flow[In, OutBatch, NotUsed] =
    // The stream coming from ReadService, involves deserialization and translation to Update-s
    Flow[In]
      // Batching plus mapping to Database DTOs encapsulates all the CPU intensive computation of the ingestion. Executed in parallel.
      .via(
        BatchN.forMaxConcurrency(
          maxBatchSize = submissionBatchSize,
          maxBatchCount = inputMappingParallelism,
        )
      )
      .mapAsync(inputMappingParallelism)(contractReInsertion)
      .mapAsync(inputMappingParallelism)(inputMapper)
      // Encapsulates sequential/stateful computation (generation of sequential IDs for events)
      .scan(seqMapperZero)(seqMapper)
      .drop(1) // remove the zero element from the beginning of the stream
      .async
      .mapAsync(dbPrepareParallelism)(dbPrepare)
      .async
      .mapConcat(identity)
      .via(
        BatchN(
          maxBatchSize = ingestionMaxBatchSize,
          maxBatchCount = ingestionParallelism,
          minBatchSize = ingestionMinBatchSize,
          costFn = batcherCostFn,
        )
      )
      .mapAsync(ingestionParallelism)(batcher)
      // Inserting data into the database. Almost no CPU load here, threads are executing SQL commands over JDBC, and waiting for the result. This defines the parallelism on the SQL database side, same amount of PostgreSQL Backend processes will do the ingestion work.
      .mapAsync(ingestionParallelism)(ingester)
      // Batching data for throttled ledger-end update in database
      .via(tailConflateFlow)
      // Updating ledger-end and related data in database (this stage completion demarcates the consistent point-in-time)
      .mapAsync(1)(ingestTail)
}
