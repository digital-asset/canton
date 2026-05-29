// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.tracing.TraceContext

/** Interface for a store that can be entirely purged once none of the data is needed anymore.
  */
trait Purgeable {

  /** Purges all data from the store. This MUST ONLY be invoked when none of the data is needed
    * anymore for example on synchronizer migration once all the data has been reassigned to the new
    * synchronizer.
    */
  def purge()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]
}

/** Interface for a store that support chunk-wise purging.
  */
trait ChunkPurgeable {
  self: FlagCloseable =>

  /** Deletes a chunk of items from this store. No guarantees are made around transactionality, nor
    * about which specific items are deleted.
    *
    * After this call, the data should be considered corrupted and should no longer be read. The
    * intended use case is incremental cleanup after the store is no longer needed, e.g. after an
    * LSU, where calling deleteAllData may cause an undesirable load spike.
    *
    * @param chunkSize
    *   Number of items to delete.
    * @return
    *   Whether any items were found to delete.
    */
  def deleteDataChunk(chunkSize: PositiveInt)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean]
}
