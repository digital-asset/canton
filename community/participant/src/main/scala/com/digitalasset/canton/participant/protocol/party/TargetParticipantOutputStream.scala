// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.data.EitherT
import cats.implicits.toTraverseOps
import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.data.ActiveContract
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.nonempty.NonEmpty

import java.io.OutputStream as JOutputStream
import scala.concurrent.ExecutionContext
import scala.util.Try

/** Contract handler that streams active contracts to a provided output stream instead of persisting
  * the contracts.
  */
class TargetParticipantOutputStream(
    out: JOutputStream,
    override protected val loggerFactory: NamedLoggerFactory,
) extends AcsReplicationContractHandler
    with NamedLogging {
  override def handleContracts(
      contracts: NonEmpty[Seq[ActiveContract]],
      checkpoint: AcsReplicationContractHandler.AcsTransferCheckpoint,
      synchronizerId: SynchronizerId,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, AcsReplicationContractHandler.AcsTransferCheckpoint] =
    EitherT.fromEither[FutureUnlessShutdown](
      contracts.forgetNE
        .traverse { activeContract =>
          logger.debug(
            s"Streaming remote-read contract to caller: ${activeContract.contract.createdEvent.map(_.contractId)}"
          )
          activeContract.writeDelimitedTo(out)
        }
        .flatMap { _ =>
          val delta = NonNegativeLong.tryCreate(contracts.size.toLong)
          logger.debug(s"Flushing $delta remote-read contracts at $checkpoint")
          Try(out.flush()).toEither
            .leftMap(e => s"Failed to flush remote-read contracts: $e")
            .map(_ =>
              checkpoint.copy(processedContractCount = checkpoint.processedContractCount + delta)
            )
        }
    )
}
