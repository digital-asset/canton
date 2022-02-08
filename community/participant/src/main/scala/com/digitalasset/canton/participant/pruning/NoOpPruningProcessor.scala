// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.data.EitherT
import cats.syntax.either._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.Pruning.{
  LedgerPruningError,
  LedgerPruningOnlySupportedInEnterpriseEdition,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

trait PruningProcessor extends AutoCloseable {
  def pruneLedgerEvents(pruneUpToInclusive: GlobalOffset)(implicit
      traceContext: TraceContext
  ): EitherT[Future, LedgerPruningError, Unit]

  def safeToPrune(beforeOrAt: CantonTimestamp, boundInclusive: GlobalOffset)(implicit
      traceContext: TraceContext
  ): EitherT[Future, LedgerPruningError, Option[GlobalOffset]]
}

object NoOpPruningProcessor extends PruningProcessor {
  override def pruneLedgerEvents(pruneUpToInclusive: GlobalOffset)(implicit
      traceContext: TraceContext
  ): EitherT[Future, LedgerPruningError, Unit] =
    NoOpPruningProcessor.pruningNotSupportedET

  override def safeToPrune(beforeOrAt: CantonTimestamp, boundInclusive: GlobalOffset)(implicit
      traceContext: TraceContext
  ): EitherT[Future, LedgerPruningError, Option[GlobalOffset]] =
    NoOpPruningProcessor.pruningNotSupportedET

  override def close(): Unit = ()

  private val pruningNotSupported: LedgerPruningError =
    LedgerPruningOnlySupportedInEnterpriseEdition(
      "Pruning of internal participant data only available in the Enterprise Edition."
    )
  private def pruningNotSupportedET[A]: EitherT[Future, LedgerPruningError, A] =
    EitherT(Future.successful(Either.left(pruningNotSupported)))
}
