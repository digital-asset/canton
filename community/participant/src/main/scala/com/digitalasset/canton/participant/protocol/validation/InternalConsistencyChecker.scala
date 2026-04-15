// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.FullTransactionViewTree
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.protocol.validation.InternalConsistencyChecker.ErrorWithInternalConsistencyCheck
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

trait InternalConsistencyChecker {
  def check(
      rootViewTrees: NonEmpty[Seq[FullTransactionViewTree]]
  )(implicit
      traceContext: TraceContext
  ): Either[ErrorWithInternalConsistencyCheck, Unit]

}

object InternalConsistencyChecker {

  def apply(
      protocolVersion: ProtocolVersion,
      loggerFactory: NamedLoggerFactory,
  ): InternalConsistencyChecker =
    if (protocolVersion >= ProtocolVersion.v35) {
      NextGenInternalConsistencyChecker
    } else {
      new LegacyInternalConsistencyChecker(loggerFactory)
    }

  trait Error extends PrettyPrinting

  final case class ErrorWithInternalConsistencyCheck(error: Error) extends PrettyPrinting {
    override protected def pretty: Pretty[ErrorWithInternalConsistencyCheck] =
      prettyOfClass(
        unnamedParam(_.error)
      )
  }

}
