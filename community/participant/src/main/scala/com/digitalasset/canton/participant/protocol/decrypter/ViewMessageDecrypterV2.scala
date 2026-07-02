// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.decrypter

import cats.data.EitherT
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.SynchronizerSnapshotSyncCryptoApi
import com.digitalasset.canton.data.LightTransactionViewTree
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.ProcessingSteps.DecryptedViews
import com.digitalasset.canton.participant.protocol.TransactionProcessor.TransactionProcessorError
import com.digitalasset.canton.protocol.messages.EncryptedViewMessage
import com.digitalasset.canton.sequencing.protocol.OpenEnvelope
import com.digitalasset.canton.store.ConfirmationRequestSessionKeyStore
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.nonempty.NonEmpty

import scala.annotation.unused
import scala.concurrent.ExecutionContext

class ViewMessageDecrypterV2(
    @unused
    participantId: ParticipantId,
    @unused
    protocolVersion: ProtocolVersion,
    @unused
    sessionKeyStore: ConfirmationRequestSessionKeyStore,
    @unused
    snapshot: SynchronizerSnapshotSyncCryptoApi,
    @unused
    futureSupervisor: FutureSupervisor,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit @unused executionContext: ExecutionContext)
    extends NamedLogging
    with ViewMessageDecrypter {

  @unused
  def decryptViews(
      batch: NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[TransactionViewType]]]]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionProcessorError, DecryptedViews[
    LightTransactionViewTree
  ]] = ???
}
