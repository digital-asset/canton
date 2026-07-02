// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.decrypter

import cats.data.EitherT
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.SynchronizerSnapshotSyncCryptoApi
import com.digitalasset.canton.data.LightTransactionViewTree
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.protocol.ProcessingSteps.DecryptedViews
import com.digitalasset.canton.participant.protocol.TransactionProcessor.TransactionProcessorError
import com.digitalasset.canton.protocol.messages.EncryptedViewMessage
import com.digitalasset.canton.sequencing.protocol.OpenEnvelope
import com.digitalasset.canton.store.ConfirmationRequestSessionKeyStore
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.nonempty.NonEmpty

import scala.concurrent.ExecutionContext

/** Interface for decrypting encrypted view messages. The decryption logic may differ based on the
  * protocol version.
  *
  * For PV up to and including v35, the decryption logic (V1) assumes view hashes are unique and may
  * fail if this assumption does not hold.
  *
  * For PV36 and above, we introduce a new decryption logic (V2) that no longer relies on view
  * hashes to guarantee uniqueness. Instead, it uses the ciphertext ID (i.e., the hash of a view’s
  * ciphertext) to identify a view.
  */
trait ViewMessageDecrypter {
  def decryptViews(
      batch: NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[TransactionViewType]]]]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionProcessorError, DecryptedViews[
    LightTransactionViewTree
  ]]
}

object ViewMessageDecrypter {
  def create(
      participantId: ParticipantId,
      protocolVersion: ProtocolVersion,
      sessionKeyStore: ConfirmationRequestSessionKeyStore,
      snapshot: SynchronizerSnapshotSyncCryptoApi,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): ViewMessageDecrypter =
    if (protocolVersion <= ProtocolVersion.v35)
      new ViewMessageDecrypterV1(
        participantId,
        protocolVersion,
        sessionKeyStore,
        snapshot,
        futureSupervisor,
        loggerFactory,
      )
    // TODO(#32393): Change to [[ViewMessageDecrypterV2]] after implementing the new decryption logic.
    else
      new ViewMessageDecrypterV1(
        participantId,
        protocolVersion,
        sessionKeyStore,
        snapshot,
        futureSupervisor,
        loggerFactory,
      )
}
