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

/** Class for decrypting encrypted view messages. The decryption logic may differ based on the
  * protocol version.
  *
  * For PV up to `transparency`, the decryption logic (V1) assumes view hashes are unique and may
  * fail if this assumption does not hold.
  *
  * For PV`transparency` and above, we introduce a new decryption logic (V2) that no longer relies
  * on view hashes to guarantee uniqueness. Instead, it uses the ciphertext ID (i.e., the hash of a
  * view’s ciphertext) to identify a view.
  */
final case class ViewMessageDecrypter(
    participantId: ParticipantId,
    sessionKeyStore: ConfirmationRequestSessionKeyStore,
    protocolVersion: ProtocolVersion,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext) {

  def decryptViews(
      batch: NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[TransactionViewType]]]],
      snapshot: SynchronizerSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionProcessorError, DecryptedViews[
    LightTransactionViewTree
  ]] =
    if (protocolVersion >= ProtocolVersion.transparency)
      // TODO(#32393): Change to [[ViewMessageDecrypterV2]] after implementing the new decryption logic.
      new ViewMessageDecrypterImplV1(
        participantId,
        sessionKeyStore,
        snapshot,
        protocolVersion,
        futureSupervisor,
        loggerFactory,
      ).decryptViews(batch)
    else
      new ViewMessageDecrypterImplV1(
        participantId,
        sessionKeyStore,
        snapshot,
        protocolVersion,
        futureSupervisor,
        loggerFactory,
      ).decryptViews(batch)
}
