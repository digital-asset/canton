// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.*
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

trait ClosedEnvelope extends Envelope[ByteString] {

  /** The semantic meaning of the bytes is different between the implementations */
  def bytes: ByteString

  def toOpenEnvelope(
      hashOps: HashOps,
      protocolVersion: ProtocolVersion,
  ): ParsingResult[DefaultOpenEnvelope]

  override def forRecipient(
      member: Member,
      groupAddresses: Set[GroupRecipient],
  ): Option[ClosedEnvelope]

  @VisibleForTesting
  def withRecipients(newRecipients: Recipients): ClosedEnvelope
}

object ClosedEnvelope {

  def verifySignatures(
      snapshot: SyncCryptoApi,
      sender: Member,
      content: ByteString,
      signatures: NonEmpty[Seq[Signature]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] = {
    val hash = snapshot.pureCrypto.digest(HashPurpose.SignedProtocolMessageSignature, content)
    snapshot.verifySignatures(hash, sender, signatures, SigningKeyUsage.ProtocolOnly)
  }

  def verifyMediatorSignatures(
      snapshot: SyncCryptoApi,
      mediatorGroupIndex: MediatorGroupIndex,
      content: ByteString,
      signatures: NonEmpty[Seq[Signature]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] = {
    val hash = snapshot.pureCrypto.digest(HashPurpose.SignedProtocolMessageSignature, content)
    snapshot.verifyMediatorSignatures(
      hash,
      mediatorGroupIndex,
      signatures,
      SigningKeyUsage.ProtocolOnly,
    )
  }

  def verifySequencerSignatures(
      snapshot: SyncCryptoApi,
      content: ByteString,
      signatures: NonEmpty[Seq[Signature]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit] = {
    val hash = snapshot.pureCrypto.digest(HashPurpose.SignedProtocolMessageSignature, content)
    snapshot.verifySequencerSignatures(hash, signatures, SigningKeyUsage.ProtocolOnly)
  }
}
