// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.EitherT
import com.digitalasset.canton.crypto.{HashPurpose, SynchronizerCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  SignedContent,
  SubmissionRequest,
}
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

trait SignatureVerifier {

  /** Verifies the signature on a [[com.digitalasset.canton.sequencing.protocol.SubmissionRequest]].
    *
    * @param estimatedSequencingTimestamp
    *   The estimated current sequencing timestamp.
    */
  def verifySubmissionRequestSignature(
      signedSubmissionRequest: SignedContent[SubmissionRequest],
      hashPurpose: HashPurpose,
      estimatedSequencingTimestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): EitherT[
    FutureUnlessShutdown,
    String,
    SignedContent[SubmissionRequest],
  ]

  /** Verifies the signature on an
    * [[com.digitalasset.canton.sequencing.protocol.AcknowledgeRequest]]. Uses the acknowledged
    * timestamp for verification.
    *
    * @param timestampToVerify
    *   Override the timestamp to use for verification. This parameter exists only to preserve the
    *   behavior for PV <= 34, where the signature must be verified with the sequencing timestamp.
    */
  def verifyAcknowledgeRequestSignature(
      signedAcknowledgeRequest: SignedContent[AcknowledgeRequest],
      hashPurpose: HashPurpose,
      timestampToVerify: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
  )(implicit traceContext: TraceContext): EitherT[
    FutureUnlessShutdown,
    String,
    SignedContent[AcknowledgeRequest],
  ]
}

object SignatureVerifier {
  def apply(
      cryptoApi: SynchronizerCryptoClient
  )(implicit executionContext: ExecutionContext): SignatureVerifier = new SignatureVerifier {
    private def verifySignatureInternal[A <: ProtocolVersionedMemoizedEvidence](
        signedContent: SignedContent[A],
        hashPurpose: HashPurpose,
        sender: Member,
        timestampToVerify: CantonTimestamp,
        requestTypeStr: String,
        useSnapshotApproximation: Boolean,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, SignedContent[A]] = {
      val headSnapshotTs = cryptoApi.headSnapshot.ipsSnapshot.timestamp
      for {
        // If the current time is later than the head snapshot, create a hypothetical snapshot
        // (i.e., one referencing a timestamp possibly in the future) so that signature verification
        // uses the correct timestamp. This is necessary because submission requests are signed
        // using the local clock, so verification must be aligned accordingly.
        snapshotTuple <- EitherTUtil.fromFuture(
          if (timestampToVerify > headSnapshotTs)
            cryptoApi
              .hypotheticalSnapshot(
                headSnapshotTs,
                timestampToVerify,
              )
              .map((_, true))
          else if (useSnapshotApproximation)
            cryptoApi.currentSnapshotApproximation.map((_, false))
          else
            cryptoApi.snapshot(timestampToVerify).map((_, false)),
          err => s"Could not obtain a snapshot to verify the signature on [$requestTypeStr]: $err",
        )
        (snapshot, usingHypotheticalSnapshot) = snapshotTuple
        _ <- signedContent
          .verifySignature(
            snapshot,
            sender,
            hashPurpose,
          )
          .leftMap { error =>
            val errMsg =
              s"Sequencer could not verify client's signature ${signedContent.timestampOfSigningKey
                  .fold("")(ts => s"at $ts ")}on [$requestTypeStr] with sequencer's "
            if (usingHypotheticalSnapshot)
              errMsg + s"hypothetical snapshot (timestamp = $headSnapshotTs, " +
                s"desiredTimestamp =$timestampToVerify). Error: $error"
            else
              errMsg + s"current snapshot at $timestampToVerify. Error: $error"
          }
      } yield signedContent
    }

    override def verifySubmissionRequestSignature(
        signedSubmissionRequest: SignedContent[SubmissionRequest],
        hashPurpose: HashPurpose,
        estimatedSequencingTimestamp: CantonTimestamp,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, SignedContent[SubmissionRequest]] =
      verifySignatureInternal(
        signedSubmissionRequest,
        hashPurpose,
        signedSubmissionRequest.content.sender,
        estimatedSequencingTimestamp.immediateSuccessor,
        "SubmissionRequest",
        // TODO(#29607): Disable when `getTime` is fixed.
        useSnapshotApproximation = true,
      )

    override def verifyAcknowledgeRequestSignature(
        signedAcknowledgeRequest: SignedContent[AcknowledgeRequest],
        hashPurpose: HashPurpose,
        timestampToVerify: Option[CantonTimestamp],
        protocolVersion: ProtocolVersion,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, SignedContent[AcknowledgeRequest]] = {
      val (timestamp, useSnapshotApproximation) = timestampToVerify match {
        // we set `isSubmissionRequest` to true to ensure the same behavior as PV34 and earlier.
        case Some(timestamp) if protocolVersion < ProtocolVersion.v35 => (timestamp, true)
        case _ => (signedAcknowledgeRequest.content.timestamp, false)
      }
      verifySignatureInternal(
        signedAcknowledgeRequest,
        hashPurpose,
        signedAcknowledgeRequest.content.member,
        timestamp,
        "AcknowledgeRequest",
        useSnapshotApproximation,
      )
    }

  }
}
