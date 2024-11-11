// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.syntax.either.*
import cats.syntax.parallel.*
import com.digitalasset.canton.crypto.{
  DomainSnapshotSyncCryptoApi,
  Hash,
  InteractiveSubmission,
  Signature,
}
import com.digitalasset.canton.data.{FullTransactionViewTree, SubmitterMetadata, ViewPosition}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps.ParsedTransactionRequest
import com.digitalasset.canton.participant.protocol.validation.ModelConformanceChecker.LazyAsyncReInterpretation
import com.digitalasset.canton.protocol.{ExternalAuthorization, RequestId}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

class AuthenticationValidator(implicit
    executionContext: ExecutionContext
) {

  private[protocol] def verifyViewSignatures(
      parsedRequest: ParsedTransactionRequest,
      reInterpretedTopLevelViewsEval: LazyAsyncReInterpretation,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[ViewPosition, String]] =
    verifyViewSignatures(
      parsedRequest.requestId,
      parsedRequest.rootViewTreesWithSignatures.forgetNE,
      parsedRequest.snapshot,
      reInterpretedTopLevelViewsEval,
      domainId,
      protocolVersion,
    )

  private def verifyViewSignatures(
      requestId: RequestId,
      rootViews: Seq[(FullTransactionViewTree, Option[Signature])],
      snapshot: DomainSnapshotSyncCryptoApi,
      reInterpretedTopLevelViews: LazyAsyncReInterpretation,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[ViewPosition, String]] = {
    def err(details: String): String =
      show"Received a request with id $requestId with a view that is not correctly authenticated. Rejecting request...\n$details"

    def verifySignatures(
        viewWithSignature: (FullTransactionViewTree, Option[Signature])
    ): FutureUnlessShutdown[Option[(ViewPosition, String)]] = {

      val (view, signatureO) = viewWithSignature

      view.submitterMetadataO match {
        // RootHash -> is a blinded tree
        case None => FutureUnlessShutdown.pure(None)
        // SubmitterMetadata -> information on the submitter of the tree
        case Some(submitterMetadata: SubmitterMetadata) =>
          for {
            participantSignatureCheck <- verifyParticipantSignature(
              view,
              signatureO,
              submitterMetadata,
            )
            externalSignatureCheck <- verifyExternalPartySignature(
              view,
              submitterMetadata,
              snapshot,
              protocolVersion,
            )
          } yield participantSignatureCheck.orElse(externalSignatureCheck)
      }
    }

    def verifyParticipantSignature(
        view: FullTransactionViewTree,
        signatureO: Option[Signature],
        submitterMetadata: SubmitterMetadata,
    ): FutureUnlessShutdown[Option[(ViewPosition, String)]] =
      signatureO match {
        case Some(signature) =>
          (for {
            // Verify the participant signature
            _ <- snapshot
              .verifySignature(
                view.rootHash.unwrap,
                submitterMetadata.submittingParticipant,
                signature,
              )
              .leftMap(_.show)
              .mapK(FutureUnlessShutdown.outcomeK)
          } yield ()).fold(
            cause =>
              Some(
                (
                  view.viewPosition,
                  err(s"View ${view.viewPosition} has an invalid signature: $cause."),
                )
              ),
            _ => None,
          )

        case None =>
          // the signature is missing
          FutureUnlessShutdown.pure(
            Some(
              (
                view.viewPosition,
                err(s"View ${view.viewPosition} is missing a signature."),
              )
            )
          )

      }

    // Checks that the provided external signatures are valid for the transaction
    def verifyExternalPartySignature(
        viewTree: FullTransactionViewTree,
        submitterMetadata: SubmitterMetadata,
        topology: DomainSnapshotSyncCryptoApi,
        protocolVersion: ProtocolVersion,
    ): FutureUnlessShutdown[Option[(ViewPosition, String)]] = {
      // Re-compute the hash from the re-interpreted transaction and necessary metadata, and verify the signature
      def computeHashAndVerifyExternalSignature(
          externalAuthorization: ExternalAuthorization
      ): FutureUnlessShutdown[Option[(ViewPosition, String)]] =
        reInterpretedTopLevelViews.get(viewTree.view.viewHash) match {
          case Some(reInterpretationET) =>
            // At this point we have to run interpretation on the view to get the necessary data to re-compute the hash
            reInterpretationET.value.value.flatMap {
              case Left(error) =>
                FutureUnlessShutdown.pure(
                  Some(
                    viewTree.viewPosition -> err(
                      s"Failed to re-interpret transaction in order to compute externally signed hash: $error"
                    )
                  )
                )
              case Right(reInterpretedTopLevelView) =>
                reInterpretedTopLevelView
                  .computeHash(
                    externalAuthorization.hashingSchemeVersion,
                    submitterMetadata.actAs,
                    submitterMetadata.commandId.unwrap,
                    viewTree.transactionUuid,
                    viewTree.mediator.group.value,
                    domainId,
                    protocolVersion,
                  )
                  // If Hash computation is successful, verify the signature is valid
                  .map(verifyExternalSignature(_, externalAuthorization))
                  .map(
                    _.map(signatureError => signatureError.map(err).map(viewTree.viewPosition -> _))
                  )
                  // If we couldn't compute the hash, fail
                  .valueOr(error =>
                    FutureUnlessShutdown.pure(
                      Some(
                        viewTree.viewPosition -> err(
                          s"Failed to compute externally signed hash: $error"
                        )
                      )
                    )
                  )
            }
          case None =>
            // If we don't have the re-interpreted transaction for this view it's either a programming error
            // (we didn't interpret all available roots in reInterpretedTopLevelViews, or we're missing the top level view entirely
            // despite having the submitterMetadata, which is also wrong
            FutureUnlessShutdown.pure(
              Some(
                viewTree.viewPosition -> err(
                  "Missing top level view to validate external signature"
                )
              )
            )
        }

      def verifyExternalSignature(
          hash: Hash,
          externalAuthorization: ExternalAuthorization,
      ): FutureUnlessShutdown[Option[String]] =
        InteractiveSubmission
          .verifySignatures(
            hash,
            externalAuthorization.signatures,
            topology,
          )
          .value
          .map(_ => None)

      submitterMetadata.externalAuthorization match {
        case Some(_) if reInterpretedTopLevelViews.size > 1 =>
          FutureUnlessShutdown.pure(
            Some(
              viewTree.viewPosition -> err(
                s"Only single root transactions can currently be externally signed"
              )
            )
          )
        case Some(externalAuthorization) =>
          computeHashAndVerifyExternalSignature(externalAuthorization)
        // External signatures are not necessarily required. If they're needed to authorize the transaction,
        // it will be checked in the AuthorizationValidator. Here we simply verify that the provided
        // signatures are valid
        case None => FutureUnlessShutdown.pure(None)
      }
    }

    for {
      signatureCheckErrors <- rootViews.parTraverseFilter(verifySignatures)
    } yield signatureCheckErrors.toMap
  }
}
