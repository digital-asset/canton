// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.syntax.parallel.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.{FullTransactionViewTree, SubmitterMetadata, ViewPosition}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps.ParsedTransactionRequest
import com.digitalasset.canton.protocol.{ExternalAuthorization, RequestId}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*

import scala.concurrent.ExecutionContext

class AuthorizationValidator(participantId: ParticipantId, enableExternalAuthorization: Boolean)(
    implicit executionContext: ExecutionContext
) {

  def checkAuthorization(
      parsedRequest: ParsedTransactionRequest
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[ViewPosition, String]] =
    checkAuthorization(
      parsedRequest.requestId,
      parsedRequest.rootViewTrees.forgetNE,
      parsedRequest.snapshot.ipsSnapshot,
    )

  def checkAuthorization(
      requestId: RequestId,
      rootViews: Seq[FullTransactionViewTree],
      snapshot: TopologySnapshot,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[ViewPosition, String]] =
    rootViews
      .parTraverseFilter { rootView =>
        val authorizers =
          rootView.viewParticipantData.rootAction.authorizers

        def err(details: String): String =
          show"Received a request with id $requestId with a view that is not correctly authorized. Rejecting request...\n$details"

        def noAuthorizationForParties(
            notCoveredBySubmittingParty: Set[LfPartyId]
        ): FutureUnlessShutdown[Option[String]] =
          FutureUnlessShutdown.pure(
            Some(
              err(
                show"Missing authorization for $notCoveredBySubmittingParty through the submitting parties."
              )
            )
          )

        def notAuthorizedBySubmittingParticipant(
            submittingParticipant: ParticipantId,
            notAllowedBySubmittingParticipant: Seq[LfPartyId],
        ): Option[String] =
          Some(
            err(
              show"The submitting participant $submittingParticipant is not authorized to submit on behalf of the submitting parties ${notAllowedBySubmittingParticipant.toSeq}."
            )
          )

        def missingExternalAuthorizers(
            parties: Set[LfPartyId]
        ): FutureUnlessShutdown[Option[String]] =
          FutureUnlessShutdown.pure(
            Some(
              err(
                show"An externally signed transaction is missing the following acting parties: $parties."
              )
            )
          )

        def missingValidFingerprints(
            parties: Set[PartyId]
        ): String =
          err(
            show"The following parties have provided fingerprints that are not valid: $parties"
          )

        def notHostedForConfirmation(
            parties: Set[LfPartyId]
        ): String =
          err(
            show"The following parties have are not hosted anywhere on the domain with confirmation rights: $parties"
          )

        def checkMetadata(
            submitterMetadata: SubmitterMetadata
        ): FutureUnlessShutdown[Option[String]] =
          submitterMetadata.externalAuthorization match {
            case None => checkNonExternallySignedMetadata(submitterMetadata)
            case Some(tx) if enableExternalAuthorization =>
              checkExternallySignedMetadata(submitterMetadata, tx)
            case Some(_) =>
              FutureUnlessShutdown.pure(
                Some(
                  err(
                    "External authentication is not enabled (to enable set enable-external-authorization parameter)"
                  )
                )
              )
          }

        def checkNonExternallySignedMetadata(
            submitterMetadata: SubmitterMetadata
        ): FutureUnlessShutdown[Option[String]] = {
          val notCoveredBySubmittingParty = authorizers -- submitterMetadata.actAs
          if (notCoveredBySubmittingParty.nonEmpty) {
            noAuthorizationForParties(notCoveredBySubmittingParty)
          } else {
            for {
              notAllowedBySubmittingParticipant <- FutureUnlessShutdown.outcomeF(
                snapshot.canNotSubmit(
                  submitterMetadata.submittingParticipant,
                  submitterMetadata.actAs.toSeq,
                )
              )
            } yield
              if (notAllowedBySubmittingParticipant.nonEmpty) {
                notAuthorizedBySubmittingParticipant(
                  submitterMetadata.submittingParticipant,
                  notAllowedBySubmittingParticipant.toSeq,
                )
              } else None
          }
        }

        def checkExternallySignedMetadata(
            submitterMetadata: SubmitterMetadata,
            externalAuthorization: ExternalAuthorization,
        ): FutureUnlessShutdown[Option[String]] = {
          def validateFingerprints =
            // The parties are external
            externalAuthorization.signatures.toSeq
              .parTraverseFilter { case (signingParty, partySignatures) =>
                // Retrieve each party's registered fingerprint from topology
                snapshot.partyAuthorization(signingParty).map {
                  case Some(info) =>
                    val signatureFingerprints = partySignatures.map(_.signedBy).toSet
                    val invalidFingerprints =
                      signatureFingerprints -- info.signingKeys.map(_.fingerprint).toSet
                    Option
                      // If some fingerprints are not registered, they're invalid
                      .when(invalidFingerprints.nonEmpty)(signingParty)
                      .orElse(
                        // If we have less fingerprints than the required threshold, it's also a reject
                        Option
                          .when(signatureFingerprints.size < info.threshold.unwrap)(signingParty)
                      )
                  // If the party hasn't registered signing keys, it's not an external party so we reject
                  case None =>
                    Some(signingParty)
                }
              }
              .map(_.toSet)
              .map { parties =>
                Option.when(parties.nonEmpty)(missingValidFingerprints(parties))
              }

          def validateExternalPartiesIsHostedForConfirmation =
            FutureUnlessShutdown
              .outcomeF(snapshot.hasNoConfirmer(submitterMetadata.actAs.forgetNE))
              .map { notHosted =>
                Option.when(notHosted.nonEmpty)(notHostedForConfirmation(notHosted))
              }

          // The signed TX parties covers all act-as parties
          val signedAs = externalAuthorization.signatures.keySet
          val missingAuthorizers = (submitterMetadata.actAs ++ authorizers) -- signedAs.map(_.toLf)
          if (missingAuthorizers.nonEmpty) {
            missingExternalAuthorizers(missingAuthorizers)
          } else {
            for {
              invalidFingerprints <- validateFingerprints
              notHosted <- validateExternalPartiesIsHostedForConfirmation
            } yield invalidFingerprints.orElse(notHosted)
          }
        }

        def checkAuthorizersAreNotHosted(): FutureUnlessShutdown[Option[String]] =
          FutureUnlessShutdown.outcomeF(snapshot.hostedOn(authorizers, participantId)).map {
            hostedAuthorizers =>
              // If this participant hosts an authorizer, it should also have received the parent view.
              // As rootView is not a top-level (submitter metadata is blinded), there is a gap in the authorization chain.

              Option.when(hostedAuthorizers.nonEmpty)(
                err(
                  show"Missing authorization for ${hostedAuthorizers.keys.toSeq.sorted}, ${rootView.viewPosition}."
                )
              )
          }

        (rootView.submitterMetadataO match {
          case Some(submitterMetadata) =>
            // The submitter metadata is unblinded -> rootView is a top-level view
            checkMetadata(submitterMetadata)

          case None =>
            // The submitter metadata is blinded -> rootView is not a top-level view
            checkAuthorizersAreNotHosted()
        })
          .map(_.map(rootView.viewPosition -> _))

      }
      .map(_.toMap)

}
