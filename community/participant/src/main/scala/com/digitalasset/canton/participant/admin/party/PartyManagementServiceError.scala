// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import com.digitalasset.base.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.PartyManagementServiceErrorGroup
import com.digitalasset.canton.error.{CantonBaseError, CantonError}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SynchronizerId}

sealed trait PartyManagementServiceError extends Product with Serializable with CantonBaseError

object PartyManagementServiceError extends PartyManagementServiceErrorGroup {

  object InvalidArgument
      extends ErrorCode(
        id = "INVALID_ARGUMENT_PARTY_MANAGEMENT_ERROR",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(reason)
        with PartyManagementServiceError
  }

  object InvalidState
      extends ErrorCode(
        id = "INVALID_STATE_PARTY_MANAGEMENT_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(reason)
        with PartyManagementServiceError
  }

  @Explanation(
    "The party ACS cannot be exported because the target participant's party-to-participant mapping lacks the required onboarding flag."
  )
  @Resolution(
    "Authorize the party-to-participant mapping with the onboarding flag set, wait for it to become effective, and retry the export."
  )
  object AcsExportMissingTargetOnboardingMapping
      extends ErrorCode(
        id = "PARTY_ACS_EXPORT_MISSING_TARGET_ONBOARDING_MAPPING",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(
        party: PartyId,
        targetParticipant: ParticipantId,
    )(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            s"ACS export aborted for party $party: missing onboarding flag for target participant $targetParticipant."
        )
        with PartyManagementServiceError
  }

  @Explanation(
    "Cannot import the party ACS because this participant either lacks a party-to-participant mapping entirely, or the existing mapping is missing the required onboarding flag."
  )
  @Resolution(
    "Authorize a party-to-participant mapping for this participant with the onboarding flag set and retry the import."
  )
  object AcsImportMissingOnboardingMapping
      extends ErrorCode(
        id = "PARTY_ACS_IMPORT_MISSING_ONBOARDING_MAPPING",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(
        partyId: PartyId
    )(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            s"Aborted ACS import for party $partyId: missing party-to-participant mapping or required onboarding flag for this participant."
        )
        with PartyManagementServiceError
  }

  @Explanation(
    "The requested timestamp does not match the exact record time of any known event (e.g., a topology transaction)."
  )
  @Resolution(
    "If you require an exact match, ensure the timestamp provided matches a known event exactly. If you intentionally want the highest prior offset regardless of an exact match, set the 'force' flag to true."
  )
  object ExactRecordTimeMatchNotFound
      extends ErrorCode(
        id = "EXACT_RECORD_TIME_MATCH_NOT_FOUND",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Error(
        foundOffset: Offset,
        requestedTimestamp: CantonTimestamp,
        foundRecordTime: CantonTimestamp,
    )(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            s"No exact match for requested timestamp $requestedTimestamp. The closest prior event was at $foundRecordTime (offset=${foundOffset.unwrap})."
        )
        with PartyManagementServiceError
  }

  @Explanation(
    "Cannot clear the onboarding flag because the party-to-participant mapping for this participant lacks the required onboarding flag."
  )
  @Resolution(
    "Authorize the party-to-participant mapping with the onboarding flag set, wait for it to become effective, and retry clearing the flag."
  )
  object OnboardingClearanceMissingMapping
      extends ErrorCode(
        id = "PARTY_ONBOARDING_CLEARANCE_MISSING_MAPPING",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(
        party: PartyId,
        targetParticipant: ParticipantId,
    )(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            s"Aborted onboarding flag clearance for party $party: missing party-to-participant mapping onboarding flag for the participant $targetParticipant."
        )
        with PartyManagementServiceError
  }

  object IOStream
      extends ErrorCode(
        id = "IO_STREAM_PARTY_MANAGEMENT_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(reason)
        with PartyManagementServiceError
  }

  @Explanation(
    "Could not find the effective party-to-participant mapping topology transaction for the target participant after the specified begin offset within the given timeout."
  )
  @Resolution(
    "Verify that the party-to-participant mapping was authorized and that the selected ledger 'begin offset exclusive' strictly predates this authorization, and retry."
  )
  object EffectivePartyToParticipantMappingNotFound
      extends ErrorCode(
        id = "EFFECTIVE_PARTY_TO_PARTICIPANT_MAPPING_NOT_FOUND",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(
        party: PartyId,
        targetParticipant: ParticipantId,
        reason: String,
    )(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            s"Failed to find the effective party-to-participant mapping topology transaction for party $party on participant $targetParticipant: $reason"
        )
        with PartyManagementServiceError
  }

  @Explanation(
    """The participant does not (yet) support serving a ledger offset at the requested timestamp. This may have happened
      |because the ledger state processing has not yet caught up."""
  )
  @Resolution(
    """Ensure the requested timestamp is valid. If so, retry after some time (possibly repeatedly)."""
  )
  object InvalidTimestamp
      extends ErrorCode(
        id = "INVALID_TIMESTAMP_PARTY_MANAGEMENT_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateSeekAfterEnd,
      ) {
    final case class Error(
        synchronizerId: SynchronizerId,
        requestedTimestamp: CantonTimestamp,
        force: Boolean,
        reason: String,
    )(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            s"No ledger offset found for the requested timestamp $requestedTimestamp on synchronizer $synchronizerId (using force flag = $force): $reason"
        )
        with PartyManagementServiceError
  }

  @Explanation(
    "The requested timestamp is in the future relative to the fully processed synchronizer index."
  )
  @Resolution(
    "Wait for the indexer to process events up to the requested timestamp, or set the 'force' flag to true to retrieve the highest available offset."
  )
  object UnprocessedRequestedTimestamp
      extends ErrorCode(
        id = "UNPROCESSED_REQUESTED_TIMESTAMP",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(
        synchronizerId: SynchronizerId,
        requestedTimestamp: CantonTimestamp,
        cleanSynchronizerTimestamp: CantonTimestamp,
    )(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            s"Requested timestamp $requestedTimestamp exceeds the clean synchronizer timestamp $cleanSynchronizerTimestamp."
        )
        with PartyManagementServiceError
  }

  @Explanation(
    "An internal system invariant was violated regarding ledger offsets and timestamps."
  )
  @Resolution(
    "Contact support. This indicates a data consistency issue or an internal coding bug within the database snapshot."
  )
  object InternalInvariantViolation
      extends ErrorCode(
        id = "INTERNAL_INVARIANT_VIOLATION",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class Error(
        reason: String
    )(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = s"Internal invariant violation: $reason"
        )
        with PartyManagementServiceError
  }

}
