// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.admin.party_management_service.{
  PartyDetails,
  UpdatePartyDetailsRequest,
}
import com.daml.ledger.api.v2.command_completion_service.{
  CompletionStreamRequest,
  GetCompletionsRequest,
}
import com.daml.ledger.api.v2.command_service.SubmitAndWaitForTransactionRequest
import com.daml.ledger.api.v2.commands.Commands
import com.daml.ledger.api.v2.interactive.interactive_submission_service.HashingSchemeVersion.HASHING_SCHEME_VERSION_V2
import com.daml.ledger.api.v2.interactive.interactive_submission_service.Metadata.SubmitterInfo
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  ExecuteSubmissionAndWaitForTransactionRequest,
  ExecuteSubmissionRequest,
  Metadata,
  PrepareSubmissionRequest,
  PreparedTransaction,
}
import com.daml.ledger.api.v2.state_service.{
  GetActiveContractsRequest,
  GetConnectedSynchronizersRequest,
  GetHostedPartiesPageRequest,
  GetHostedPartiesRequest,
  HostedPartiesFilter,
}
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{
  ContractIdFilter,
  EventFormat,
  Filters,
  ParticipantAuthorizationTopologyFormat,
  TopologyFormat,
  TransactionFormat,
  UpdateFormat,
}
import com.daml.ledger.api.v2.update_service.GetUpdatesRequest
import com.digitalasset.canton.auth.RequiredClaim
import com.digitalasset.canton.ledger.api.auth.services.ApiServicesRequiredClaimSpec.{
  executeSubmissionAndWaitForTransactionRequest,
  executeSubmissionRequest,
  prepareSubmissionRequest,
  submitAndWaitForTransactionRequest,
}
import com.digitalasset.canton.ledger.api.auth.{RequiredClaims, TrafficAccountAuthorization}
import com.digitalasset.canton.platform.apiserver.services.RequestWithReadAsParty
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.{BaseTest, LfPartyId}
import com.google.protobuf.ByteString
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import scalapb.lenses.Lens

import java.util.UUID

class ApiServicesRequiredClaimSpec extends AsyncFlatSpec with BaseTest with Matchers {

  behavior of "CommandCompletionServiceAuthorization.completionStreamClaims"

  it should "compute the correct claims in the happy path" in {
    val result = CommandCompletionServiceAuthorization.completionStreamClaims(
      CompletionStreamRequest(
        userId = "qwe",
        parties = Seq("a", "b", "c"),
        beginExclusive = 1234L,
      )
    )
    result should have size (4)
    result.collect(readAs) should contain theSameElementsAs List(
      RequiredClaim.ReadAs[CompletionStreamRequest]("a"),
      RequiredClaim.ReadAs[CompletionStreamRequest]("b"),
      RequiredClaim.ReadAs[CompletionStreamRequest]("c"),
    )
    result
      .collectFirst(matchUserId)
      .value
      .skipUserIdValidationForAnyPartyReaders shouldBe true
  }

  it should "compute the correct claims if empty parties" in {
    val result = CommandCompletionServiceAuthorization.completionStreamClaims(
      CompletionStreamRequest(
        userId = "qwe",
        parties = Nil,
        beginExclusive = 1234L,
      )
    )
    result should have size (1)
    result.collect(readAs) shouldBe Nil
    result
      .collectFirst(matchUserId)
      .value
      .skipUserIdValidationForAnyPartyReaders shouldBe true
  }

  behavior of "CommandCompletionServiceAuthorization.getCompletionsClaims"

  it should "map specific parties to ReadAs claims with no user claim" in {
    val result = CommandCompletionServiceAuthorization.getCompletionsClaims(
      GetCompletionsRequest(
        parties = Seq("a", "b", "c"),
        beginExclusive = 1234L,
      )
    )
    result should contain theSameElementsAs List(
      RequiredClaim.ReadAs[GetCompletionsRequest]("a"),
      RequiredClaim.ReadAs[GetCompletionsRequest]("b"),
      RequiredClaim.ReadAs[GetCompletionsRequest]("c"),
    )
  }

  it should "map empty parties to a single ReadAsAnyParty claim" in {
    val result = CommandCompletionServiceAuthorization.getCompletionsClaims(
      GetCompletionsRequest(
        parties = Nil,
        beginExclusive = 1234L,
      )
    )
    result shouldBe List(RequiredClaim.ReadAsAnyParty[GetCompletionsRequest]())
  }

  behavior of "PartyManagementServiceAuthorization.updatePartyDetailsClaims"

  it should "compute the correct claims in the happy path" in {
    val result = PartyManagementServiceAuthorization.updatePartyDetailsClaims(
      UpdatePartyDetailsRequest(
        partyDetails = Some(
          PartyDetails(
            party = "abc",
            isLocal = true,
            localMetadata = None,
            identityProviderId = "ABC",
          )
        ),
        updateMask = None,
      )
    )
    result should have size (2)
    result.collectFirst(adminOrIdp).isDefined shouldBe true
    result.collectFirst(matchIdentityProviderId).isDefined shouldBe true
  }

  it should "compute the correct claims if no party details provided" in {
    val result = PartyManagementServiceAuthorization.updatePartyDetailsClaims(
      UpdatePartyDetailsRequest(
        partyDetails = None,
        updateMask = None,
      )
    )
    result should have size (1)
    result.collectFirst(adminOrIdp).isDefined shouldBe true
  }

  behavior of "StateServiceAuthorization.getActiveContractsClaims"

  it should "compute the correct claims in the happy path" in {
    StateServiceAuthorization.getActiveContractsClaims(
      GetActiveContractsRequest(
        activeAtOffset = 15,
        eventFormat = Some(
          EventFormat(
            filtersByParty = Map(
              "a" -> Filters(Nil),
              "b" -> Filters(Nil),
              "c" -> Filters(Nil),
            ),
            filtersForAnyParty = Some(Filters(Nil)),
            verbose = true,
            filtersByContractId = Seq.empty,
          )
        ),
        streamContinuationToken = None,
        includeIncompleteReassignments = false,
      )
    ) should contain theSameElementsAs RequiredClaims[GetActiveContractsRequest](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
      RequiredClaim.ReadAsAnyParty(),
    )
  }

  it should "compute the correct claims if no filtersForAnyParty" in {
    StateServiceAuthorization.getActiveContractsClaims(
      GetActiveContractsRequest(
        activeAtOffset = 15,
        eventFormat = Some(
          EventFormat(
            filtersByParty = Map(
              "a" -> Filters(Nil),
              "b" -> Filters(Nil),
              "c" -> Filters(Nil),
            ),
            filtersForAnyParty = None,
            verbose = true,
            filtersByContractId = Seq.empty,
          )
        ),
        streamContinuationToken = None,
        includeIncompleteReassignments = false,
      )
    ) should contain theSameElementsAs RequiredClaims[GetActiveContractsRequest](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
    )
  }

  it should "compute the correct claims if no filtersByParty" in {
    StateServiceAuthorization.getActiveContractsClaims(
      GetActiveContractsRequest(
        activeAtOffset = 15,
        eventFormat = Some(
          EventFormat(
            filtersByParty = Map.empty,
            filtersForAnyParty = None,
            verbose = true,
            filtersByContractId = Seq.empty,
          )
        ),
        streamContinuationToken = None,
        includeIncompleteReassignments = false,
      )
    ) shouldBe Nil
  }

  it should "compute the correct claims if no eventFormat" in {
    StateServiceAuthorization.getActiveContractsClaims(
      GetActiveContractsRequest(
        activeAtOffset = 15,
        eventFormat = None,
        streamContinuationToken = None,
        includeIncompleteReassignments = false,
      )
    ) shouldBe Nil
  }

  behavior of "StateServiceAuthorization.getHostedPartiesClaims"

  it should "require public access and fill the filter with readable parties when it is absent" in {
    val request = GetHostedPartiesRequest(
      hostedAtOffset = Some(42L),
      filter = None,
      streamContinuationToken = Some(ByteString.copyFromUtf8("continuation")),
    )
    val partiesL = StateServiceAuthorization.partiesForGetHostedPartiesL
    StateServiceAuthorization.getHostedPartiesClaims(request) shouldBe List(
      RequiredClaim.Public[GetHostedPartiesRequest](),
      RequiredClaim.MatchReadAsParties(partiesL),
    )
    partiesL.set(Set("b", "a"))(request) shouldBe GetHostedPartiesRequest(
      hostedAtOffset = Some(42L),
      filter = Some(
        HostedPartiesFilter(
          partyFilter = Seq("a", "b"),
          allParticipants = false,
          synchronizerIdFilter = Seq.empty,
        )
      ),
      streamContinuationToken = Some(ByteString.copyFromUtf8("continuation")),
    )
    partiesL.set(Set.empty)(request) shouldBe GetHostedPartiesRequest(
      hostedAtOffset = Some(42L),
      filter = Some(
        HostedPartiesFilter(
          partyFilter = Seq.empty,
          allParticipants = false,
          synchronizerIdFilter = Seq.empty,
        )
      ),
      streamContinuationToken = Some(ByteString.copyFromUtf8("continuation")),
    )
  }

  it should "require only public access for a supplied empty filter" in {
    val request = GetHostedPartiesRequest(
      hostedAtOffset = None,
      filter = Some(
        HostedPartiesFilter(
          partyFilter = Seq.empty,
          allParticipants = false,
          synchronizerIdFilter = Seq.empty,
        )
      ),
      streamContinuationToken = None,
    )
    StateServiceAuthorization.getHostedPartiesClaims(request) shouldBe
      List(RequiredClaim.Public[GetHostedPartiesRequest]())
  }

  it should "require only public access for a supplied full filter" in {
    val request = GetHostedPartiesRequest(
      hostedAtOffset = None,
      filter = Some(
        HostedPartiesFilter(
          partyFilter = Seq("a", "b"),
          allParticipants = true,
          synchronizerIdFilter = Seq("synchronizer::namespace"),
        )
      ),
      streamContinuationToken = None,
    )
    StateServiceAuthorization.getHostedPartiesClaims(request) shouldBe
      List(RequiredClaim.Public[GetHostedPartiesRequest]())
  }

  behavior of "StateServiceAuthorization.getHostedPartiesPageClaims"

  it should "require public access and fill the filter with readable parties when it is absent" in {
    val request = GetHostedPartiesPageRequest(
      hostedAtOffset = Some(42L),
      filter = None,
      pageToken = Some(ByteString.copyFromUtf8("page")),
      maxPageSize = Some(1),
    )
    val partiesL = StateServiceAuthorization.partiesForGetHostedPartiesPageL
    StateServiceAuthorization.getHostedPartiesPageClaims(request) shouldBe List(
      RequiredClaim.Public[GetHostedPartiesPageRequest](),
      RequiredClaim.MatchReadAsParties(partiesL),
    )
    partiesL.set(Set("b", "a"))(request) shouldBe GetHostedPartiesPageRequest(
      hostedAtOffset = Some(42L),
      filter = Some(
        HostedPartiesFilter(
          partyFilter = Seq("a", "b"),
          allParticipants = false,
          synchronizerIdFilter = Seq.empty,
        )
      ),
      pageToken = Some(ByteString.copyFromUtf8("page")),
      maxPageSize = Some(1),
    )
    partiesL.set(Set.empty)(request) shouldBe GetHostedPartiesPageRequest(
      hostedAtOffset = Some(42L),
      filter = Some(
        HostedPartiesFilter(
          partyFilter = Seq.empty,
          allParticipants = false,
          synchronizerIdFilter = Seq.empty,
        )
      ),
      pageToken = Some(ByteString.copyFromUtf8("page")),
      maxPageSize = Some(1),
    )
  }

  it should "require only public access for a supplied empty filter" in {
    val request = GetHostedPartiesPageRequest(
      hostedAtOffset = None,
      filter = Some(
        HostedPartiesFilter(
          partyFilter = Seq.empty,
          allParticipants = false,
          synchronizerIdFilter = Seq.empty,
        )
      ),
      pageToken = None,
      maxPageSize = None,
    )
    StateServiceAuthorization.getHostedPartiesPageClaims(request) shouldBe
      List(RequiredClaim.Public[GetHostedPartiesPageRequest]())
  }

  it should "require only public access for a supplied full filter" in {
    val request = GetHostedPartiesPageRequest(
      hostedAtOffset = None,
      filter = Some(
        HostedPartiesFilter(
          partyFilter = Seq("a", "b"),
          allParticipants = true,
          synchronizerIdFilter = Seq("synchronizer::namespace"),
        )
      ),
      pageToken = None,
      maxPageSize = None,
    )
    StateServiceAuthorization.getHostedPartiesPageClaims(request) shouldBe
      List(RequiredClaim.Public[GetHostedPartiesPageRequest]())
  }

  behavior of "StateServiceAuthorization.getConnectedSynchronizersClaims"

  it should "require only a public claim if no parameters are given" in {
    StateServiceAuthorization.getConnectedSynchronizersClaims(
      GetConnectedSynchronizersRequest(party = "", participantId = "", identityProviderId = "")
    ) shouldBe List(RequiredClaim.Public[GetConnectedSynchronizersRequest]())
  }

  it should "require only a public claim if no party is given, regardless of the other parameters" in {
    StateServiceAuthorization.getConnectedSynchronizersClaims(
      GetConnectedSynchronizersRequest(
        party = "",
        participantId = "some-participant",
        identityProviderId = "some-idp",
      )
    ) shouldBe List(RequiredClaim.Public[GetConnectedSynchronizersRequest]())
  }

  it should "require admin, IDP admin or operate as party rights with a matching IDP if a party is given" in {
    StateServiceAuthorization.getConnectedSynchronizersClaims(
      GetConnectedSynchronizersRequest(
        party = "alice",
        participantId = "",
        identityProviderId = "some-idp",
      )
    ) shouldBe List(
      RequiredClaim.AdminOrIdpAdminOrOperateAsParty[GetConnectedSynchronizersRequest](Seq("alice")),
      RequiredClaim.MatchIdentityProviderId(
        StateServiceAuthorization.identityProviderIdForGetConnectedSynchronizersL
      ),
    )
  }

  behavior of "InteractiveSubmissionServiceAuthorization.prepareSubmission"

  it should "compute the correct claims in the happy path" in {
    InteractiveSubmissionServiceAuthorization.getPreparedSubmissionClaims(
      prepareSubmissionRequest
    ) should contain theSameElementsAs RequiredClaims[PrepareSubmissionRequest](
      RequiredClaim.ReadAs("1"),
      RequiredClaim.ReadAs("2"),
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.MatchUserId(
        InteractiveSubmissionServiceAuthorization.userIdForPrepareSubmissionL
      ),
    )
  }

  behavior of "InteractiveSubmissionServiceAuthorization.executeSubmission"

  it should "compute the correct claims in the happy path" in {
    InteractiveSubmissionServiceAuthorization.getExecuteSubmissionClaims(
      executeSubmissionRequest,
      InteractiveSubmissionServiceAuthorization.preparedTransactionForExecuteSubmissionL,
      InteractiveSubmissionServiceAuthorization.userIdForExecuteSubmissionL,
    ) should contain theSameElementsAs RequiredClaims[ExecuteSubmissionRequest](
      RequiredClaim.ExecuteAs("1"),
      RequiredClaim.ExecuteAs("2"),
      RequiredClaim.MatchUserId(
        InteractiveSubmissionServiceAuthorization.userIdForExecuteSubmissionL
      ),
    )
  }

  behavior of "InteractiveSubmissionServiceAuthorization.executeSubmissionAndWaitForTransaction"

  it should "compute the correct claims in the happy path" in {
    InteractiveSubmissionServiceAuthorization.getExecuteSubmissionAndWaitForTransactionClaims(
      executeSubmissionAndWaitForTransactionRequest
    ) should contain theSameElementsAs RequiredClaims[
      ExecuteSubmissionAndWaitForTransactionRequest
    ](
      RequiredClaim.ExecuteAs("1"),
      RequiredClaim.ExecuteAs("2"),
      RequiredClaim.ReadAs("i"),
      RequiredClaim.ReadAs("ii"),
      RequiredClaim.ReadAsAnyParty(),
      RequiredClaim.MatchUserId(
        InteractiveSubmissionServiceAuthorization.userIdForExecuteSubmissionAndWaitForTransactionL
      ),
    )
  }

  behavior of "CommandServiceAuthorization.getSubmitAndWaitForTransactionClaims"

  it should "compute the correct claims in the happy path" in {
    CommandServiceAuthorization.getSubmitAndWaitForTransactionClaims(
      submitAndWaitForTransactionRequest
    ) should contain theSameElementsAs RequiredClaims[SubmitAndWaitForTransactionRequest](
      RequiredClaim.ActAs("1"),
      RequiredClaim.ActAs("2"),
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("i"),
      RequiredClaim.ReadAs("ii"),
      RequiredClaim.ReadAsAnyParty(),
      RequiredClaim.MatchUserId(CommandServiceAuthorization.userIdForTransactionL),
    )
  }

  it should "compute the correct claims if no filtersForAnyParty" in {
    CommandServiceAuthorization.getSubmitAndWaitForTransactionClaims(
      submitAndWaitForTransactionRequest.update(
        _.transactionFormat.eventFormat.modify(_.clearFiltersForAnyParty)
      )
    ) should contain theSameElementsAs RequiredClaims[SubmitAndWaitForTransactionRequest](
      RequiredClaim.ActAs("1"),
      RequiredClaim.ActAs("2"),
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("i"),
      RequiredClaim.ReadAs("ii"),
      RequiredClaim.MatchUserId(CommandServiceAuthorization.userIdForTransactionL),
    )
  }

  it should "compute the correct claims if no filtersByParty" in {
    CommandServiceAuthorization.getSubmitAndWaitForTransactionClaims(
      submitAndWaitForTransactionRequest.update(
        _.transactionFormat.eventFormat.modify(_.clearFiltersByParty)
      )
    ) should contain theSameElementsAs RequiredClaims[SubmitAndWaitForTransactionRequest](
      RequiredClaim.ActAs("1"),
      RequiredClaim.ActAs("2"),
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAsAnyParty(),
      RequiredClaim.MatchUserId(CommandServiceAuthorization.userIdForTransactionL),
    )
  }

  it should "compute the correct claims if no transactionFormat" in {
    CommandServiceAuthorization.getSubmitAndWaitForTransactionClaims(
      submitAndWaitForTransactionRequest.clearTransactionFormat
    ) should contain theSameElementsAs RequiredClaims[SubmitAndWaitForTransactionRequest](
      RequiredClaim.ActAs("1"),
      RequiredClaim.ActAs("2"),
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.MatchUserId(CommandServiceAuthorization.userIdForTransactionL),
    )
  }

  it should "compute the correct claims if no actAs" in {
    CommandServiceAuthorization.getSubmitAndWaitForTransactionClaims(
      submitAndWaitForTransactionRequest.update(_.commands.modify(_.clearActAs))
    ) should contain theSameElementsAs RequiredClaims[SubmitAndWaitForTransactionRequest](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("i"),
      RequiredClaim.ReadAs("ii"),
      RequiredClaim.ReadAsAnyParty(),
      RequiredClaim.MatchUserId(CommandServiceAuthorization.userIdForTransactionL),
    )
  }

  it should "compute the correct claims if no readAs" in {
    CommandServiceAuthorization.getSubmitAndWaitForTransactionClaims(
      submitAndWaitForTransactionRequest.update(_.commands.modify(_.clearReadAs))
    ) should contain theSameElementsAs RequiredClaims[SubmitAndWaitForTransactionRequest](
      RequiredClaim.ActAs("1"),
      RequiredClaim.ActAs("2"),
      RequiredClaim.ReadAs("i"),
      RequiredClaim.ReadAs("ii"),
      RequiredClaim.ReadAsAnyParty(),
      RequiredClaim.MatchUserId(CommandServiceAuthorization.userIdForTransactionL),
    )
  }

  it should "compute the correct claims if no actAs and no readAs" in {
    CommandServiceAuthorization.getSubmitAndWaitForTransactionClaims(
      submitAndWaitForTransactionRequest.update(
        _.commands.modify(_.clearReadAs),
        _.commands.modify(_.clearActAs),
      )
    ) should contain theSameElementsAs RequiredClaims[SubmitAndWaitForTransactionRequest](
      RequiredClaim.ReadAs("i"),
      RequiredClaim.ReadAs("ii"),
      RequiredClaim.ReadAsAnyParty(),
      RequiredClaim.MatchUserId(CommandServiceAuthorization.userIdForTransactionL),
    )
  }

  behavior of "TrafficAccountAuthorization.requiredClaims"

  it should "require debit rights for explicit accounts, and for the fallback party in strict mode" in {
    val adminParty = LfPartyId.assertFromString("participant1::default")
    val cases = Table(
      ("implicit", "accountId", "actAs", "expected"),
      (true, "acme/billing", Set("alice", "bob"), Some("acme/billing" -> true)),
      (true, "", Set("alice"), None),
      (false, "acme/billing", Set("alice"), Some("acme/billing" -> false)),
      (false, "", Set("alice"), Some("alice" -> false)),
      (false, "", Set(adminParty: String), None),
      (false, "", Set("alice", "bob"), None),
    )
    forAll(cases) { (implicitDebit, accountId, actAs, expected) =>
      new TrafficAccountAuthorization(adminParty, implicitDebit)
        .requiredClaims[Unit](accountId, actAs) shouldBe expected.toList.map {
        case (account, acceptExecuteRights) =>
          RequiredClaim.DebitTrafficFrom[Unit](account, acceptExecuteRights)
      }
    }
  }

  behavior of "UpdateServiceAuthorization.getUpdateFormatClaimsAndFillReadAsParties"

  it should "compute the correct claims in the happy path" in {
    getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(
              TransactionFormat(
                eventFormat = Some(
                  EventFormat(
                    filtersByParty = Map(
                      "a" -> Filters(Nil),
                      "b" -> Filters(Nil),
                    ),
                    filtersForAnyParty = Some(Filters(Nil)),
                    verbose = true,
                    filtersByContractId = Seq.empty,
                  )
                ),
                transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
              )
            ),
            includeReassignments = Some(
              EventFormat(
                filtersByParty = Map(
                  "c" -> Filters(Nil),
                  "d" -> Filters(Nil),
                ),
                filtersForAnyParty = Some(Filters(Nil)),
                verbose = true,
                filtersByContractId = Seq.empty,
              )
            ),
            includeTopologyEvents = Some(
              TopologyFormat(Some(ParticipantAuthorizationTopologyFormat(parties = Seq("e", "f"))))
            ),
            includeTransactionsAndReassignments = None,
          )
        ),
        descendingOrder = false,
      )
    ) should contain theSameElementsAs RequiredClaims[RequestWithReadAsParty[GetUpdatesRequest]](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
      RequiredClaim.ReadAs("d"),
      RequiredClaim.ReadAsAnyParty(),
    )
  }

  it should "compute the correct claims if no party wildcards exist" in {
    getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(
              TransactionFormat(
                eventFormat = Some(
                  EventFormat(
                    filtersByParty = Map(
                      "a" -> Filters(Nil),
                      "b" -> Filters(Nil),
                    ),
                    filtersForAnyParty = None,
                    verbose = true,
                    filtersByContractId = Seq.empty,
                  )
                ),
                transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
              )
            ),
            includeReassignments = Some(
              EventFormat(
                filtersByParty = Map(
                  "c" -> Filters(Nil),
                  "d" -> Filters(Nil),
                ),
                filtersForAnyParty = None,
                verbose = true,
                filtersByContractId = Seq.empty,
              )
            ),
            includeTopologyEvents = Some(
              TopologyFormat(Some(ParticipantAuthorizationTopologyFormat(parties = Seq("e", "f"))))
            ),
            includeTransactionsAndReassignments = None,
          )
        ),
        descendingOrder = false,
      )
    ) should contain theSameElementsAs RequiredClaims[RequestWithReadAsParty[GetUpdatesRequest]](
      RequiredClaim.ReadAs("a"),
      RequiredClaim.ReadAs("b"),
      RequiredClaim.ReadAs("c"),
      RequiredClaim.ReadAs("d"),
    )
  }

  it should "compute the correct claims if no filtersByParty in transactions and reassignments exists" in {
    val eventFormatO = Some(
      EventFormat(
        filtersByParty = Map.empty,
        filtersForAnyParty = None,
        verbose = true,
        filtersByContractId = Seq.empty,
      )
    )
    getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(
              TransactionFormat(
                eventFormat = eventFormatO,
                transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
              )
            ),
            includeReassignments = eventFormatO,
            includeTopologyEvents = Some(
              TopologyFormat(Some(ParticipantAuthorizationTopologyFormat(parties = Seq("e", "f"))))
            ),
            includeTransactionsAndReassignments = None,
          )
        ),
        descendingOrder = false,
      )
    ) should contain theSameElementsAs RequiredClaims[RequestWithReadAsParty[GetUpdatesRequest]](
      RequiredClaim.Public()
    )
  }

  it should "require only a public claim if no filtersByParty in transactions and reassignments exists and topology format is empty" in {
    val eventFormatO = Some(
      EventFormat(
        filtersByParty = Map.empty,
        filtersForAnyParty = None,
        verbose = true,
        filtersByContractId = Seq.empty,
      )
    )
    getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(
              TransactionFormat(
                eventFormat = eventFormatO,
                transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
              )
            ),
            includeReassignments = eventFormatO,
            includeTopologyEvents = Some(
              TopologyFormat(None)
            ),
            includeTransactionsAndReassignments = None,
          )
        ),
        descendingOrder = false,
      )
    ) should contain theSameElementsAs RequiredClaims[RequestWithReadAsParty[GetUpdatesRequest]](
      RequiredClaim.Public()
    )
  }

  it should "compute the correct claims if no updateFormat exists" in {
    getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        updateFormat = None,
        descendingOrder = false,
      )
    ) shouldBe Nil
  }

  it should "require only a public claim for topology format with parties" in {
    getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = None,
            includeReassignments = None,
            includeTopologyEvents = Some(
              TopologyFormat(Some(ParticipantAuthorizationTopologyFormat(parties = Seq("e", "f"))))
            ),
            includeTransactionsAndReassignments = None,
          )
        ),
        descendingOrder = false,
      )
    ) should contain theSameElementsAs RequiredClaims[RequestWithReadAsParty[GetUpdatesRequest]](
      RequiredClaim.Public()
    )
  }

  it should "compute the correct claims for transactions format with wildcard" in {
    getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(
              TransactionFormat(
                eventFormat = Some(
                  EventFormat(
                    filtersByParty = Map.empty,
                    filtersForAnyParty = Some(Filters(Nil)),
                    verbose = false,
                    filtersByContractId = Seq.empty,
                  )
                ),
                transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
              )
            ),
            includeReassignments = None,
            includeTopologyEvents = None,
            includeTransactionsAndReassignments = None,
          )
        ),
        descendingOrder = false,
      )
    ) should contain theSameElementsAs RequiredClaims[RequestWithReadAsParty[GetUpdatesRequest]](
      RequiredClaim.ReadAsAnyParty()
    )
  }

  it should "compute the correct claims for reassignments with wildcard" in {
    getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        updateFormat = Some(
          UpdateFormat(
            includeReassignments = Some(
              EventFormat(
                filtersByParty = Map.empty,
                filtersForAnyParty = Some(Filters(Nil)),
                verbose = false,
                filtersByContractId = Seq.empty,
              )
            ),
            includeTransactions = None,
            includeTopologyEvents = None,
            includeTransactionsAndReassignments = None,
          )
        ),
        descendingOrder = false,
      )
    ) should contain theSameElementsAs RequiredClaims[RequestWithReadAsParty[GetUpdatesRequest]](
      RequiredClaim.ReadAsAnyParty()
    )
  }

  it should "require only a public claim for topology format with wildcard" in {
    getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = None,
            includeReassignments = None,
            includeTopologyEvents = Some(
              TopologyFormat(Some(ParticipantAuthorizationTopologyFormat(parties = Seq.empty)))
            ),
            includeTransactionsAndReassignments = None,
          )
        ),
        descendingOrder = false,
      )
    ) should contain theSameElementsAs RequiredClaims[RequestWithReadAsParty[GetUpdatesRequest]](
      RequiredClaim.Public()
    )
  }

  it should "require a MatchReadAsParties claim if transactions format filters by contract id" in {
    val result = getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(
              TransactionFormat(
                eventFormat = Some(
                  EventFormat(
                    filtersByParty = Map("a" -> Filters(Nil)),
                    filtersForAnyParty = None,
                    verbose = false,
                    filtersByContractId = Seq(contractIdFilter),
                  )
                ),
                transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
              )
            ),
            includeReassignments = None,
            includeTopologyEvents = None,
            includeTransactionsAndReassignments = None,
          )
        ),
        descendingOrder = false,
      )
    )
    assertSingleMatchReadAsParties(result)
    result.filterNot(isMatchReadAsParties) should contain theSameElementsAs RequiredClaims[
      RequestWithReadAsParty[GetUpdatesRequest]
    ](
      RequiredClaim.ReadAs("a")
    )
  }

  it should "require a MatchReadAsParties claim if reassignments format filters by contract id" in {
    val result = getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = None,
            includeReassignments = Some(
              EventFormat(
                filtersByParty = Map.empty,
                filtersForAnyParty = Some(Filters(Nil)),
                verbose = false,
                filtersByContractId = Seq(contractIdFilter),
              )
            ),
            includeTopologyEvents = None,
            includeTransactionsAndReassignments = None,
          )
        ),
        descendingOrder = false,
      )
    )
    assertSingleMatchReadAsParties(result)
    result.filterNot(isMatchReadAsParties) should contain theSameElementsAs RequiredClaims[
      RequestWithReadAsParty[GetUpdatesRequest]
    ](
      RequiredClaim.ReadAsAnyParty()
    )
  }

  it should "require a MatchReadAsParties claim if transactions and reassignments format filters by contract id" in {
    val result = getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = None,
            includeReassignments = None,
            includeTopologyEvents = None,
            includeTransactionsAndReassignments = Some(
              TransactionFormat(
                eventFormat = Some(
                  EventFormat(
                    filtersByParty = Map("a" -> Filters(Nil)),
                    filtersForAnyParty = None,
                    verbose = false,
                    filtersByContractId = Seq(contractIdFilter),
                  )
                ),
                transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
              )
            ),
          )
        ),
        descendingOrder = false,
      )
    )
    assertSingleMatchReadAsParties(result)
    result.filterNot(isMatchReadAsParties) should contain theSameElementsAs RequiredClaims[
      RequestWithReadAsParty[GetUpdatesRequest]
    ](
      RequiredClaim.ReadAs("a")
    )
  }

  it should "require a single MatchReadAsParties claim if both transactions and reassignments formats filter by contract id" in {
    val eventFormatO = Some(
      EventFormat(
        filtersByParty = Map.empty,
        filtersForAnyParty = None,
        verbose = false,
        filtersByContractId = Seq(contractIdFilter),
      )
    )
    val result = getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(
              TransactionFormat(
                eventFormat = eventFormatO,
                transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
              )
            ),
            includeReassignments = eventFormatO,
            includeTopologyEvents = None,
            includeTransactionsAndReassignments = None,
          )
        ),
        descendingOrder = false,
      )
    )
    assertSingleMatchReadAsParties(result)
    result.filterNot(isMatchReadAsParties) should contain theSameElementsAs RequiredClaims[
      RequestWithReadAsParty[GetUpdatesRequest]
    ](
      RequiredClaim.Public()
    )
  }

  it should "not require a MatchReadAsParties claim if no contract id filters are given" in {
    getUpdatesClaims(
      GetUpdatesRequest(
        beginExclusive = 10,
        endInclusive = Some(15),
        updateFormat = Some(
          UpdateFormat(
            includeTransactions = Some(
              TransactionFormat(
                eventFormat = Some(
                  EventFormat(
                    filtersByParty = Map("a" -> Filters(Nil)),
                    filtersForAnyParty = None,
                    verbose = false,
                    filtersByContractId = Seq.empty,
                  )
                ),
                transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
              )
            ),
            includeReassignments = None,
            includeTopologyEvents = None,
            includeTransactionsAndReassignments = None,
          )
        ),
        descendingOrder = false,
      )
    ).exists(isMatchReadAsParties) shouldBe false
  }

  private val contractIdFilter: ContractIdFilter =
    ContractIdFilter(
      contractId = "00abcd",
      includeCreatedEventBlob = false,
      includeInterfaceViews = Seq.empty,
    )

  private def getUpdatesClaims(
      request: GetUpdatesRequest
  ): Seq[RequiredClaim[RequestWithReadAsParty[GetUpdatesRequest]]] =
    UpdateServiceAuthorization.getUpdateFormatClaimsAndFillReadAsParties(request)(
      Lens.unit[GetUpdatesRequest].optionalUpdateFormat
    )

  private def isMatchReadAsParties[Req](claim: RequiredClaim[Req]): Boolean = claim match {
    case _: RequiredClaim.MatchReadAsParties[Req] => true
    case _ => false
  }

  private def assertSingleMatchReadAsParties(
      claims: Seq[RequiredClaim[RequestWithReadAsParty[GetUpdatesRequest]]]
  ): Assertion = {
    val lenses = claims.collect { case RequiredClaim.MatchReadAsParties(partiesL) => partiesL }
    lenses should have size 1
    // the lens must fill in the readAsParty field of the wrapped request
    val wrapped = RequestWithReadAsParty(GetUpdatesRequest.defaultInstance, None)
    val filled = lenses.head.set(Set("alice"))(wrapped)
    filled.readAsParty shouldBe Some(Seq(LfPartyId.assertFromString("alice")))
    lenses.head.get(filled) shouldBe Set("alice")
  }

  behavior of "UserManagementServiceAuthorization.userReaderClaims"

  it should "compute the correct claims in the happy path" in {
    val userIdL = Lens.unit[String]
    val identityProviderIdL = Lens.unit[String]
    UserManagementServiceAuthorization.userReaderClaims[String](
      userIdL = userIdL,
      identityProviderIdL = identityProviderIdL,
    ) should contain theSameElementsAs RequiredClaims[String](
      RequiredClaim.MatchUserIdForUserManagement(userIdL),
      RequiredClaim.MatchIdentityProviderId(identityProviderIdL),
    )
  }

  def readAs[Req]: PartialFunction[RequiredClaim[Req], RequiredClaim.ReadAs[Req]] = {
    case readAs: RequiredClaim.ReadAs[Req] => readAs
  }

  def admin[Req]: PartialFunction[RequiredClaim[Req], RequiredClaim.Admin[Req]] = {
    case admin: RequiredClaim.Admin[Req] => admin
  }

  def adminOrIdp[Req]: PartialFunction[RequiredClaim[Req], RequiredClaim.AdminOrIdpAdmin[Req]] = {
    case adminOrIdp: RequiredClaim.AdminOrIdpAdmin[Req] => adminOrIdp
  }

  def matchUserId[Req]: PartialFunction[RequiredClaim[Req], RequiredClaim.MatchUserId[Req]] = {
    case matchUserId: RequiredClaim.MatchUserId[Req] => matchUserId
  }

  def matchIdentityProviderId[Req]
      : PartialFunction[RequiredClaim[Req], RequiredClaim.MatchIdentityProviderId[Req]] = {
    case matchIdentityProviderId: RequiredClaim.MatchIdentityProviderId[Req] =>
      matchIdentityProviderId
  }
}
object ApiServicesRequiredClaimSpec {
  val transactionFormat = TransactionFormat(
    eventFormat = Some(
      EventFormat(
        filtersByParty = Map(
          "i" -> Filters(Nil),
          "ii" -> Filters(Nil),
        ),
        filtersForAnyParty = Some(Filters(Nil)),
        verbose = true,
        filtersByContractId = Seq.empty,
      )
    ),
    transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
  )
  val submitAndWaitForTransactionRequest =
    SubmitAndWaitForTransactionRequest(
      commands = Some(
        Commands.defaultInstance.copy(
          actAs = Seq("1", "2"),
          readAs = Seq("a", "b"),
          userId = "userId",
        )
      ),
      transactionFormat = Some(transactionFormat),
    )

  val prepareSubmissionRequest =
    PrepareSubmissionRequest(
      userId = "userId",
      commandId = "commandId",
      commands = List.empty,
      minLedgerTime = None,
      actAs = Seq("1", "2"),
      readAs = Seq("a", "b"),
      disclosedContracts = Seq.empty,
      synchronizerId = "",
      packageIdSelectionPreference = Seq.empty,
      verboseHashing = true,
      prefetchContractKeys = Seq.empty,
      maxRecordTime = Option.empty,
      estimateTrafficCost = None,
      tapsMaxPasses = None,
      hashingSchemeVersion = None,
      accountId = "",
    )

  val preparedTransaction = PreparedTransaction(
    transaction = None,
    metadata = Some(
      Metadata(
        submitterInfo = Some(
          SubmitterInfo(
            actAs = Seq("1", "2"),
            commandId = "commandId",
          )
        ),
        synchronizerId = "synchronizerId",
        mediatorGroup = 0,
        transactionUuid = UUID.randomUUID().toString,
        preparationTime = 0,
        inputContracts = Seq.empty,
        minLedgerEffectiveTime = None,
        maxLedgerEffectiveTime = None,
        globalKeyMapping = Seq.empty,
        maxRecordTime = Option.empty,
      )
    ),
  )

  val executeSubmissionRequest =
    ExecuteSubmissionRequest(
      preparedTransaction = Some(preparedTransaction),
      partySignatures = None,
      deduplicationPeriod = ExecuteSubmissionRequest.DeduplicationPeriod.DeduplicationDuration(
        ProtoConverter.DurationConverter.toProtoPrimitive(java.time.Duration.ofSeconds(1))
      ),
      submissionId = "submissionId",
      userId = "userId",
      hashingSchemeVersion = HASHING_SCHEME_VERSION_V2,
      minLedgerTime = None,
      accountId = "",
    )

  val executeSubmissionAndWaitForTransactionRequest =
    ExecuteSubmissionAndWaitForTransactionRequest(
      preparedTransaction = Some(preparedTransaction),
      partySignatures = None,
      deduplicationPeriod =
        ExecuteSubmissionAndWaitForTransactionRequest.DeduplicationPeriod.DeduplicationDuration(
          ProtoConverter.DurationConverter.toProtoPrimitive(java.time.Duration.ofSeconds(1))
        ),
      submissionId = "submissionId",
      userId = "userId",
      hashingSchemeVersion = HASHING_SCHEME_VERSION_V2,
      minLedgerTime = None,
      transactionFormat = Some(transactionFormat),
      accountId = "",
    )
}
