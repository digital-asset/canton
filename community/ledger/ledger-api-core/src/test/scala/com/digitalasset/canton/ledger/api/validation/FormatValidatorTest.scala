// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.{
  ContractIdFilter,
  EventFormat,
  Filters,
  ParticipantAuthorizationTopologyFormat,
  TransactionFormat,
  TransactionShape,
  UpdateFormat,
  CumulativeFilter as ProtoCumulativeFilter,
  InterfaceFilter as ProtoInterfaceFilter,
  TemplateFilter as ProtoTemplateFilter,
  WildcardFilter as ProtoWildcardFilter,
}
import com.daml.ledger.api.v2.value.Identifier
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.ledger.api.sizeconfig.v2.{
  EventFormatSizeConfig,
  FiltersSizeConfig,
  ParticipantAuthorizationTopologyFormatSizeConfig,
}
import com.digitalasset.canton.ledger.api.validation.FormatValidator.{
  AllowContractIdFilters,
  RejectContractIdFilters,
}
import com.digitalasset.canton.logging.NoLogging
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.value.Value.ContractId
import io.grpc.Status.Code
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class FormatValidatorTest
    extends AnyWordSpec
    with ValidatorTestUtils
    with SizeLimitTestUtils
    with TableDrivenPropertyChecks {

  private val templateId = Identifier(packageNameRefEncoded, includedModule, includedTemplate)
  private val interfaceSize = FormatValidator.decomposedInterfaceFilterSize

  private val templateFilter = ProtoCumulativeFilter(
    IdentifierFilter.TemplateFilter(
      ProtoTemplateFilter(templateId = Some(templateId), includeCreatedEventBlob = false)
    )
  )
  private val interfaceFilter = ProtoCumulativeFilter(
    IdentifierFilter.InterfaceFilter(
      ProtoInterfaceFilter(
        interfaceId = Some(templateId),
        includeInterfaceView = false,
        includeCreatedEventBlob = false,
      )
    )
  )
  private val wildcardFilter = ProtoCumulativeFilter(
    IdentifierFilter.WildcardFilter(ProtoWildcardFilter(includeCreatedEventBlob = false))
  )
  private val emptyFilter = ProtoCumulativeFilter(IdentifierFilter.Empty)

  private def eventFormat(
      filtersByParty: Map[String, Filters],
      filtersForAnyParty: Option[Filters],
  ): EventFormat =
    EventFormat(filtersByParty, filtersForAnyParty, verbose = false, Seq.empty)

  private def transactionFormat(eventFormat: EventFormat): TransactionFormat =
    TransactionFormat(Some(eventFormat), TransactionShape.TRANSACTION_SHAPE_ACS_DELTA)

  private def updateFormat(
      transactions: Option[EventFormat],
      reassignments: Option[EventFormat],
      transactionsAndReassignments: Option[EventFormat] = None,
  ): UpdateFormat =
    UpdateFormat(
      includeTransactions = transactions.map(transactionFormat),
      includeReassignments = reassignments,
      includeTopologyEvents = None,
      includeTransactionsAndReassignments = transactionsAndReassignments.map(transactionFormat),
    )

  "validating the size limits" should {
    val maxFiltersByParty = 2
    val maxCumulativeFilters = 3
    val maxParticipantAuthorizationParties = 2
    val maxContractIdFilters = 4
    val limits = sizeLimitsConfig.copy(
      publicLapiSizeConfig = sizeLimitsConfig.publicLapiSizeConfig.copy(
        eventFormat = EventFormatSizeConfig(
          filtersByPartySizeLimit = PositiveInt.tryCreate(maxFiltersByParty),
          filtersByContractIdSizeLimit = PositiveInt.tryCreate(maxContractIdFilters),
        ),
        filters = FiltersSizeConfig(
          cumulativeSizeLimit = PositiveInt.tryCreate(maxCumulativeFilters)
        ),
        participantAuthorizationTopologyFormat = ParticipantAuthorizationTopologyFormatSizeConfig(
          partiesSizeLimit = PositiveInt.tryCreate(maxParticipantAuthorizationParties)
        ),
      )
    )

    def validate(eventFormat: EventFormat) =
      FormatValidator.validate(eventFormat, AllowContractIdFilters(None))(NoLogging, limits)

    cardinalityLimit("filters_by_party", maxFiltersByParty) { numberOfParties =>
      validate(eventFormat(parties(numberOfParties).map(_ -> Filters(Nil)).toMap, None))
    }

    cardinalityLimit("cumulative", maxCumulativeFilters, limitName = "limit for a party") {
      numberOfFilters =>
        validate(
          eventFormat(Map(party -> Filters(Seq.fill(numberOfFilters)(templateFilter))), None)
        )
    }

    cardinalityLimit("cumulative", maxCumulativeFilters, limitName = "limit for any party") {
      numberOfFilters =>
        validate(eventFormat(Map.empty, Some(Filters(Seq.fill(numberOfFilters)(templateFilter)))))
    }

    cardinalityLimit("parties", maxParticipantAuthorizationParties) { numberOfParties =>
      FormatValidator.validate(
        ParticipantAuthorizationTopologyFormat(parties(numberOfParties))
      )(NoLogging, limits)
    }

    cardinalityLimit("filters_by_contract_id", maxContractIdFilters) { numberOfContractIdFilters =>
      validate(
        EventFormat.defaultInstance.addFiltersByContractId(
          Seq.tabulate(numberOfContractIdFilters)(id =>
            ContractIdFilter(ContractId.V1(Hash.hashPrivateKey(id.toString)).coid, false, Seq())
          )*
        )
      )
    }
  }

  "filtersSize" should {
    "count template and wildcard filters as 1 and interface filters as the interface size" in {
      forEvery(
        Table[Seq[ProtoCumulativeFilter], Int](
          ("cumulative", "size"),
          (Seq(templateFilter), 1),
          (Seq(wildcardFilter), 1),
          (Seq(interfaceFilter), interfaceSize),
          (Seq(templateFilter, interfaceFilter, wildcardFilter), interfaceSize + 2),
          (Seq(templateFilter, templateFilter), 2),
          (Seq(templateFilter, wildcardFilter), 2),
          (Seq(wildcardFilter, templateFilter), 2),
        )
      ) { (cumulative, size) =>
        FormatValidator.filtersSize(Filters(cumulative)) shouldBe size
      }
    }

    "ignore empty identifier filters next to non-empty ones" in {
      FormatValidator.filtersSize(Filters(Seq(emptyFilter, templateFilter))) shouldBe 1
      FormatValidator.filtersSize(Filters(Seq(templateFilter, emptyFilter))) shouldBe 1
    }

    "count filters without any identifier filter as a template wildcard" in {
      forAll(
        Table[Seq[ProtoCumulativeFilter]](
          "cumulative",
          Seq.empty,
          Seq(emptyFilter),
          Seq(emptyFilter, emptyFilter),
        )
      ) { cumulative =>
        FormatValidator.filtersSize(Filters(cumulative)) shouldBe 1
      }
    }
  }

  "numberOfFilters of an event format" should {
    "sum the filters of every party and of any party" in {
      FormatValidator.numberOfFilters(
        eventFormat(
          filtersByParty = Map(
            party -> Filters(Seq(interfaceFilter)),
            party2 -> Filters(Seq(templateFilter, templateFilter)),
          ),
          filtersForAnyParty = Some(Filters(Seq(wildcardFilter))),
        )
      ) shouldBe interfaceSize + 3
    }

    "count every party with empty filters" in {
      FormatValidator.numberOfFilters(
        eventFormat(
          filtersByParty = parties(3).map(_ -> Filters(Nil)).toMap,
          filtersForAnyParty = None,
        )
      ) shouldBe 3
    }
  }

  "validating contract id filters" should {
    val allowContractIdFilters = AllowContractIdFilters(None)

    val contractIdEventFormat: EventFormat =
      EventFormat.defaultInstance.withFiltersByContractId(protoContractIdFilters)
    val partyEventFormat: EventFormat =
      eventFormat(Map(party -> Filters(Seq(templateFilter))), filtersForAnyParty = None)
    val anyPartyEventFormat: EventFormat =
      eventFormat(Map.empty, filtersForAnyParty = Some(Filters(Seq(wildcardFilter))))

    def invalidArgumentDescription(message: String): String =
      s"INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: $message"

    def validateEventFormat(eventFormat: EventFormat) =
      FormatValidator.validate(eventFormat, allowContractIdFilters)(NoLogging, sizeLimitsConfig)

    def validateTransactionFormat(transactionFormat: TransactionFormat) =
      FormatValidator.validate(transactionFormat, allowContractIdFilters)(
        NoLogging,
        sizeLimitsConfig,
      )

    def validateUpdateFormat(updateFormat: UpdateFormat) =
      FormatValidator.validate(updateFormat, allowContractIdFilters)(NoLogging, sizeLimitsConfig)

    "accept an event format with only contract id filters" in {
      validateEventFormat(contractIdEventFormat).value.filtersByContractId should have size 1
    }

    "reject an event format mixing contract id filters with party filters" in {
      forEvery(
        Table[String, EventFormat](
          ("description", "eventFormat"),
          (
            "filters_by_party",
            contractIdEventFormat.withFiltersByParty(partyEventFormat.filtersByParty),
          ),
          (
            "filters_for_any_party",
            contractIdEventFormat.withFiltersForAnyParty(Filters(Seq(wildcardFilter))),
          ),
          (
            "filters_by_party and filters_for_any_party",
            contractIdEventFormat
              .withFiltersByParty(partyEventFormat.filtersByParty)
              .withFiltersForAnyParty(Filters(Seq(wildcardFilter))),
          ),
        )
      ) { (_, eventFormat) =>
        requestMustFailWith(
          request = validateEventFormat(eventFormat),
          code = Code.INVALID_ARGUMENT,
          description = invalidArgumentDescription(
            "filtersByContractId cannot be used together with filtersByParty"
          ),
        )
      }
    }

    "reject contract id filters when they are not allowed" in {
      mustRejectContractIdFilters(
        FormatValidator.validate(contractIdEventFormat, RejectContractIdFilters)(
          NoLogging,
          sizeLimitsConfig,
        )
      )
    }

    "accept contract id filters with the ACS_DELTA transaction shape" in {
      validateTransactionFormat(
        TransactionFormat(Some(contractIdEventFormat), TransactionShape.TRANSACTION_SHAPE_ACS_DELTA)
      ).value.eventFormat.filtersByContractId should have size 1
    }

    "reject contract id filters with the LEDGER_EFFECTS transaction shape" in {
      requestMustFailWith(
        request = validateTransactionFormat(
          TransactionFormat(
            Some(contractIdEventFormat),
            TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS,
          )
        ),
        code = Code.INVALID_ARGUMENT,
        description = invalidArgumentDescription(
          "Contract id filter is supported only for ACS_DELTA transaction shape"
        ),
      )
    }

    "accept party filters with the LEDGER_EFFECTS transaction shape" in {
      validateTransactionFormat(
        TransactionFormat(Some(partyEventFormat), TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS)
      ) shouldBe a[Right[?, ?]]
    }

    "reject contract id filters with the LEDGER_EFFECTS shape in an update format" in {
      forEvery(
        Table[String, UpdateFormat](
          ("description", "updateFormat"),
          (
            "include_transactions",
            UpdateFormat.defaultInstance.withIncludeTransactions(
              TransactionFormat(
                Some(contractIdEventFormat),
                TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS,
              )
            ),
          ),
          (
            "include_transactions_and_reassignments",
            UpdateFormat.defaultInstance.withIncludeTransactionsAndReassignments(
              TransactionFormat(
                Some(contractIdEventFormat),
                TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS,
              )
            ),
          ),
        )
      ) { (_, updateFormat) =>
        requestMustFailWith(
          request = validateUpdateFormat(updateFormat),
          code = Code.INVALID_ARGUMENT,
          description = invalidArgumentDescription(
            "Contract id filter is supported only for ACS_DELTA transaction shape"
          ),
        )
      }
    }

    "accept update formats using the same filter type for transactions and reassignments" in {
      forEvery(
        Table[String, UpdateFormat](
          ("description", "updateFormat"),
          (
            "contract id filters in both",
            updateFormat(Some(contractIdEventFormat), Some(contractIdEventFormat)),
          ),
          (
            "party filters in both",
            updateFormat(Some(partyEventFormat), Some(anyPartyEventFormat)),
          ),
          (
            "contract id filters in transactions only",
            updateFormat(Some(contractIdEventFormat), None),
          ),
          (
            "contract id filters in reassignments only",
            updateFormat(None, Some(contractIdEventFormat)),
          ),
          (
            "contract id filters in include_transactions_and_reassignments",
            updateFormat(None, None, transactionsAndReassignments = Some(contractIdEventFormat)),
          ),
        )
      ) { (_, updateFormat) =>
        validateUpdateFormat(updateFormat) shouldBe a[Right[?, ?]]
      }
    }

    "reject contract id filters for transactions with party filters for reassignments" in {
      forEvery(
        Table[EventFormat]("reassignments", partyEventFormat, anyPartyEventFormat)
      ) { reassignments =>
        requestMustFailWith(
          request =
            validateUpdateFormat(updateFormat(Some(contractIdEventFormat), Some(reassignments))),
          code = Code.INVALID_ARGUMENT,
          description = invalidArgumentDescription(
            "If contract id filter is used in include_transaction, no other filter types are allowed in include_reassignments"
          ),
        )
      }
    }

    "reject party filters for transactions with contract id filters for reassignments" in {
      forEvery(
        Table[EventFormat]("transactions", partyEventFormat, anyPartyEventFormat)
      ) { transactions =>
        requestMustFailWith(
          request =
            validateUpdateFormat(updateFormat(Some(transactions), Some(contractIdEventFormat))),
          code = Code.INVALID_ARGUMENT,
          description = invalidArgumentDescription(
            "If contract id filter is used in include_reassignments, no other filter types are allowed in include_transactions"
          ),
        )
      }
    }
  }

  "numberOfFilters of an update format" should {
    "sum the event formats of transactions and reassignments" in {
      val transactionsEventFormat =
        eventFormat(Map(party -> Filters(Seq(interfaceFilter))), filtersForAnyParty = None)
      val reassignmentsEventFormat =
        eventFormat(Map.empty, filtersForAnyParty = Some(Filters(Seq(templateFilter))))

      forEvery(
        Table[Option[EventFormat], Option[EventFormat], Int](
          ("transactions", "reassignments", "size"),
          (None, None, 0),
          (Some(transactionsEventFormat), None, interfaceSize),
          (None, Some(reassignmentsEventFormat), 1),
          (Some(transactionsEventFormat), Some(reassignmentsEventFormat), interfaceSize + 1),
        )
      ) { (transactions, reassignments, size) =>
        FormatValidator.numberOfFilters(updateFormat(transactions, reassignments)) shouldBe size
      }
    }

    "count the event format of include_transactions_and_reassignments for both" in {
      val eventFormatForBoth =
        eventFormat(Map(party -> Filters(Seq(interfaceFilter))), filtersForAnyParty = None)
      FormatValidator.numberOfFilters(
        updateFormat(
          transactions = None,
          reassignments = None,
          transactionsAndReassignments = Some(eventFormatForBoth),
        )
      ) shouldBe 2 * interfaceSize
    }
  }
}
