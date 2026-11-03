// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.{
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
import com.digitalasset.canton.logging.NoLogging
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
    EventFormat(filtersByParty, filtersForAnyParty, verbose = false)

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
    val limits = sizeLimitsConfig.copy(
      publicLapiSizeConfig = sizeLimitsConfig.publicLapiSizeConfig.copy(
        eventFormat = EventFormatSizeConfig(
          filtersByPartySizeLimit = PositiveInt.tryCreate(maxFiltersByParty)
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
      FormatValidator.validate(eventFormat)(NoLogging, limits)

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
