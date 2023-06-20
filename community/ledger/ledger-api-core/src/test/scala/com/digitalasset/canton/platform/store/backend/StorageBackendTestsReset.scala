// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsReset extends Matchers with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (reset)"

  import StorageBackendTestValues.*

  it should "start with an empty index" in {
    val identity = executeSql(backend.parameter.ledgerIdentity)
    val end = executeSql(backend.parameter.ledgerEnd)
    val parties = executeSql(backend.party.knownParties)
    val config = executeSql(backend.configuration.ledgerConfiguration)
    val packages = executeSql(backend.packageBackend.lfPackages)
    val events = executeSql(backend.contract.contractStateEvents(0, Long.MaxValue))
    val stringInterningEntries = executeSql(
      backend.stringInterning.loadStringInterningEntries(0, 1000)
    )

    identity shouldBe None
    end shouldBe ParameterStorageBackend.LedgerEnd.beforeBegin
    parties shouldBe empty
    packages shouldBe empty
    events shouldBe empty
    config shouldBe None
    stringInterningEntries shouldBe empty
  }

  it should "not see any data after advancing the ledger end" in {
    advanceLedgerEndToMakeOldDataVisible()
    val parties = executeSql(backend.party.knownParties)
    val config = executeSql(backend.configuration.ledgerConfiguration)
    val packages = executeSql(backend.packageBackend.lfPackages)

    parties shouldBe empty
    packages shouldBe empty
    config shouldBe None
  }

  it should "reset everything when using resetAll" in {
    val dtos: Vector[DbDto] = Vector(
      // 1: config change
      dtoConfiguration(offset(1)),
      // 2: party allocation
      dtoPartyEntry(offset(2)),
      // 3: package upload
      dtoPackage(offset(3)),
      dtoPackageEntry(offset(3)),
      // 4: transaction with create node
      dtoCreate(offset(4), 1L, hashCid("#4")),
      DbDto.IdFilterCreateStakeholder(1L, someTemplateId.toString, someParty.toString),
      dtoCompletion(offset(4)),
      // 5: transaction with exercise node and retroactive divulgence
      dtoExercise(offset(5), 2L, true, hashCid("#4")),
      dtoDivulgence(Some(offset(5)), 3L, hashCid("#4")),
      dtoCompletion(offset(5)),
      // 6: assign event
      dtoAssign(offset(6), 4L, hashCid("#5")),
      DbDto.IdFilterAssignStakeholder(4L, someTemplateId.toString, someParty.toString),
      // 7: unassign event
      dtoUnassign(offset(7), 5L, hashCid("#6")),
      DbDto.IdFilterUnassignStakeholder(5L, someTemplateId.toString, someParty.toString),
      // String interning
      DbDto.StringInterningDto(10, "d|x:abc"),
    )

    // Initialize and insert some data
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(ledgerEnd(5, 3L)))

    // queries
    def identity = executeSql(backend.parameter.ledgerIdentity)

    def end = executeSql(backend.parameter.ledgerEnd)

    def events = executeSql(backend.contract.contractStateEvents(0, Long.MaxValue))

    def parties = executeSql(backend.party.knownParties)

    def config = executeSql(backend.configuration.ledgerConfiguration)

    def packages = executeSql(backend.packageBackend.lfPackages)

    def stringInterningEntries = executeSql(
      backend.stringInterning.loadStringInterningEntries(0, 1000)
    )

    def filterIds = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholder = someParty,
        templateIdO = None,
        startExclusive = 0,
        endInclusive = 1000,
        limit = 1000,
      )
    )

    def assignEvents = executeSql(
      backend.event.assignEventBatch(
        eventSequentialIds = List(4),
        allFilterParties = Set.empty,
      )
    )

    def unassignEvents = executeSql(
      backend.event.unassignEventBatch(
        eventSequentialIds = List(5),
        allFilterParties = Set.empty,
      )
    )

    def assignIds = executeSql(
      backend.event.fetchAssignEventIdsForStakeholder(
        stakeholder = someParty,
        templateId = None,
        startExclusive = 0L,
        endInclusive = 1000L,
        1000,
      )
    )

    def unassignIds = executeSql(
      backend.event.fetchUnassignEventIdsForStakeholder(
        stakeholder = someParty,
        templateId = None,
        startExclusive = 0L,
        endInclusive = 1000L,
        1000,
      )
    )

    // verify queries indeed returning something
    identity should not be None
    end should not be ParameterStorageBackend.LedgerEnd.beforeBegin
    events should not be empty
    parties should not be empty
    packages should not be empty
    config should not be None
    stringInterningEntries should not be empty
    filterIds should not be empty
    assignEvents should not be empty
    unassignEvents should not be empty
    assignIds should not be empty
    unassignIds should not be empty

    // Reset
    executeSql(backend.reset.resetAll)

    // Check the contents (queries that do not depend on ledger end)
    identity shouldBe None
    end shouldBe ParameterStorageBackend.LedgerEnd.beforeBegin
    events shouldBe empty

    // Check the contents (queries that don't read beyond ledger end)
    advanceLedgerEndToMakeOldDataVisible()

    parties shouldBe empty
    packages shouldBe empty // Note: resetAll() does delete packages
    config shouldBe None
    stringInterningEntries shouldBe empty
    filterIds shouldBe empty
    assignEvents shouldBe empty
    unassignEvents shouldBe empty
    assignIds shouldBe empty
    unassignIds shouldBe empty
  }

  // Some queries are protected to never return data beyond the current ledger end.
  // By advancing the ledger end to a large value, we can check whether these
  // queries now find any left-over data not cleaned by reset.
  private def advanceLedgerEndToMakeOldDataVisible(): Unit = {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(ledgerEnd(10000, 10000)))
    ()
  }
}
