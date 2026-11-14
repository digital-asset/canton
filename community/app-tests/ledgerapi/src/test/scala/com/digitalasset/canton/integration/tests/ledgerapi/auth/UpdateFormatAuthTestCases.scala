// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.transaction_filter.*
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*

trait UpdateFormatAuthTestCases {
  self: ReadOnlyServiceCallAuthTests =>

  private def transactionsAndReassignmentsFormat(eventFormat: EventFormat): Option[UpdateFormat] =
    Some(
      UpdateFormat(
        includeTransactions = None,
        includeReassignments = None,
        includeTopologyEvents = None,
        includeTransactionsAndReassignments = Some(
          TransactionFormat(
            eventFormat = Some(eventFormat),
            transactionShape = TransactionShape.TRANSACTION_SHAPE_ACS_DELTA,
          )
        ),
      )
    )

  private def transactionsAndReassignmentsFormatForParties(
      parties: String*
  ): Option[UpdateFormat] =
    transactionsAndReassignmentsFormat(
      EventFormat(
        filtersByParty = parties.map(_ -> Filters(Nil)).toMap,
        filtersForAnyParty = None,
        verbose = false,
      )
    )

  private def transactionsAndReassignmentsFormatPartyWildcard: Option[UpdateFormat] =
    transactionsAndReassignmentsFormat(
      EventFormat(
        filtersByParty = Map.empty,
        filtersForAnyParty = Some(Filters(Nil)),
        verbose = false,
      )
    )

  serviceCallName should {
    "allow calls with transactions-and-reassignments filters of the correct party" taggedAs securityAsset
      .setHappyCase(
        "Present a JWT that can read as the party of transactions-and-reassignments filters"
      ) in { implicit env =>
      import env.*
      expectSuccess(
        serviceCall(
          canReadAsMainActor.copy(updateFormat =
            transactionsAndReassignmentsFormatForParties(getMainActorId)
          )
        )
      )
    }

    "deny calls with transactions-and-reassignments filters of the wrong party" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT that cannot read for a party of transactions-and-reassignments filters"
        )
      ) in { implicit env =>
      import env.*
      expectPermissionDenied(
        serviceCall(
          canActAsRandomParty.copy(updateFormat =
            transactionsAndReassignmentsFormatForParties(getMainActorId)
          )
        )
      )
    }

    "deny calls with transactions-and-reassignments filters of the wrong party along with the correct" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT that cannot read for one of the parties of transactions-and-reassignments filters"
        )
      ) in { implicit env =>
      import env.*
      expectPermissionDenied(
        serviceCall(
          canActAsMainActor.copy(updateFormat =
            transactionsAndReassignmentsFormatForParties(getMainActorId, getRandomPartyId)
          )
        )
      )
    }

    "deny calls with transactions-and-reassignments filters of party wildcard with a user that can act as main actor" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT that cannot read as any party of transactions-and-reassignments filters"
        )
      ) in { implicit env =>
      import env.*
      expectPermissionDenied(
        serviceCall(
          canActAsMainActor.copy(updateFormat = transactionsAndReassignmentsFormatPartyWildcard)
        )
      )
    }

    "allow calls with transactions-and-reassignments filters of party wildcard with a can-read-as-any-party user" taggedAs securityAsset
      .setHappyCase(
        "Present a JWT that can read as any party of transactions-and-reassignments filters"
      ) in { implicit env =>
      import env.*
      expectSuccess(
        serviceCall(
          canReadAsAnyParty.copy(updateFormat = transactionsAndReassignmentsFormatPartyWildcard)
        )
      )
    }

    "allow calls with transaction filters of the correct party" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT with an unknown party authorized to read/write"
        )
      ) in { implicit env =>
      import env.*
      expectSuccess(
        serviceCall(canReadAsMainActor.copy(updateFormat = updateFormat(Some(getMainActorId))))
      )
    }

    "deny calls with transaction filters of the wrong party" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT that cannot read for a party of transaction filters"
        )
      ) in { implicit env =>
      import env.*
      expectPermissionDenied(
        serviceCall(canActAsRandomParty.copy(updateFormat = updateFormat(Some(getMainActorId))))
      )
    }

    "deny calls with reassignment filters of the wrong party" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT that cannot read for a party of reassignment filters"
        )
      ) in { implicit env =>
      import env.*
      expectPermissionDenied(
        serviceCall(
          canActAsMainActor.copy(updateFormat =
            updateFormat(
              transactionsPartyO = None,
              reassignmentsPartyO = Some(getRandomPartyId),
            )
          )
        )
      )
    }

    "allow calls with topology filters of the wrong party" taggedAs securityAsset
      .setHappyCase(
        "Present a JWT that cannot read for the single party of topology filters"
      ) in { implicit env =>
      import env.*
      expectSuccess(
        serviceCall(
          canActAsMainActor.copy(updateFormat =
            updateFormat(
              transactionsPartyO = None,
              reassignmentsPartyO = None,
              topologyPartiesO = Some(Seq(getRandomPartyId)),
            )
          )
        )
      )
    }

    "allow calls with topology filters of the wrong party along with the correct" taggedAs securityAsset
      .setHappyCase(
        "Present a JWT that cannot read for a party of topology filters"
      ) in { implicit env =>
      import env.*
      expectSuccess(
        serviceCall(
          canActAsMainActor.copy(updateFormat =
            updateFormat(
              transactionsPartyO = None,
              reassignmentsPartyO = None,
              topologyPartiesO = Some(Seq(getMainActorId, getRandomPartyId)),
            )
          )
        )
      )
    }

    "allow calls with topology filters of party wildcard" taggedAs securityAsset
      .setHappyCase(
        "Present a JWT that cannot read as any party of topology filters"
      ) in { implicit env =>
      import env.*
      expectSuccess(
        serviceCall(
          canActAsMainActor.copy(updateFormat =
            updateFormat(
              transactionsPartyO = None,
              reassignmentsPartyO = None,
              topologyPartiesO = Some(Seq.empty),
            )
          )
        )
      )
    }

    "allow calls with transaction filters of party wildcard with a user that can act as main actor" taggedAs securityAsset
      .setHappyCase(
        "Present a JWT that cannot read as any party of transaction filters"
      ) in { implicit env =>
      import env.*
      expectSuccess(
        serviceCall(
          canReadAsAnyParty.copy(updateFormat =
            Some(
              UpdateFormat(
                includeTransactions = Some(
                  TransactionFormat(
                    eventFormat = Some(
                      EventFormat(
                        filtersByParty = Map.empty,
                        filtersForAnyParty = Some(Filters(Nil)),
                        verbose = false,
                      )
                    ),
                    transactionShape = TransactionShape.TRANSACTION_SHAPE_ACS_DELTA,
                  )
                ),
                includeReassignments = None,
                includeTopologyEvents = None,
                includeTransactionsAndReassignments = None,
              )
            )
          )
        )
      )
    }

    "deny calls with transaction filters of party wildcard with a user that can act as main actor" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT that cannot read as any party of transaction filters"
        )
      ) in { implicit env =>
      import env.*
      expectPermissionDenied(
        serviceCall(
          canActAsMainActor.copy(updateFormat =
            Some(
              UpdateFormat(
                includeTransactions = Some(
                  TransactionFormat(
                    eventFormat = Some(
                      EventFormat(
                        filtersByParty = Map.empty,
                        filtersForAnyParty = Some(Filters(Nil)),
                        verbose = false,
                      )
                    ),
                    transactionShape = TransactionShape.TRANSACTION_SHAPE_ACS_DELTA,
                  )
                ),
                includeReassignments = None,
                includeTopologyEvents = None,
                includeTransactionsAndReassignments = None,
              )
            )
          )
        )
      )
    }

    "deny calls with reassignment filters of party wildcard with a user that can act as main actor" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT that cannot read as any party of reassignment filters"
        )
      ) in { implicit env =>
      import env.*
      expectPermissionDenied(
        serviceCall(
          canActAsMainActor.copy(updateFormat =
            Some(
              UpdateFormat(
                includeTransactions = None,
                includeReassignments = Some(
                  EventFormat(
                    filtersByParty = Map.empty,
                    filtersForAnyParty = Some(Filters(Nil)),
                    verbose = false,
                  )
                ),
                includeTopologyEvents = None,
                includeTransactionsAndReassignments = None,
              )
            )
          )
        )
      )
    }

    "allow calls with reassignment filters of party wildcard with a user that can act as main actor" taggedAs securityAsset
      .setHappyCase(
        "Present a JWT that cannot read as any party of reassignment filters"
      ) in { implicit env =>
      import env.*
      expectSuccess(
        serviceCall(
          canReadAsAnyParty.copy(updateFormat =
            updateFormat(Some(getMainActorId)).map(
              _.update(
                _.includeReassignments.filtersForAnyParty := Filters(Nil)
              )
            )
          )
        )
      )
    }

    "allow calls with topology filters of party wildcard with a can-read-as-any-party user" taggedAs securityAsset
      .setHappyCase(
        "Present a JWT that can read as any party of topology filters"
      ) in { implicit env =>
      import env.*
      expectSuccess(
        serviceCall(
          canReadAsAnyParty.copy(updateFormat =
            updateFormat(
              transactionsPartyO = None,
              reassignmentsPartyO = None,
              topologyPartiesO = Some(Seq.empty),
            )
          )
        )
      )
    }

    "allow calls with topology filters of several parties with a can-read-as-any-party user" taggedAs securityAsset
      .setHappyCase(
        "Present a JWT that can read as any party of topology filters"
      ) in { implicit env =>
      import env.*
      expectSuccess(
        serviceCall(
          canReadAsAnyParty.copy(updateFormat =
            updateFormat(
              transactionsPartyO = None,
              reassignmentsPartyO = None,
              topologyPartiesO = Some(Seq(getMainActorId, getRandomPartyId)),
            )
          )
        )
      )
    }
  }
}
