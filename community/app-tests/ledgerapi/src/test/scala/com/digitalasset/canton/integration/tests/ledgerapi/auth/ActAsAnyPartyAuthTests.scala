// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.AuthServiceJWTSuppressionRule
import org.scalatest.Assertion

import scala.concurrent.{ExecutionContext, Future}

/** Tests for the ``CanActAsAnyParty`` right. A user holding this right may act as any party, so it
  * must be authorized wherever an act-as (or, transitively, read-as / execute-as) claim for a
  * specific party is required.
  *
  * The notion of a successful (i.e. authorized) call is delegated to ``successfulBehavior``, which
  * each concrete service provides (e.g. a successful submission, or a non-PERMISSION_DENIED
  * rejection).
  */
trait ActAsAnyPartyAuthTests { self: ServiceCallWithMainActorAuthTests =>

  /** Provided by [[SyncServiceCallAuthTests]] / [[ReadOnlyServiceCallAuthTests]] of the concrete
    * service under test.
    */
  def successfulBehavior(f: Future[Any])(implicit ec: ExecutionContext): Assertion

  serviceCallName should {
    "allow calls with can-act-as-any-party token without expiration" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client with the can-act-as-any-party right can act as the main actor"
      ) in { implicit env =>
      import env.*
      successfulBehavior(serviceCall(canActAsAnyParty))
    }

    "allow calls with can-act-as-any-party token that expires in an hour" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client with the can-act-as-any-party right can act as the main actor with a non-expired token"
      ) in { implicit env =>
      import env.*
      successfulBehavior(serviceCall(canActAsAnyPartyExpiresInAnHour))
    }

    "deny calls with an expired can-act-as-any-party token" taggedAs securityAsset.setAttack(
      attackUnauthenticated(threat = "Present an expired can-act-as-any-party JWT")
    ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(serviceCall(canActAsAnyPartyExpired))
      }
    }
  }
}
