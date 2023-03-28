// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.sandbox.services.identity

import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundEach
import com.digitalasset.canton.platform.sandbox.SandboxBackend
import com.digitalasset.canton.platform.sandbox.fixture.SandboxFixture
import com.digitalasset.canton.platform.store.testing.postgresql.PostgresAroundAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scalaz.syntax.tag.*

import java.util.UUID

sealed trait LedgerIdentityServiceITBaseGiven
    extends AnyWordSpec
    with Matchers
    with SandboxFixture
    with SuiteResourceManagementAroundEach {

  private lazy val givenLedgerId: String = UUID.randomUUID.toString

  override def config = super.config.copy(
    ledgerId = givenLedgerId
  )

  // This test relies on inheriting from SuiteResourceManagementAroundEach to restart the ledger across test cases

  "A platform" when {
    "started" should {
      "expose the expected ledger identifier" in {
        ledgerId().unwrap shouldEqual givenLedgerId
      }
      "expose the expected ledger identifier across restarts" in {
        ledgerId().unwrap shouldEqual givenLedgerId
      }
    }
  }
}

final class LedgerIdentityServicePostgresGivenIT
    extends LedgerIdentityServiceITBaseGiven
    with SandboxBackend.Postgresql

sealed trait LedgerIdentityServiceITBaseDynamic
    extends AnyWordSpec
    with Matchers
    with SandboxFixture
    with SuiteResourceManagementAroundEach {

  @volatile private var firstRunLedgerId: String = _

  override def config = super.config.copy(
    ledgerId = UUID.randomUUID.toString
  )

  // This test relies on inheriting from SuiteResourceManagementAroundEach to restart the ledger across test cases

  "A platform" when {

    "started" should {

      "expose a ledger identifer" in {
        firstRunLedgerId = ledgerId().unwrap
        firstRunLedgerId should not be empty
      }

      "have different identifiers across restarts" in {
        firstRunLedgerId should not equal ledgerId().unwrap
      }
    }
  }
}

final class LedgerIdentityServicePostgresDynamicIT
    extends LedgerIdentityServiceITBaseDynamic
    with SandboxBackend.Postgresql

final class LedgerIdentityServicePostgresDynamicSharedPostgresIT
    extends AnyWordSpec
    with Matchers
    with SandboxFixture
    with SuiteResourceManagementAroundEach
    with PostgresAroundAll {

  override def config = super.config.copy(
    ledgerId = Option(firstRunLedgerId).fold(UUID.randomUUID.toString)(identity)
  )

  @volatile private var firstRunLedgerId: String = _

  // This test relies on inheriting from SuiteResourceManagementAroundEach to restart the ledger
  // across test cases AND on PostgresAroundAll to share the Postgres instance across restarts to
  // test the peculiar semantics of this case

  "A platform" when {
    "started" should {
      "expose a ledger identifer" in {
        firstRunLedgerId = ledgerId().unwrap
        firstRunLedgerId should not be empty
      }

      "have the assigned random ledger identifier after a restart" in {
        firstRunLedgerId shouldEqual ledgerId().unwrap
      }
    }
  }
}
