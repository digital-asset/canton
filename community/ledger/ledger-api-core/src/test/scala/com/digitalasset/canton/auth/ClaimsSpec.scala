// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.digitalasset.canton.auth.ClaimApiArea.ledgerRead
import com.digitalasset.daml.lf.data.Ref
import org.scalatest.EitherValues
import org.scalatest.Inspectors.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ClaimsSpec extends AnyWordSpec with EitherValues with Matchers {

  private val alice: Claim = ClaimActAsParty(Ref.Party.assertFromString("alice"))
  private val bob: Claim = ClaimReadAsParty(Ref.Party.assertFromString("bob"))

  private lazy val allApiAreas = {
    val allReadWrite = List[ClaimApiArea.ReadWrite](
      ClaimApiArea.ReadWrite.Read,
      ClaimApiArea.ReadWrite.Write,
    )
    val allSelfReadWrite = List[ClaimApiArea.SelfReadWrite](
      ClaimApiArea.SelfReadWrite.Self,
      ClaimApiArea.SelfReadWrite.Read,
      ClaimApiArea.SelfReadWrite.Write,
    )

    List[List[ClaimApiArea.Area]](
      List(ClaimApiArea.Base(ClaimApiArea.Read)),
      List[ClaimApiArea.LedgerVerb](
        ClaimApiArea.Ledger.Read,
        ClaimApiArea.Ledger.Execute,
        ClaimApiArea.Ledger.Act,
      ).map(ClaimApiArea.Ledger(_)),
      List(ClaimApiArea.Packages(ClaimApiArea.Write)),
      List(ClaimApiArea.Vetting(ClaimApiArea.Write)),
      List[ClaimApiArea.PartiesVerb](
        ClaimApiArea.Parties.Allocate,
        ClaimApiArea.Parties.Self,
        ClaimApiArea.Parties.Read,
        ClaimApiArea.Parties.Write,
      ).map(ClaimApiArea.Parties(_)),
      allSelfReadWrite.map(ClaimApiArea.Users(_)),
      allReadWrite.map(ClaimApiArea.Idps(_)),
      List(ClaimApiArea.Time(ClaimApiArea.Write)),
      allSelfReadWrite.map(ClaimApiArea.Traffic(_)),
    ).flatten
  }

  "API area claims" should {
    "should parse and unparse to a JWT scope" in {
      forEvery(allApiAreas) { area =>
        Claim.parseJwtScope(area.toJwtScope) shouldBe Seq(ClaimApiArea(area))
      }
    }

    "should fit within a small amount of memory" in {
      // Once we hit this limit, we could consider storing these as a bit array
      // since they are present in memory on each request.
      allApiAreas.length should be < 256
    }

    "get a transitive closure from implies" in {
      forEvery(allApiAreas) { area =>
        val first = area.implies.toSet + area
        val second = first.flatMap(_.implies) ++ first
        first shouldBe second
      }
    }

    "be exhaustive for implies" in {
      // This is just a santiy check that can catch some cases where we forget
      // something in the all claims list.
      (allApiAreas ++ allApiAreas.flatMap(_.implies)).toSet shouldBe allApiAreas.toSet
    }
  }

  "Claim.restrict" when {

    "there are no API area restrictions" should {
      "return the assigned claims unchanged for an empty restriction" in {
        val assigned = Seq(alice, ClaimApiArea.usersWrite, ClaimApiArea.ledgerAct)
        Claim.restrict(assigned, Seq.empty).value shouldBe assigned
      }

      "return the assigned claims unchanged if the restriction contains no API areas" in {
        val assigned = Seq(alice, ClaimApiArea.usersWrite)
        Claim.restrict(assigned, Seq[Claim](ClaimAdmin, bob)).value shouldBe assigned
      }

      "return empty claims for empty assigned claims" in {
        Claim.restrict(Seq.empty, Seq.empty).value shouldBe empty
      }
    }

    "restricting to a subset of the assigned areas" should {
      "keep only the restricted areas" in {
        Claim
          .restrict(
            Seq(ClaimApiArea.usersWrite, ClaimApiArea.partiesWrite, ClaimApiArea.timeWrite),
            Seq(ClaimApiArea.partiesWrite),
          )
          .value should contain theSameElementsAs Seq(ClaimApiArea.partiesWrite)
      }

      "accept a restriction identical to the assigned areas" in {
        val assigned = Seq(ClaimApiArea.usersWrite, ClaimApiArea.packagesWrite)
        Claim.restrict(assigned, assigned).value should contain theSameElementsAs assigned
      }

      "accept restricting the full admin set to a single area" in {
        Claim
          .restrict(ClaimApiArea.admin, Seq(ClaimApiArea.vettingWrite))
          .value should contain theSameElementsAs Seq(ClaimApiArea.vettingWrite)
      }

      "keep non-area claims" in {
        Claim
          .restrict(
            Seq[Claim](alice, bob, ClaimApiArea.usersWrite, ClaimApiArea.partiesWrite),
            Seq(ClaimApiArea.usersWrite),
          )
          .value should contain theSameElementsAs Seq(
          alice,
          bob,
          ClaimApiArea.usersWrite,
        )
      }

      "ignore non-area claims in the restriction" in {
        // Restricting cannot add or remove non-area claims
        Claim
          .restrict(
            Seq(alice, ClaimApiArea.usersWrite),
            Seq[Claim](ClaimApiArea.usersWrite, ClaimAdmin, bob),
          )
          .value should contain theSameElementsAs Seq(alice, ClaimApiArea.usersWrite)
      }

      "drop admin when a restriction is present" in {
        // Some endpoints are still checking the presence of Admin.  We want to avoid a token
        // with a restriction passing these checks.
        Claim
          .restrict(
            ClaimApiArea.admin,
            Seq(ClaimApiArea.ledgerRead),
          )
          .value should not contain ClaimAdmin
      }

      "not duplicate claims when the restriction names an area only once" in {
        Claim
          .restrict(
            Seq(ClaimApiArea.usersWrite, ClaimApiArea.usersWrite),
            Seq(ClaimApiArea.usersWrite),
          )
          .value shouldBe Seq(ClaimApiArea.usersWrite)
      }
    }

    "verbs are involved" should {
      "allow downgrading write to read" in {
        Claim
          .restrict(Seq(ClaimApiArea.usersWrite), Seq(ClaimApiArea.usersRead))
          .value shouldBe Seq(ClaimApiArea.usersRead)
      }

      "reject upgrading read to write" in {
        Claim
          .restrict(Seq(ClaimApiArea.usersRead), Seq(ClaimApiArea.usersWrite))
          .left
          .value shouldBe AuthorizationError.InvalidScopeRestriction(ClaimApiArea.usersWrite.area)
      }

      "allow downgrading ledger act to read" in {
        Claim
          .restrict(Seq(ClaimApiArea.ledgerAct), Seq(ClaimApiArea.ledgerRead))
          .value shouldBe Seq(ClaimApiArea.ledgerRead)
      }

      "allow downgrading ledger act to execute" in {
        Claim
          .restrict(Seq(ClaimApiArea.ledgerAct), Seq(ClaimApiArea.ledgerExecute))
          .value shouldBe Seq(ClaimApiArea.ledgerExecute)
      }

      "allow restricting ledger act to both read and execute" in {
        Claim
          .restrict(
            Seq(ClaimApiArea.ledgerAct),
            Seq(ClaimApiArea.ledgerRead, ClaimApiArea.ledgerExecute),
          )
          .value should contain theSameElementsAs Seq(
          ClaimApiArea.ledgerRead,
          ClaimApiArea.ledgerExecute,
        )
      }

      "reject upgrading ledger read to act" in {
        Claim
          .restrict(Seq(ClaimApiArea.ledgerRead), Seq(ClaimApiArea.ledgerAct))
          .left
          .value shouldBe AuthorizationError.InvalidScopeRestriction(ClaimApiArea.ledgerAct.area)
      }

      "reject ledger execute to read, as execute does not imply read" in {
        Claim
          .restrict(Seq(ClaimApiArea.ledgerExecute), Seq(ClaimApiArea.ledgerRead))
          .left
          .value shouldBe AuthorizationError.InvalidScopeRestriction(ClaimApiArea.ledgerRead.area)
      }

      "reject ledger read to execute, as read does not imply execute" in {
        Claim
          .restrict(Seq(ClaimApiArea.ledgerRead), Seq(ClaimApiArea.ledgerExecute))
          .left
          .value shouldBe AuthorizationError.InvalidScopeRestriction(
          ClaimApiArea.ledgerExecute.area
        )
      }

      "merge verbs of multiple assigned claims for the same area" in {
        // Neither read nor execute implies the other, but together they cover both
        Claim
          .restrict(
            Seq(ClaimApiArea.ledgerRead, ClaimApiArea.ledgerExecute),
            Seq(ClaimApiArea.ledgerExecute, ClaimApiArea.ledgerRead),
          )
          .value should contain theSameElementsAs Seq(
          ClaimApiArea.ledgerRead,
          ClaimApiArea.ledgerExecute,
        )
      }

      "not let merged verbs of one claim imply an act claim" in {
        Claim
          .restrict(
            Seq(ClaimApiArea.ledgerRead, ClaimApiArea.ledgerExecute),
            Seq(ClaimApiArea.ledgerAct),
          )
          .left
          .value shouldBe AuthorizationError.InvalidScopeRestriction(ClaimApiArea.ledgerAct.area)
      }
    }

    "restricting to an area that was not assigned" should {
      "reject a completely different area" in {
        Claim
          .restrict(Seq(ClaimApiArea.usersWrite), Seq(ClaimApiArea.partiesRead))
          .left
          .value shouldBe AuthorizationError.InvalidScopeRestriction(ClaimApiArea.partiesRead.area)
      }

      "reject any area if no claims are assigned" in {
        Claim
          .restrict(Seq.empty, Seq(ClaimApiArea.usersRead))
          .left
          .value shouldBe AuthorizationError.InvalidScopeRestriction(ClaimApiArea.usersRead.area)
      }

      "reject any area if only non-area claims are assigned" in {
        Claim
          .restrict(Seq[Claim](ClaimAdmin, alice), Seq(ClaimApiArea.usersRead))
          .left
          .value shouldBe AuthorizationError.InvalidScopeRestriction(ClaimApiArea.usersRead.area)
      }

      "not treat ClaimAdmin as granting API areas after resolving" in {
        Claim
          .restrict(Seq[Claim](ClaimAdmin), Seq(ClaimApiArea.packagesWrite))
          .left
          .value shouldBe AuthorizationError.InvalidScopeRestriction(
          ClaimApiArea.packagesWrite.area
        )
      }

      "not treat areas with different resources as related" in {
        // Same verb, different resource
        Claim
          .restrict(Seq(ClaimApiArea.packagesWrite), Seq(ClaimApiArea.vettingWrite))
          .left
          .value shouldBe AuthorizationError.InvalidScopeRestriction(ClaimApiArea.vettingWrite.area)
      }

      "reject if only one of several restricted areas was not assigned" in {
        Claim
          .restrict(
            Seq(ClaimApiArea.usersWrite),
            Seq(ClaimApiArea.usersRead, ClaimApiArea.partiesRead),
          )
          .left
          .value shouldBe AuthorizationError.InvalidScopeRestriction(ClaimApiArea.partiesRead.area)
      }
    }

    "combined with parsed JWT scopes" should {
      "restrict user claims according to the scope" in {
        Claim
          .restrict(ClaimApiArea.user, Claim.parseJwtScope("ledger:read"))
          .value shouldBe Seq(ClaimApiArea.ledgerRead)
      }

      "leave claims unrestricted if the scope has only unknown claims" in {
        Claim
          .restrict(ClaimApiArea.user, Claim.parseJwtScope("openid profile"))
          .value shouldBe ClaimApiArea.user
      }

      "reject a scope asking for more than the user has" in {
        Claim
          .restrict(ClaimApiArea.user, Claim.parseJwtScope("ledger:read users:write"))
          .left
          .value shouldBe AuthorizationError.InvalidScopeRestriction(ClaimApiArea.usersWrite.area)
      }
    }

    "implies for ledger:read" should {
      "work for base:read" in {
        Claim
          .restrict(List(ClaimApiArea.ledgerRead), Claim.parseJwtScope("base:read"))
          .value shouldBe Seq(ClaimApiArea.baseRead)
      }

      "work for base:read through a user" in {
        Claim
          .restrict(ClaimApiArea.user, Claim.parseJwtScope("base:read"))
          .value shouldBe Seq(ClaimApiArea.baseRead)
      }

      "not work in the opposite direction" in {
        Claim
          .restrict(List(ClaimApiArea.baseRead), List(ledgerRead))
          .left
          .value shouldBe AuthorizationError.InvalidScopeRestriction(ClaimApiArea.ledgerRead.area)
      }
    }
  }
}
