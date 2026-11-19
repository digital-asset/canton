// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.daml.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.auth.ClaimApiArea
import com.digitalasset.canton.tea.AccountId
import com.digitalasset.canton.{BaseTest, LfLedgerString}
import com.digitalasset.daml.lf.data.Ref
import io.grpc.{Status, StatusRuntimeException}
import org.mockito.MockitoSugar
import org.scalatest.Assertion
import org.scalatest.Assertions.succeed
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import scalapb.lenses.Lens

import java.time.Instant
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class AuthorizerSpec
    extends AsyncFlatSpec
    with BaseTest
    with Matchers
    with MockitoSugar
    with PekkoBeforeAndAfterAll {

  import AuthorizerSpec.*

  private val className = classOf[Authorizer].getSimpleName
  private val dummyRequest = 1337L
  private val expectedSuccessfulResponse = "expectedSuccessfulResponse"
  private val dummyReqRes: Long => Future[String] =
    Map(dummyRequest -> Future.successful(expectedSuccessfulResponse))

  private val party = Ref.Party.assertFromString("party")
  private val party2 = Ref.Party.assertFromString("party2")
  private val account = AccountId.tryCreate("party")
  private val account2 = AccountId.tryCreate("party2")
  private val debitAcceptingExecute =
    RequiredClaim.DebitTrafficFrom[String]("party", acceptExecuteRights = true)
  private val debitIgnoringExecute =
    RequiredClaim.DebitTrafficFrom[String]("party", acceptExecuteRights = false)
  private val listAcceptingExecute =
    RequiredClaim.ListTrafficAccounts[String](acceptExecuteRights = true)
  private val listIgnoringExecute =
    RequiredClaim.ListTrafficAccounts[String](acceptExecuteRights = false)

  private def withClaims(claims: Claim*): ClaimSet.Claims =
    ClaimSet.Claims.Empty.copy(claims = claims)

  behavior of s"$className.authorize for RequiredClaim.Admin"

  List(
    TestDefinition(RequiredClaim.Admin(), ClaimSet.Claims.Empty, expectedPermissionDenied),
    TestDefinition(RequiredClaim.Admin(), ClaimSet.Claims.Wildcard, ExpectedSuccess),
    TestDefinition(
      RequiredClaim.Admin(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimAdmin)),
      ExpectedSuccess,
    ),
    TestDefinition(RequiredClaim.Admin(), ClaimSet.Claims.Admin, ExpectedSuccess),
    TestDefinition(
      RequiredClaim.Admin(),
      withClaims(ClaimApiArea.admin*),
      expectedPermissionDenied,
      descSuffix = "(admin API areas without ClaimAdmin)",
    ),
    TestDefinition(
      RequiredClaim.Admin(),
      withClaims(ClaimApiArea.idpAdmin*),
      expectedPermissionDenied,
      descSuffix = "(IDP admin API areas)",
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for RequiredClaim.ApiArea"

  List[(ClaimSet.Claims, ClaimApiArea, ExpectedResult)](
    // ledger verbs: act implies read and execute, but read and execute are independent
    (withClaims(ClaimApiArea.ledgerRead), ClaimApiArea.ledgerRead, ExpectedSuccess),
    (withClaims(ClaimApiArea.ledgerAct), ClaimApiArea.ledgerRead, ExpectedSuccess),
    (withClaims(ClaimApiArea.ledgerExecute), ClaimApiArea.ledgerRead, expectedPermissionDenied),
    (withClaims(ClaimApiArea.ledgerExecute), ClaimApiArea.ledgerExecute, ExpectedSuccess),
    (withClaims(ClaimApiArea.ledgerAct), ClaimApiArea.ledgerExecute, ExpectedSuccess),
    (withClaims(ClaimApiArea.ledgerRead), ClaimApiArea.ledgerExecute, expectedPermissionDenied),
    (withClaims(ClaimApiArea.ledgerAct), ClaimApiArea.ledgerAct, ExpectedSuccess),
    (
      withClaims(ClaimApiArea.ledgerRead, ClaimApiArea.ledgerExecute),
      ClaimApiArea.ledgerAct,
      expectedPermissionDenied,
    ),
    // parties verbs: write implies read, but not allocate
    (withClaims(ClaimApiArea.partiesAllocate), ClaimApiArea.partiesAllocate, ExpectedSuccess),
    (
      withClaims(ClaimApiArea.partiesWrite),
      ClaimApiArea.partiesAllocate,
      expectedPermissionDenied,
    ),
    (withClaims(ClaimApiArea.partiesWrite), ClaimApiArea.partiesRead, ExpectedSuccess),
    (withClaims(ClaimApiArea.partiesAllocate), ClaimApiArea.partiesRead, expectedPermissionDenied),
    (withClaims(ClaimApiArea.partiesRead), ClaimApiArea.partiesWrite, expectedPermissionDenied),
    // read/write areas: write implies read
    (withClaims(ClaimApiArea.usersWrite), ClaimApiArea.usersRead, ExpectedSuccess),
    (withClaims(ClaimApiArea.usersRead), ClaimApiArea.usersWrite, expectedPermissionDenied),
    (withClaims(ClaimApiArea.idpsWrite), ClaimApiArea.idpsRead, ExpectedSuccess),
    (withClaims(ClaimApiArea.trafficWrite), ClaimApiArea.trafficRead, ExpectedSuccess),
    (withClaims(ClaimApiArea.trafficRead), ClaimApiArea.trafficWrite, expectedPermissionDenied),
    // areas of different resources are unrelated
    (withClaims(ClaimApiArea.vettingWrite), ClaimApiArea.packagesWrite, expectedPermissionDenied),
    (withClaims(ClaimApiArea.usersWrite), ClaimApiArea.idpsRead, expectedPermissionDenied),
    // non-area claims do not grant API areas
    (ClaimSet.Claims.Empty, ClaimApiArea.ledgerRead, expectedPermissionDenied),
    (withClaims(ClaimAdmin), ClaimApiArea.timeWrite, expectedPermissionDenied),
    (withClaims(ClaimActAsAnyParty), ClaimApiArea.ledgerRead, expectedPermissionDenied),
    (
      withClaims(ClaimDebitTrafficFromAnyAccount),
      ClaimApiArea.trafficRead,
      expectedPermissionDenied,
    ),
    // predefined sets
    (ClaimSet.Claims.Admin, ClaimApiArea.timeWrite, ExpectedSuccess),
    (ClaimSet.Claims.Wildcard, ClaimApiArea.packagesWrite, ExpectedSuccess),
    (withClaims(ClaimApiArea.idpAdmin*), ClaimApiArea.usersWrite, ExpectedSuccess),
    (withClaims(ClaimApiArea.idpAdmin*), ClaimApiArea.trafficWrite, expectedPermissionDenied),
    (withClaims(ClaimApiArea.idpAdmin*), ClaimApiArea.idpsRead, expectedPermissionDenied),
    (withClaims(ClaimApiArea.idpAdmin*), ClaimApiArea.packagesWrite, expectedPermissionDenied),
    (withClaims(ClaimApiArea.user*), ClaimApiArea.partiesAllocate, ExpectedSuccess),
    (withClaims(ClaimApiArea.user*), ClaimApiArea.partiesRead, expectedPermissionDenied),
    (withClaims(ClaimApiArea.user*), ClaimApiArea.usersRead, expectedPermissionDenied),
  ).foreach { case (claims, required, expected) =>
    generateAuthorizationTest(
      TestDefinition(
        RequiredClaim.ApiArea(required.area),
        claims,
        expected,
        descSuffix = s"requiring ${required.area.toJwtScope}",
      )
    )
  }

  behavior of s"$className.authorize for RequiredClaim.ReadAs"

  List(
    TestDefinition(RequiredClaim.ReadAs("party"), ClaimSet.Claims.Empty, expectedPermissionDenied),
    TestDefinition(RequiredClaim.ReadAs("party"), ClaimSet.Claims.Wildcard, ExpectedSuccess),
    TestDefinition(RequiredClaim.ReadAs("party"), ClaimSet.Claims.Admin, expectedPermissionDenied),
    TestDefinition(
      RequiredClaim.ReadAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsAnyParty)),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.ReadAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsAnyParty)),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ReadAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsAnyParty)),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.ReadAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsParty(party))),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.ReadAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsParty(party))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ReadAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsParty(party))),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.ReadAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsParty(party2))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ReadAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsParty(party2))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ReadAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsParty(party2))),
      expectedPermissionDenied,
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for RequiredClaim.ExecuteAs"

  List(
    TestDefinition(
      RequiredClaim.ExecuteAs("party"),
      ClaimSet.Claims.Empty,
      expectedPermissionDenied,
    ),
    TestDefinition(RequiredClaim.ExecuteAs("party"), ClaimSet.Claims.Wildcard, ExpectedSuccess),
    TestDefinition(
      RequiredClaim.ExecuteAs("party"),
      ClaimSet.Claims.Admin,
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ExecuteAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsAnyParty)),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ExecuteAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsAnyParty)),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.ExecuteAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsAnyParty)),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.ExecuteAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsParty(party))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ExecuteAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsParty(party))),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.ExecuteAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsParty(party))),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.ExecuteAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsParty(party2))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ExecuteAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsParty(party2))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ExecuteAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsParty(party2))),
      expectedPermissionDenied,
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for RequiredClaim.DebitTrafficFrom accepting execute rights"

  List[(ClaimSet.Claims, ExpectedResult)](
    withClaims(ClaimDebitTrafficFromAccount(account)) -> ExpectedSuccess,
    withClaims(ClaimDebitTrafficFromAnyAccount) -> ExpectedSuccess,
    withClaims(ClaimExecuteAsParty(party)) -> ExpectedSuccess,
    ClaimSet.Claims.Wildcard -> ExpectedSuccess,
    withClaims(ClaimDebitTrafficFromAccount(account2)) -> expectedPermissionDenied,
    withClaims(ClaimReadAsParty(party)) -> expectedPermissionDenied,
    ClaimSet.Claims.Admin -> expectedPermissionDenied,
  ).foreach { case (claims, expected) =>
    generateAuthorizationTest(TestDefinition(debitAcceptingExecute, claims, expected))
  }

  behavior of s"$className.authorize for RequiredClaim.DebitTrafficFrom ignoring execute rights"

  List[(ClaimSet.Claims, ExpectedResult)](
    withClaims(ClaimDebitTrafficFromAccount(account)) -> ExpectedSuccess,
    withClaims(ClaimDebitTrafficFromAnyAccount) -> ExpectedSuccess,
    withClaims(
      ClaimActAsParty(party),
      ClaimExecuteAsParty(party),
      ClaimActAsAnyParty,
      ClaimExecuteAsAnyParty,
    ) -> expectedPermissionDenied,
  ).foreach { case (claims, expected) =>
    generateAuthorizationTest(TestDefinition(debitIgnoringExecute, claims, expected))
  }

  behavior of s"$className.authorize for RequiredClaim.ListTrafficAccounts accepting execute rights"

  List[(ClaimSet.Claims, ExpectedResult)](
    withClaims(ClaimDebitTrafficFromAnyAccount) -> ExpectedSuccess,
    withClaims(ClaimExecuteAsAnyParty) -> ExpectedSuccess,
    withClaims(ClaimActAsAnyParty) -> ExpectedSuccess,
    ClaimSet.Claims.Wildcard -> ExpectedSuccess,
    ClaimSet.Claims.Admin -> ExpectedSuccess,
    withClaims(ClaimApiArea(ClaimApiArea.trafficRead.area)) -> ExpectedSuccess,
    withClaims(
      ClaimDebitTrafficFromAccount(account),
      ClaimExecuteAsParty(party),
      ClaimActAsParty(party),
      ClaimReadAsAnyParty,
    ) -> expectedPermissionDenied,
    ClaimSet.Claims.Empty -> expectedPermissionDenied,
  ).foreach { case (claims, expected) =>
    generateAuthorizationTest(TestDefinition(listAcceptingExecute, claims, expected))
  }

  behavior of s"$className.authorize for RequiredClaim.ListTrafficAccounts ignoring execute rights"

  List[(ClaimSet.Claims, ExpectedResult)](
    withClaims(ClaimDebitTrafficFromAnyAccount) -> ExpectedSuccess,
    withClaims(
      ClaimDebitTrafficFromAccount(account),
      ClaimActAsParty(party),
      ClaimExecuteAsParty(party),
      ClaimActAsAnyParty,
      ClaimExecuteAsAnyParty,
    ) -> expectedPermissionDenied,
  ).foreach { case (claims, expected) =>
    generateAuthorizationTest(TestDefinition(listIgnoringExecute, claims, expected))
  }

  behavior of s"$className.authorize for RequiredClaim.ReadTrafficFrom"

  List[(Boolean, ClaimSet.Claims, ExpectedResult)](
    (true, withClaims(ClaimApiArea.trafficRead), ExpectedSuccess),
    (true, withClaims(ClaimApiArea.trafficWrite), ExpectedSuccess),
    (true, withClaims(ClaimApiArea.admin*), ExpectedSuccess),
    (
      true,
      withClaims(ClaimApiArea.trafficSelf, ClaimDebitTrafficFromAccount(account)),
      ExpectedSuccess,
    ),
    (true, withClaims(ClaimDebitTrafficFromAnyAccount), ExpectedSuccess),
    (true, withClaims(ClaimApiArea.trafficSelf, ClaimExecuteAsParty(party)), ExpectedSuccess),
    (
      false,
      withClaims(ClaimApiArea.trafficSelf, ClaimDebitTrafficFromAccount(account)),
      ExpectedSuccess,
    ),
    (false, withClaims(ClaimExecuteAsParty(party)), expectedPermissionDenied),
    (false, withClaims(ClaimActAsAnyParty), expectedPermissionDenied),
    (true, withClaims(ClaimDebitTrafficFromAccount(account2)), expectedPermissionDenied),
    (true, withClaims(ClaimReadAsParty(party)), expectedPermissionDenied),
    (true, withClaims(ClaimAdmin), expectedPermissionDenied),
    (true, withClaims(ClaimApiArea.user*), expectedPermissionDenied),
    (true, ClaimSet.Claims.Empty, expectedPermissionDenied),
  ).foreach { case (acceptExecuteRights, claims, expected) =>
    generateAuthorizationTest(
      TestDefinition(
        RequiredClaim.ReadTrafficFrom("party", acceptExecuteRights),
        claims,
        expected,
        descSuffix = s"with acceptExecuteRights=$acceptExecuteRights",
      )
    )
  }

  behavior of s"$className.authorize for RequiredClaim.PartiesReadOrSelfRead"

  List[(Seq[String], ClaimSet.Claims, ExpectedResult)](
    (Nil, withClaims(ClaimApiArea.partiesRead), ExpectedSuccess),
    (Nil, withClaims(ClaimApiArea.partiesWrite), ExpectedSuccess),
    (Seq("party", "party2"), withClaims(ClaimApiArea.partiesRead), ExpectedSuccess),
    (Seq("party"), ClaimSet.Claims.Admin, ExpectedSuccess),
    (Seq("party"), withClaims(ClaimApiArea.idpAdmin*), ExpectedSuccess),
    (Seq("party"), withClaims(ClaimApiArea.partiesSelf, ClaimReadAsParty(party)), ExpectedSuccess),
    (
      Seq("party"),
      withClaims(ClaimApiArea.partiesSelf, ClaimExecuteAsParty(party)),
      ExpectedSuccess,
    ),
    (Seq("party"), withClaims(ClaimApiArea.partiesSelf, ClaimActAsParty(party)), ExpectedSuccess),
    (
      Seq("party", "party2"),
      withClaims(ClaimApiArea.partiesSelf, ClaimReadAsAnyParty),
      ExpectedSuccess,
    ),
    (
      Seq("party", "party2"),
      withClaims(ClaimApiArea.partiesSelf, ClaimExecuteAsAnyParty),
      ExpectedSuccess,
    ),
    (
      Seq("party", "party2"),
      withClaims(ClaimApiArea.partiesSelf, ClaimActAsAnyParty),
      ExpectedSuccess,
    ),
    (
      Seq("party", "party2"),
      withClaims(ClaimApiArea.partiesSelf, ClaimReadAsParty(party), ClaimActAsParty(party2)),
      ExpectedSuccess,
    ),
    (Seq("party", "party2"), withClaims(ClaimReadAsParty(party)), expectedPermissionDenied),
    (Seq("party"), withClaims(ClaimReadAsParty(party2)), expectedPermissionDenied),
    // operating as parties does not grant reading all parties
    (Nil, withClaims(ClaimReadAsParty(party)), expectedPermissionDenied),
    (Nil, withClaims(ClaimActAsAnyParty), expectedPermissionDenied),
    (Nil, ClaimSet.Claims.Empty, expectedPermissionDenied),
    (Seq("party"), withClaims(ClaimAdmin), expectedPermissionDenied),
    (Seq("party"), withClaims(ClaimApiArea.user*), expectedPermissionDenied),
    (Seq("party"), withClaims(ClaimApiArea.partiesAllocate), expectedPermissionDenied),
  ).foreach { case (parties, claims, expected) =>
    generateAuthorizationTest(
      TestDefinition(
        RequiredClaim.PartiesReadOrSelfRead(parties),
        claims,
        expected,
        descSuffix = s"for parties [${parties.mkString(",")}]",
      )
    )
  }

  behavior of s"$className.authorize for RequiredClaim.ReadAsAnyParty"

  List(
    TestDefinition(RequiredClaim.ReadAsAnyParty(), ClaimSet.Claims.Empty, expectedPermissionDenied),
    TestDefinition(RequiredClaim.ReadAsAnyParty(), ClaimSet.Claims.Wildcard, ExpectedSuccess),
    TestDefinition(RequiredClaim.ReadAsAnyParty(), ClaimSet.Claims.Admin, expectedPermissionDenied),
    TestDefinition(
      RequiredClaim.ReadAsAnyParty(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsAnyParty)),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.ReadAsAnyParty(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsAnyParty)),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ReadAsAnyParty(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsAnyParty)),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.ReadAsAnyParty(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsParty(party))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ReadAsAnyParty(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsParty(party))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ReadAsAnyParty(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsParty(party))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ReadAsAnyParty(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsParty(party2))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ReadAsAnyParty(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsParty(party2))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ReadAsAnyParty(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsParty(party2))),
      expectedPermissionDenied,
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for RequiredClaim.ActAs"

  List(
    TestDefinition(RequiredClaim.ActAs("party"), ClaimSet.Claims.Empty, expectedPermissionDenied),
    TestDefinition(RequiredClaim.ActAs("party"), ClaimSet.Claims.Wildcard, ExpectedSuccess),
    TestDefinition(RequiredClaim.ActAs("party"), ClaimSet.Claims.Admin, expectedPermissionDenied),
    TestDefinition(
      RequiredClaim.ActAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsAnyParty)),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ActAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsAnyParty)),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ActAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsAnyParty)),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.ActAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsParty(party))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ActAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsParty(party))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ActAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsParty(party))),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.ActAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsParty(party2))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ActAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsParty(party2))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ActAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsParty(party2))),
      expectedPermissionDenied,
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for RequiredClaim.MatchReadAsParties"

  private val testPartiesL: Lens[TestPartyReq, Set[String]] =
    Lens[TestPartyReq, Set[String]](_.parties)((q, p) => q.copy(parties = p))
  private val testPartyReqRes: TestPartyReq => Future[Set[String]] = q =>
    Future.successful(q.parties)
  private val emptyPartyReq: TestPartyReq = TestPartyReq(Set.empty)

  it should "backfill Set.empty and succeed for ClaimReadAsAnyParty" in {
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsAnyParty))
    ) {
      authorizer().rpc(testPartyReqRes)(RequiredClaim.MatchReadAsParties(testPartiesL))(
        emptyPartyReq
      )
    }.map(_ shouldBe Set.empty[String])
  }

  it should "backfill Set.empty and succeed for ClaimActAsAnyParty" in {
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsAnyParty))
    ) {
      authorizer().rpc(testPartyReqRes)(RequiredClaim.MatchReadAsParties(testPartiesL))(
        emptyPartyReq
      )
    }.map(_ shouldBe Set.empty[String])
  }

  it should "backfill the party set and succeed for ClaimReadAsParty" in {
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsParty(party)))
    ) {
      authorizer().rpc(testPartyReqRes)(RequiredClaim.MatchReadAsParties(testPartiesL))(
        emptyPartyReq
      )
    }.map(_ shouldBe Set(party.toString))
  }

  it should "backfill the party set and succeed for ClaimActAsParty" in {
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsParty(party)))
    ) {
      authorizer().rpc(testPartyReqRes)(RequiredClaim.MatchReadAsParties(testPartiesL))(
        emptyPartyReq
      )
    }.map(_ shouldBe Set(party.toString))
  }

  it should "backfill the union of act-as and read-as parties" in {
    contextWithClaims(
      ClaimSet.Claims.Empty
        .copy(claims = Seq[Claim](ClaimActAsParty(party), ClaimReadAsParty(party2)))
    ) {
      authorizer().rpc(testPartyReqRes)(RequiredClaim.MatchReadAsParties(testPartiesL))(
        emptyPartyReq
      )
    }.map(_ shouldBe Set(party.toString, party2.toString))
  }

  it should "return permission denied for empty claims" in {
    contextWithClaims(ClaimSet.Claims.Empty) {
      authorizer().rpc(testPartyReqRes)(RequiredClaim.MatchReadAsParties(testPartiesL))(
        emptyPartyReq
      )
    }.transform(assertExpectedFailure(Status.PERMISSION_DENIED.getCode))
  }

  it should "return permission denied for ClaimAdmin without party read access" in {
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimAdmin))
    ) {
      authorizer().rpc(testPartyReqRes)(RequiredClaim.MatchReadAsParties(testPartiesL))(
        emptyPartyReq
      )
    }.transform(assertExpectedFailure(Status.PERMISSION_DENIED.getCode))
  }

  it should "return permission denied for ClaimExecuteAsParty without read access" in {
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsParty(party)))
    ) {
      authorizer().rpc(testPartyReqRes)(RequiredClaim.MatchReadAsParties(testPartiesL))(
        emptyPartyReq
      )
    }.transform(assertExpectedFailure(Status.PERMISSION_DENIED.getCode))
  }

  it should "return permission denied for ClaimExecuteAsAnyParty without read access" in {
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsAnyParty))
    ) {
      authorizer().rpc(testPartyReqRes)(RequiredClaim.MatchReadAsParties(testPartiesL))(
        emptyPartyReq
      )
    }.transform(assertExpectedFailure(Status.PERMISSION_DENIED.getCode))
  }

  val userId1 = "userId1"
  val userId2 = "userId2"

  def matchUserIdTestDef(
      claims: Seq[Claim],
      userId: Option[String],
      req: String,
      expectedResult: ExpectedResult,
      descSuffix: String,
      resolvedFromUser: Boolean = true,
  ): TestDefinition =
    TestDefinition(
      RequiredClaim.MatchUserIdForUserRead(simpleLens),
      ClaimSet.Claims.Empty.copy(
        claims = claims,
        userId = userId,
        resolvedFromUser = resolvedFromUser,
      ),
      expectedResult,
      req = req,
      descSuffix = descSuffix,
      resultAssert = _ shouldBe userId1,
    )

  behavior of s"$className.authorize for RequiredClaim.MatchUserIdForUserRead without Admin rights"
  List(
    matchUserIdTestDef(
      Seq(ClaimApiArea.usersSelf),
      Some(userId1),
      userId1,
      ExpectedSuccess,
      "when authorized and request user IDs match",
    ),
    matchUserIdTestDef(
      Seq(ClaimApiArea.usersSelf),
      Some(userId1),
      "",
      ExpectedSuccess,
      "when missing request user ID",
    ),
    matchUserIdTestDef(
      Seq(ClaimApiArea.usersSelf),
      Some(userId2),
      userId1,
      expectedPermissionDenied,
      "when authorized and request user IDs don't match",
    ),
    matchUserIdTestDef(
      Seq(ClaimApiArea.usersSelf),
      None,
      userId1,
      expectedInternal,
      "when undefined authorized user ID",
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for RequiredClaim.MatchUserIdForUserRead with Admin rights"
  List(
    matchUserIdTestDef(
      ClaimApiArea.admin,
      Some(userId1),
      userId1,
      ExpectedSuccess,
      "when authorized and request user IDs match",
    ),
    matchUserIdTestDef(
      ClaimApiArea.admin,
      Some(userId1),
      "",
      ExpectedSuccess,
      "when missing request user ID",
    ),
    matchUserIdTestDef(
      ClaimApiArea.admin,
      Some(userId2),
      userId1,
      ExpectedSuccess,
      "when authorized and request user IDs don't match",
    ),
    matchUserIdTestDef(
      ClaimApiArea.admin,
      None,
      userId1,
      expectedInternal,
      "when undefined authorized user ID",
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for RequiredClaim.MatchUserIdForUserRead with IDP Admin rights"
  List(
    matchUserIdTestDef(
      ClaimApiArea.idpAdmin,
      Some(userId1),
      userId1,
      ExpectedSuccess,
      "when authorized and request user IDs match",
    ),
    matchUserIdTestDef(
      ClaimApiArea.idpAdmin,
      Some(userId1),
      "",
      ExpectedSuccess,
      "when missing request user ID",
    ),
    matchUserIdTestDef(
      ClaimApiArea.idpAdmin,
      Some(userId2),
      userId1,
      ExpectedSuccess,
      "when authorized and request user IDs don't match",
    ),
    matchUserIdTestDef(
      ClaimApiArea.idpAdmin,
      None,
      userId1,
      expectedInternal,
      "when undefined authorized user ID",
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for RequiredClaim.MatchUserIdForUserRead with other claims"
  List(
    matchUserIdTestDef(
      Seq(ClaimAdmin),
      Some(userId2),
      userId1,
      expectedPermissionDenied,
      "when ClaimAdmin without users:read reads another user",
    ),
    matchUserIdTestDef(
      Seq(ClaimApiArea.usersSelf),
      Some(userId1),
      userId1,
      ExpectedSuccess,
      "when user with users:self reads itself",
    ),
    matchUserIdTestDef(
      Seq(ClaimApiArea.usersRead),
      Some(userId1),
      userId1,
      ExpectedSuccess,
      "when user with users:read reads itself",
    ),
    matchUserIdTestDef(
      Seq(ClaimApiArea.usersWrite),
      Some(userId2),
      userId1,
      ExpectedSuccess,
      "when users:write reads another user",
    ),
    matchUserIdTestDef(
      ClaimApiArea.user,
      Some(userId2),
      userId1,
      expectedPermissionDenied,
      "when user reads another user",
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for RequiredClaim.MatchUserIdForUserRead not resolvedFromUser"
  List(
    matchUserIdTestDef(
      Nil,
      Some(userId1),
      userId1,
      expectedPermissionDenied,
      "when authorized and request user IDs match",
      resolvedFromUser = false,
    ),
    matchUserIdTestDef(
      Nil,
      None,
      userId1,
      expectedPermissionDenied,
      "when undefined authenticated user ID",
      resolvedFromUser = false,
    ),
    matchUserIdTestDef(
      Nil,
      Some(userId2),
      userId1,
      expectedPermissionDenied,
      "when authorized and request user IDs don't match",
      resolvedFromUser = false,
    ),
    matchUserIdTestDef(
      Seq(ClaimApiArea.usersRead),
      Some(userId1),
      userId1,
      ExpectedSuccess,
      "when authorized and request user IDs match",
      resolvedFromUser = false,
    ),
    matchUserIdTestDef(
      Seq(ClaimApiArea.usersRead),
      None,
      userId1,
      ExpectedSuccess,
      "when undefined authenticated user ID",
      resolvedFromUser = false,
    ),
    matchUserIdTestDef(
      Seq(ClaimApiArea.usersRead),
      Some(userId2),
      userId1,
      ExpectedSuccess,
      "when authorized and request user IDs don't match",
      resolvedFromUser = false,
    ),
  ).foreach(generateAuthorizationTest)

  def partiesWriteOrSelfAllocateTestDef(
      claims: Seq[Claim],
      userId: Option[String],
      req: String,
      expectedResult: ExpectedResult,
      descSuffix: String,
      resolvedFromUser: Boolean = true,
  ): TestDefinition =
    TestDefinition(
      RequiredClaim.PartiesWriteOrSelfAllocate(simpleLens),
      ClaimSet.Claims.Empty.copy(
        claims = claims,
        userId = userId,
        resolvedFromUser = resolvedFromUser,
      ),
      expectedResult,
      req = req,
      descSuffix = descSuffix,
    )

  behavior of s"$className.authorize for PartiesWriteOrSelfAllocate with Admin claims"
  List(
    partiesWriteOrSelfAllocateTestDef(
      ClaimApiArea.admin,
      Some(userId2),
      userId2,
      ExpectedSuccess,
      "when authorized and request user IDs match",
    ),
    partiesWriteOrSelfAllocateTestDef(
      ClaimApiArea.admin,
      Some(userId1),
      userId2,
      ExpectedSuccess,
      "when authorized and request user IDs don't match",
    ),
    partiesWriteOrSelfAllocateTestDef(
      ClaimApiArea.admin,
      Some(userId1),
      "",
      ExpectedSuccess,
      "when request user ID missing",
    ),
    partiesWriteOrSelfAllocateTestDef(
      ClaimApiArea.admin,
      None,
      userId2,
      ExpectedSuccess,
      "when authorized user ID missing",
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for PartiesWriteOrSelfAllocate with Idp Admin claims"
  List(
    partiesWriteOrSelfAllocateTestDef(
      Seq(ClaimApiArea.partiesWrite),
      Some(userId2),
      userId2,
      ExpectedSuccess,
      "when authorized and request user IDs match",
    ),
    partiesWriteOrSelfAllocateTestDef(
      Seq(ClaimApiArea.partiesWrite),
      Some(userId1),
      userId2,
      ExpectedSuccess,
      "when authorized and request user IDs don't match",
    ),
    partiesWriteOrSelfAllocateTestDef(
      Seq(ClaimApiArea.partiesWrite),
      Some(userId1),
      "",
      ExpectedSuccess,
      "when request user ID missing",
    ),
    partiesWriteOrSelfAllocateTestDef(
      Seq(ClaimApiArea.partiesWrite),
      None,
      userId2,
      ExpectedSuccess,
      "when authorized user ID missing",
    ),
  )
    .foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for PartiesWriteOrSelfAllocate without Admin claims"
  List(
    partiesWriteOrSelfAllocateTestDef(
      Seq(ClaimApiArea.partiesAllocate),
      Some(userId2),
      userId2,
      ExpectedSuccess,
      "when authorized and request user IDs match",
    ),
    partiesWriteOrSelfAllocateTestDef(
      Seq(ClaimApiArea.partiesAllocate),
      Some(userId1),
      userId2,
      expectedPermissionDenied,
      "when authorized and request user IDs don't match",
    ),
    partiesWriteOrSelfAllocateTestDef(
      Seq(ClaimApiArea.partiesAllocate),
      Some(userId1),
      "",
      expectedPermissionDenied,
      "when request user ID missing",
    ),
    partiesWriteOrSelfAllocateTestDef(
      Nil,
      None,
      userId2,
      expectedInternal,
      "when authorized user ID missing",
    ),
    partiesWriteOrSelfAllocateTestDef(
      Nil,
      Some(userId2),
      userId2,
      expectedPermissionDenied,
      "when missing parties:allocate claim",
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for PartiesWriteOrSelfAllocate with Admin claims when not resolvedFromUser"
  List(
    partiesWriteOrSelfAllocateTestDef(
      ClaimApiArea.admin,
      Some(userId2),
      userId2,
      ExpectedSuccess,
      "when authorized and request user IDs match",
      resolvedFromUser = false,
    ),
    partiesWriteOrSelfAllocateTestDef(
      ClaimApiArea.admin,
      Some(userId1),
      userId2,
      ExpectedSuccess,
      "when authorized and request user IDs don't match",
      resolvedFromUser = false,
    ),
    partiesWriteOrSelfAllocateTestDef(
      ClaimApiArea.admin,
      Some(userId1),
      "",
      ExpectedSuccess,
      "when request user ID missing",
      resolvedFromUser = false,
    ),
    partiesWriteOrSelfAllocateTestDef(
      ClaimApiArea.admin,
      None,
      userId2,
      ExpectedSuccess,
      "when authorized user ID missing",
      resolvedFromUser = false,
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for PartiesWriteOrSelfAllocate with parties:write claims when not resolvedFromUser"
  List(
    partiesWriteOrSelfAllocateTestDef(
      Seq(ClaimApiArea.partiesWrite),
      Some(userId2),
      userId2,
      ExpectedSuccess,
      "when authorized and request user IDs match",
      resolvedFromUser = false,
    ),
    partiesWriteOrSelfAllocateTestDef(
      Seq(ClaimApiArea.partiesWrite),
      Some(userId1),
      userId2,
      ExpectedSuccess,
      "when authorized and request user IDs don't match",
      resolvedFromUser = false,
    ),
    partiesWriteOrSelfAllocateTestDef(
      Seq(ClaimApiArea.partiesWrite),
      Some(userId1),
      "",
      ExpectedSuccess,
      "when request user ID missing",
      resolvedFromUser = false,
    ),
    partiesWriteOrSelfAllocateTestDef(
      Seq(ClaimApiArea.partiesWrite),
      None,
      userId2,
      ExpectedSuccess,
      "when authorized user ID missing",
      resolvedFromUser = false,
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for PartiesWriteOrSelfAllocate without Admin claims when not resolvedFromUser"
  List(
    partiesWriteOrSelfAllocateTestDef(
      Nil,
      Some(userId2),
      userId2,
      expectedPermissionDenied,
      "when authorized and request user IDs match",
      resolvedFromUser = false,
    ),
    partiesWriteOrSelfAllocateTestDef(
      Nil,
      Some(userId1),
      userId2,
      expectedPermissionDenied,
      "when authorized and request user IDs don't match",
      resolvedFromUser = false,
    ),
    partiesWriteOrSelfAllocateTestDef(
      Nil,
      Some(userId1),
      "",
      expectedPermissionDenied,
      "when request user ID missing",
      resolvedFromUser = false,
    ),
    partiesWriteOrSelfAllocateTestDef(
      Nil,
      None,
      userId2,
      expectedPermissionDenied,
      "when authorized user ID missing",
      resolvedFromUser = false,
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for PartiesWriteOrSelfAllocate with other claims"
  List(
    partiesWriteOrSelfAllocateTestDef(
      Seq(ClaimAdmin),
      Some(userId1),
      userId2,
      expectedPermissionDenied,
      "when ClaimAdmin without parties:write allocates for another user",
    ),
    partiesWriteOrSelfAllocateTestDef(
      Seq(ClaimAdmin),
      None,
      userId2,
      expectedPermissionDenied,
      "when ClaimAdmin without parties:write is not a user",
      resolvedFromUser = false,
    ),
    partiesWriteOrSelfAllocateTestDef(
      Seq(ClaimApiArea.partiesRead, ClaimApiArea.partiesAllocate),
      Some(userId1),
      userId2,
      expectedPermissionDenied,
      "when parties:read and parties:allocate allocate for another user",
    ),
    partiesWriteOrSelfAllocateTestDef(
      ClaimApiArea.user,
      Some(userId2),
      userId2,
      ExpectedSuccess,
      "when user API areas allocate for itself",
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for RequiredClaim.MatchUserId"

  it should "authorize for authenticated user ID matching request user ID" in {
    val userIdL = Lens[Long, String](_.toString)((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Nil,
        userId = Some(dummyRequest.toString),
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.MatchUserId(userIdL))(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "authorize for authenticated user ID when request user ID is missing" in {
    val userIdL = Lens[Long, String](l => if (l == 0) "" else l.toString)((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Nil,
        userId = Some(dummyRequest.toString),
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.MatchUserId(userIdL))(0)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "return permission denied for authenticated user ID not matching request user ID" in {
    val userIdL = Lens[Long, String](_.toString)((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Nil,
        userId = Some("15"),
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.MatchUserId(userIdL))(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  it should "return permission denied for authenticated user ID not matching request user ID if skipUserIdValidationForAnyPartyReaders" in {
    val userIdL = Lens[Long, String](_.toString)((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Nil,
        userId = Some("15"),
      )
    ) {
      authorizer().rpc(dummyReqRes)(
        RequiredClaim.MatchUserId(
          userIdL,
          skipUserIdValidationForAnyPartyReaders = true,
        )
      )(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  it should "authorize for authenticated user ID not matching request user ID for AnyPartyReaders-s if skipUserIdValidationForAnyPartyReaders" in {
    val userIdL = Lens[Long, String](_.toString)((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Seq(ClaimReadAsAnyParty),
        userId = Some("15"),
      )
    ) {
      authorizer().rpc(dummyReqRes)(
        RequiredClaim.MatchUserId(
          userIdL,
          skipUserIdValidationForAnyPartyReaders = true,
        )
      )(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "return invalid argument for no authenticated user ID, and no request user ID" in {
    val userIdL = Lens[Long, String](l => if (l == 0) "" else l.toString)((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Nil,
        userId = None,
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.MatchUserId(userIdL))(0)
    }
      .transform(
        assertExpectedFailure(Status.INVALID_ARGUMENT.getCode)
      )
  }

  behavior of s"$className.authorize for RequiredClaim.MatchIdentityProviderId"

  it should "authorize for identity provider ID in claims matching provided identity provider ID" in {
    val idpIdL = Lens[Long, String](_.toString)((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Nil,
        identityProviderId = Some(LfLedgerString.assertFromString(dummyRequest.toString)),
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.MatchIdentityProviderId(idpIdL))(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "return invalid argument for malformed provided identity provider ID" in {
    val idpIdL = Lens[Long, String](_ => "!@#$%^%&^^&*&*)")((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Nil,
        identityProviderId = Some(LfLedgerString.assertFromString(dummyRequest.toString)),
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.MatchIdentityProviderId(idpIdL))(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.INVALID_ARGUMENT.getCode)
      )
  }

  it should "authorize for identity provider ID in claims with no identity provider ID provided in the request" in {
    val idpIdL = Lens[Long, String](l => if (l == 0) "" else l.toString)((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Nil,
        identityProviderId = Some(LfLedgerString.assertFromString(dummyRequest.toString)),
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.MatchIdentityProviderId(idpIdL))(0)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "return permission denied for identity provider ID in claims with no identity provider ID provided in the request for Admins (for admins no auto-resolution from claims)" in {
    val idpIdL = Lens[Long, String](l => if (l == 0) "" else l.toString)((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Seq(ClaimAdmin),
        identityProviderId = Some(LfLedgerString.assertFromString(dummyRequest.toString)),
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.MatchIdentityProviderId(idpIdL))(0)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  it should "authorize for no identity provider ID in claims with no identity provider ID provided in the request (resolving to the default provider)" in {
    val idpIdL = Lens[Long, String](l => if (l == 0) "" else l.toString)((_, s) =>
      if (s == "") dummyRequest else s.toLong
    )
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Nil,
        identityProviderId = None,
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.MatchIdentityProviderId(idpIdL))(0)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  private def assertExpectedFailure[T](
      expectedStatusCode: Status.Code
  ): Try[T] => Try[Assertion] = {
    case Failure(ex: StatusRuntimeException) =>
      ex.getStatus.getCode shouldBe expectedStatusCode
      Success(succeed)
    case ex => fail(s"Expected a failure with StatusRuntimeException but got $ex")
  }

  private def contextWithClaims[R](claims: ClaimSet.Claims)(f: => R): R =
    io.grpc.Context.ROOT
      .withValue(AuthInterceptor.contextKeyClaimSet, claims)
      .call(() => f)

  private def authorizer() = new Authorizer(
    () => Instant.ofEpochSecond(1337L),
    "participant-id",
    loggerFactory = loggerFactory,
  )

  def generateAuthorizationTest(td: TestDefinition): Unit = {
    val prettyClaims = s"Claims(${td.suppliedClaim.claims.map(_.toString).mkString(",")})"
    val testDescription = td.expectedResult match {
      case ExpectedSuccess =>
        s"authorize for $prettyClaims ${td.descSuffix}"
      case ExpectedFailure(code) =>
        s"return $code for $prettyClaims ${td.descSuffix}"
    }
    it should testDescription in {
      contextWithClaims(td.suppliedClaim) {
        authorizer().rpc(simpleReqRes)(td.requiredClaim)(td.req)
      }.map(td.resultAssert).transform {
        case Success(_) =>
          td.expectedResult match {
            case ExpectedSuccess =>
              Success(succeed)
            case ExpectedFailure(_) =>
              fail("Unexpected success")
          }
        case Failure(ex: StatusRuntimeException) =>
          td.expectedResult match {
            case ExpectedSuccess =>
              fail(s"Unexpected error ${ex.getStatus.getCode}")
            case ExpectedFailure(code) if code != ex.getStatus.getCode =>
              fail(s"Unexpected error ${ex.getStatus.getCode}")
            case _ => Success(succeed)
          }
        case ex => fail(s"Unexpected error $ex")
      }
    }
  }
}

object AuthorizerSpec {
  sealed trait ExpectedResult
  final case object ExpectedSuccess extends ExpectedResult
  final case class ExpectedFailure(code: Status.Code) extends ExpectedResult

  final case class TestPartyReq(parties: Set[String])

  val expectedPermissionDenied: ExpectedFailure = ExpectedFailure(Status.PERMISSION_DENIED.getCode)
  val expectedInternal: ExpectedFailure = ExpectedFailure(Status.INTERNAL.getCode)

  val simpleRequest: String = "simpleRequest"
  val simpleReqRes: String => Future[String] = s => Future.successful(s)
  val simpleLens: Lens[String, String] = Lens[String, String](s => s)((_, s) => s)

  final case class TestDefinition(
      requiredClaim: RequiredClaim[String],
      suppliedClaim: ClaimSet.Claims,
      expectedResult: ExpectedResult,
      req: String = simpleRequest,
      descSuffix: String = "",
      resultAssert: String => Assertion = _ => succeed,
  )
}
