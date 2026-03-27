// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrading

import com.daml.ledger.api.v2.transaction_filter
import com.daml.ledger.javaapi.data.Identifier
import com.daml.ledger.javaapi.data.codegen.UnknownTrailingFieldPolicy
import com.daml.ledger.javaapi.data.codegen.json.JsonLfReader
import com.digitalasset.canton.admin.api.client.data.TemplateId
import com.digitalasset.canton.damltests.upgrade.v1.java as v1
import com.digitalasset.canton.damltests.upgrade.v1.java.upgrade.Upgrading
import com.digitalasset.canton.damltests.upgrade.v2.java as v2
import com.digitalasset.canton.http.json.v2.JsCommandServiceCodecs.*
import com.digitalasset.canton.http.json.v2.JsContractEntry.JsActiveContract
import com.digitalasset.canton.http.json.v2.JsSchema.JsServicesCommonCodecs.*
import com.digitalasset.canton.http.json.v2.JsStateServiceCodecs.*
import com.digitalasset.canton.http.json.v2.{JsCommand, JsCommands, JsGetActiveContractsResponse}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.jsonapi.AbstractHttpServiceIntegrationTestFuns.HttpServiceTestFixtureData
import com.digitalasset.canton.integration.tests.jsonapi.{HttpServiceUserFixture, HttpTestFuns}
import com.digitalasset.canton.integration.tests.upgrading.UpgradingBaseTest.{UpgradeV1, UpgradeV2}
import com.digitalasset.canton.integration.{EnvironmentDefinition, TestConsoleEnvironment}
import com.digitalasset.canton.topology.PartyId
import io.circe.parser.decode
import io.circe.syntax.*
import io.circe.{Json, parser}
import org.apache.pekko.http.scaladsl.model.Uri

import java.util.UUID

class ValueDecoderUpgradingIntegrationTest
    extends HttpTestFuns
    with HttpServiceUserFixture.UserToken {

  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  private def party(name: String)(implicit env: TestConsoleEnvironment): PartyId =
    env.participant1.parties.list(name).headOption.valueOrFail("where is " + name).party

  override lazy val environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.withSetup { implicit env =>
      import env.*
      participant1.parties.enable("alice")
      participant1.dars.upload(UpgradeV1)
      participant1.dars.upload(UpgradeV2)
    }

  "Client" should {
    "be able to decode Upgrade V2 JSON payload into V1 with policy Ignore" in httpTestFixture {
      fixture =>
        implicit val env: TestConsoleEnvironment = provideEnvironment
        val alice = party("alice")

        val issuer = alice.toLf
        val owner = alice.toLf
        val field = 1337
        val more = "extra data"
        for {
          endOffset <- sendCreateContract(fixture = fixture)(
            targetTemplateId = v2.upgrade.Upgrading.TEMPLATE_ID_WITH_PACKAGE_ID,
            alice = alice,
          )(
            jsonDamlValue =
              s"""{"issuer":"$issuer", "owner":"$owner", "field": "$field", "more": ["$more"] }"""
          )
          response <- readAcs(fixture)(endOffset)
        } yield {
          val contracts = response.valueOrFail("response should be present")
          val contract =
            inside(contracts.loneElement.contractEntry) {
              case JsActiveContract(createdEvent, _, _) =>
                createdEvent.createArgument.valueOrFail("should be able to get create argument")
            }
          val contractJson = contract.noSpaces
          val decodedContract = v1.upgrade.Upgrading
            .jsonDecoder()
            .decode(new JsonLfReader(contractJson), UnknownTrailingFieldPolicy.IGNORE)
          decodedContract shouldBe new Upgrading(issuer, owner, field) withClue (
            "Decoded contract should match submitted contract (ignoring extra V2 fields)",
          )
          intercept[Exception](
            v1.upgrade.Upgrading
              .jsonDecoder()
              .decode(new JsonLfReader(contractJson), UnknownTrailingFieldPolicy.STRICT)
          ).getMessage shouldBe "Unknown field more (known fields are [issuer, owner, field]) at line: 1, column: 190"
        }

    }

    def sendCreateContract(fixture: HttpServiceTestFixtureData)(
        targetTemplateId: Identifier,
        alice: PartyId,
    )(jsonDamlValue: String) =
      fixture.headersWithAuth.flatMap { headers =>
        val jsCommand = JsCommand.CreateCommand(
          templateId = TemplateId.fromJavaIdentifier(targetTemplateId).toIdentifier,
          createArguments = parser.parse(jsonDamlValue).valueOrFail("unparseable test data"),
        )

        val cmds = JsCommands(
          commands = Seq(jsCommand),
          userId = Some(s"CantonConsole"),
          commandId = s"commandid_${UUID.randomUUID()}",
          actAs = Seq(alice.toLf),
        )
        for {
          _ <- postJsonRequest(
            uri = fixture.uri.withPath(Uri.Path("/v2/commands/submit-and-wait")),
            json = cmds.asJson,
            headers = headers,
          )
          endOffset <- fixture.client.stateService.getLedgerEndOffset()
        } yield endOffset
      }

    def readAcs(fixture: HttpServiceTestFixtureData)(endOffset: Long) =
      for {
        headers <- fixture.headersWithAuth
        (_, responseBody) <- postJsonRequest(
          uri = fixture.uri.withPath(Uri.Path("/v2/state/active-contracts")),
          Json.obj(
            "eventFormat" -> Some(
              transaction_filter.EventFormat(
                filtersByParty = Map.empty,
                filtersForAnyParty = Some(transaction_filter.Filters(cumulative = Seq())),
                verbose = false,
              )
            ).asJson,
            "activeAtOffset" -> endOffset.asJson,
          ),
          headers = headers,
        )
      } yield decode[Seq[JsGetActiveContractsResponse]](responseBody.toString())
  }

}
