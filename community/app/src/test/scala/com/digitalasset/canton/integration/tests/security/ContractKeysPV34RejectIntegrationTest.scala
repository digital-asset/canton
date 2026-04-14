// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.damltestslf23.java.basickeys.BasicKey
import com.digitalasset.canton.damltestslf23.java.da.set.types
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.VettedPackage
import com.digitalasset.canton.util.SetupPackageVetting
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.language.LanguageVersion

import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsJava}

class ContractKeysPV34RejectIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {
  @volatile private var alice, bob: PartyId = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.setAlphaVersionSupport(true)*
      )
      .withSetup { implicit env =>
        import env.*

        participants.all.foreach(_.synchronizers.connect_local(sequencer1, daName))

        alice = participant1.parties.enable("alice")
        bob = participant2.parties.enable("bob")

        val vettedPackage =
          VettedPackage(LfPackageId.assertFromString(BasicKey.PACKAGE_ID), None, None)

        SetupPackageVetting(
          Set(CantonTestsLF23Path),
          targetTopology = Map(
            daId -> Map(
              participant1 -> Set(vettedPackage),
              participant2 -> Set(vettedPackage),
            )
          ),
        )
      }

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  s"Submitting commands for contracts with keys using LF ${LanguageVersion.v2_3}" should {
    s"be rejected by the submitting participant on protocol version ${ProtocolVersion.v34}" onlyRunWith ProtocolVersion.v34 in {
      implicit env =>
        import env.*

        val owners = new types.Set(
          Map(alice.toProtoPrimitive -> com.daml.ledger.javaapi.data.Unit.getInstance()).asJava
        )
        val basicKeysCreate = new BasicKey(
          owners,
          new types.Set(
            Map(bob.toProtoPrimitive -> com.daml.ledger.javaapi.data.Unit.getInstance()).asJava
          ),
        )

        // Submitting a create command for a contract with keys should fail on PV 3.4,
        // because LF > 2.2 contract keys are not supported on protocol version 3.4.
        assertThrowsAndLogsCommandFailures(
          participant1.ledger_api.javaapi.commands
            .submit(Seq(alice), basicKeysCreate.create().commands().asScala.toList),
          _.commandFailureMessage should include(
            CommandExecutionErrors.Package.AllowedLanguageVersions.id
          ),
        )

        // Double check that the failed submission above did not commit the contract.
        assertThrowsAndLogsCommandFailures(
          participant1.ledger_api.javaapi.commands
            .submit(Seq(alice), BasicKey.byKey(owners).exerciseArchive().commands().asScala.toList),
          _.commandFailureMessage should include(
            CommandExecutionErrors.Interpreter.LookupErrors.ContractKeyNotFound.id
          ),
        )
    }

    // TODO(#31902): Consider adding a malicious participant test that submits confirmation requests with keys
    //               via a PV34 synchronizer.
  }
}
