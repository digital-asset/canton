// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.daml.ledger.api.v2.commands.Command
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.damltests.java.maptest.UsesMap
import com.digitalasset.canton.data.*
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.util.TestSubmissionService.CommandsWithMetadata
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.LocalRejectError.MalformedRejects.ModelConformance
import com.digitalasset.canton.protocol.{ContractInstance, InputContract, LfContractId}
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.util.MaliciousParticipantNode
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.transaction.FatContractInstance
import com.digitalasset.daml.lf.value.Value.ValueRecord
import io.grpc.Status.Code
import monocle.macros.GenLens
import monocle.macros.syntax.lens.*

import java.util
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.CollectionHasAsScala

/** Tests workaround of contract id authentication issue on AuthenticatedContractIdVersionV11. For
  * more background, see #32765.
  */
sealed trait ContractIdV11AuthenticationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with HasCycleUtils
    with SecurityTestHelpers {

  private lazy val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference()
  override def pureCrypto: CryptoPureApi = pureCryptoRef.get()
  private var maliciousParticipant: Map[ParticipantReference, MaliciousParticipantNode] = Map.empty

  private var contractByParticipant: Map[ParticipantReference, (PartyId, UsesMap.Contract)] =
    Map.empty

  override protected def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      // Enable contract id V11 so that the problems can actually be observed.
      .updateTestingConfig(_.focus(_.useLegacyContractIdVersionV11).replace(true))
      .addConfigTransforms(
        // Disable the workaround on participant2.
        ConfigTransforms.updateParticipantConfig("participant2")(
          _.focus(_.parameters.validateLegacyContractsV11).replace(false)
        )
      )
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, daName)
        participants.all.dars.upload(CantonTestsPath)

        val nonEmptyMap: util.HashMap[java.lang.Long, java.lang.Long] = new util.HashMap()
        nonEmptyMap.put(0, 42)

        participants.all.foreach { participant =>
          val party = participant.adminParty
          val cmd =
            // Note the first map is empty.
            new UsesMap(party.toProtoPrimitive, new util.HashMap(), nonEmptyMap)
              .create()
              .commands()
              .asScala
              .toSeq

          val contract = JavaDecodeUtil
            .decodeAllCreated(UsesMap.COMPANION)(
              participant.ledger_api.javaapi.commands.submit(Seq(party), cmd)
            )
            .loneElement

          contractByParticipant = contractByParticipant.updated(participant, party -> contract)
        }

        maliciousParticipant = participants.local
          .map(p =>
            p -> MaliciousParticipantNode(
              p,
              daId,
              testedProtocolVersion,
              timeouts,
              loggerFactory,
            )
          )
          .toMap

        pureCryptoRef.set(sequencer1.crypto.pureCrypto)
      }

  "A participant" when {
    "contract id V11 is used" can {
      "use contracts with GenMap" in { implicit env =>
        import env.*

        forEvery(participants.all) { participant =>
          val (party, contract) = contractByParticipant(participant)
          participant.ledger_api.javaapi.commands
            .submit(Seq(party), contract.id.exerciseTouch().commands().asScala.toSeq)
        }
      }

      "observe tampering of contract data if validation is disabled" in { implicit env =>
        import env.*

        val events = touchWithTampering(participant2)
        events.assertStatusOk(participant2)
      }

      "remediate tampering of contract data if validation is enabled" in { implicit env =>
        import env.*

        val events = loggerFactory.assertLoggedWarningsAndErrorsSeq(
          touchWithTampering(participant1),
          LogEntry.assertLogSeq(
            Seq(
              (
                _.shouldBeCantonError(
                  SyncServiceAlarm,
                  _ should fullyMatch regex raw"Contract data mismatch: The input contract in the request \(ContractId\(\S+\)\) does not match the version in the contract store\. Rejecting request\.",
                ),
                "Contract data mismatch",
              ),
              (
                _.shouldBeCantonError(
                  ModelConformance,
                  _ should startWith(
                    "Rejected transaction due to a failed model conformance check: Contract data mismatch:"
                  ),
                ),
                "Model conformance error",
              ),
            )
          ),
        )

        events
          .assertExactlyOneCompletion(participant1)
          .status
          .value
          .code shouldBe Code.INVALID_ARGUMENT.value()
      }
    }
  }

  private def touchWithTampering(
      participant: LocalParticipantReference
  )(implicit executionContext: ExecutionContext): TrackingResult = {
    val (party, contract) = contractByParticipant(participant)

    val cmd = CommandsWithMetadata(
      contract.id
        .exerciseTouch()
        .commands()
        .asScala
        .toSeq
        .map(c => Command.fromJavaProto(c.toProtoCommand)),
      Seq(party),
    )

    val cid = LfContractId.assertFromString(contract.id.contractId)

    val (_, events) =
      trackingLedgerEvents(Seq(participant), Seq.empty)(
        maliciousParticipant(participant)
          .submitCommand(
            cmd,
            // Flip myMap1 and myMap2 in the contract data of the input contract.
            transactionTreeInterceptor = GenTransactionTree.rootViewsUnsafe
              .andThen(
                MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion)
              )
              .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
              .andThen(TransactionView.Optics.viewParticipantDataUnsafe)
              .andThen(MerkleTree.Optics.unblinded[ViewParticipantData])
              .andThen(ViewParticipantData.Optics.coreInputsUnsafe)
              .at(cid)
              .some
              .andThen(GenLens[InputContract](_.contract))
              .andThen(ContractInstance.Optics.instUnsafe)
              .andThen(FatContractInstance.Optics.createArgUnsafe)
              .andThen(GenLens[ValueRecord](_.fields))
              .modify(_.toSeq match {
                case Seq(k0 -> v0, k1 -> v1, k2 -> v2) =>
                  ImmArray(k0 -> v0, k1 -> v2, k2 -> v1)
                case fields => fail(s"Expected exactly three fields, but encountered $fields")
              }),
          )
          .futureValueUS
          .valueOrFail("Submission failed")
      )

    events
  }
}

final class ContractIdV11AuthenticationIntegrationTestPostgres
    extends ContractIdV11AuthenticationIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
