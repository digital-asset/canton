// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.util

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.EngineController.EngineAbortStatus
import com.digitalasset.canton.participant.store.ReplayContractLookup
import com.digitalasset.canton.protocol.ExampleContractFactory
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ContractValidator
import com.digitalasset.canton.util.PackageConsumer.PackageResolver
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext, LfCommand}
import com.digitalasset.daml.lf.command.ReplayCommand
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.{Identifier, PackageId, PackageName, QualifiedName}
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref}
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.daml.lf.interpretation.InterpretationConfig
import com.digitalasset.daml.lf.language.Ast.Package
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.{
  CreationTime,
  ExternalCallResult,
  Node,
  SerializationVersion,
}
import com.digitalasset.daml.lf.value.Value
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class DAMLeExternalCallTest
    extends AnyWordSpec
    with Matchers
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown
    with MockitoSugar {

  private implicit val parserParameters: ParserParameters[Nothing] = ParserParameters(
    defaultPackageId = PackageId.assertFromString("-external-call-test-"),
    languageVersion = LanguageVersion.v2_dev,
  )

  private val packageId = parserParameters.defaultPackageId
  private val pkg: Package = p"""
    metadata ( 'external-call-test' : '1.0.0' )

    module M {
      record @serializable T = { party: Party };

      template (this: T) = {
        precondition True;
        signatories Cons @Party [M:T {party} this] (Nil @Party);
        observers Nil @Party;

        choice Call (self) (arg: Unit) : Text,
          controllers Cons @Party [M:T {party} this] (Nil @Party)
          to EXTERNAL_CALL "ext" "fun" "0a0b" "c0ff";
      };
    }
  """
  private val packageName = PackageName.assertFromString("external-call-test")
  private val templateId = Identifier(packageId, QualifiedName.assertFromString("M:T"))
  private val alice = Ref.Party.assertFromString("Alice")
  private val ledgerTime = CantonTimestamp.Epoch
  private val externalCallResult = ExternalCallResult(
    extensionId = "ext",
    functionId = "fun",
    config = Bytes.assertFromString("0a0b"),
    input = Bytes.assertFromString("c0ff"),
    output = Bytes.assertFromString("beef"),
  )
  private val defaultReplayData =
    DAMLe.ExternalCallReplayData.fromResults(Seq(externalCallResult))
  private val externalCallKey = DAMLe.ExternalCallKey.fromResult(externalCallResult)

  private val packageResolver = new PackageResolver {
    override protected def resolveInternal(packageId: PackageId)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Option[Package]] =
      FutureUnlessShutdown.pure(Option.when(packageId == DAMLeExternalCallTest.this.packageId)(pkg))
  }

  private val contract = {
    val create = Node.Create(
      coid = Value.ContractId.V1(Hash.hashPrivateKey("damle-external-call-test")),
      packageName = packageName,
      templateId = templateId,
      arg = Value.ValueRecord(None, ImmArray(None -> Value.ValueParty(alice))),
      signatories = Set(alice),
      stakeholders = Set(alice),
      keyOpt = None,
      version = SerializationVersion.VDev,
    )
    ExampleContractFactory.fromCreate(
      create,
      createdAt = CreationTime.CreatedAt(ledgerTime.toLf),
    )
  }
  private val command: LfCommand = ReplayCommand.Exercise(
    templateId = templateId,
    interfaceId = None,
    contractId = contract.contractId,
    choiceId = Ref.ChoiceName.assertFromString("Call"),
    argument = Value.ValueUnit,
  )
  private val contracts = new ReplayContractLookup(Map(contract.contractId -> contract), Map.empty)

  private def runReinterpret(
      replayData: DAMLe.ExternalCallReplayData =
        defaultReplayData
  ): Either[DAMLe.ReinterpretationError, DAMLe.ReInterpretationResult] = {
    val damle = new DAMLe(
      participantId = DefaultTestIdentities.participant1,
      resolvePackage = packageResolver,
      engine = new Engine(Engine.DevConfig, loggerFactory),
      interpretationConfig = InterpretationConfig.Dev,
      loggerFactory = loggerFactory,
    )

    val result = damle
      .reinterpret(
        contracts = contracts,
        contractAuthenticator = ContractValidator.AllowAll.authenticateHash,
        submitters = Set(alice),
        command = command,
        topologySnapshot = mock[TopologySnapshot],
        ledgerTime = ledgerTime,
        preparationTime = ledgerTime,
        rootSeed = Some(Hash.hashPrivateKey("damle-external-call-root-seed")),
        packageResolution = Map(packageName -> packageId),
        expectFailure = false,
        getEngineAbortStatus = () => EngineAbortStatus.notAborted,
        externalCallReplayData = () =>
          FutureUnlessShutdown.pure(replayData),
      )
      .value

    timeouts.default.awaitUS("reinterpret external call")(result).failOnShutdown
  }

  "DAMLe external-call replay" should {
    "reject external calls without recorded output" in {
      inside(
        runReinterpret(
          replayData = DAMLe.ExternalCallReplayData.empty
        )
      ) { case Left(error: DAMLe.ExternalCallReplayMissing) =>
        error.key shouldBe externalCallKey
      }
    }

    "replay stored external-call results" in {
      inside(runReinterpret()) { case Right(result) =>
        val exerciseNodes = result.transaction.nodes.collect { case (_, exercise: Node.Exercise) =>
          exercise
        }
        exerciseNodes should have size 1
        exerciseNodes.head.externalCallResults shouldBe ImmArray(externalCallResult)
      }
    }

    "reject conflicting semantic outputs without leaking their payloads" in {
      val conflictingOutput = externalCallResult.copy(output = Bytes.assertFromString("cafe"))
      val conflictingReplayData =
        DAMLe.ExternalCallReplayData.fromResults(Seq(externalCallResult, conflictingOutput))

      inside(
        runReinterpret(
          replayData = conflictingReplayData
        )
      ) { case Left(disagreement: DAMLe.ExternalCallRecordedResultDisagreement) =>
        disagreement.toString should not include externalCallResult.output.toHexString
        disagreement.toString should not include conflictingOutput.output.toHexString
        disagreement.toString should not include externalCallResult.config.toHexString
        disagreement.toString should not include externalCallResult.input.toHexString
      }
    }

    "not leak external-call payloads for replay errors" in {
      inside(
        runReinterpret(
          replayData = DAMLe.ExternalCallReplayData.empty
        )
      ) { case Left(error: DAMLe.ExternalCallReplayMissing) =>
        error.key shouldBe externalCallKey
        error.toString should not include externalCallResult.config.toHexString
        error.toString should not include externalCallResult.input.toHexString
      }
    }
  }
}
