// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.command.{ApiCommand, ApiCommands}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine.Result.lookupHandler
import com.digitalasset.daml.lf.interpretation.InterpretationConfig
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.transaction.{
  Node,
  Normalization,
  SerializationVersion,
  SubmittedTransaction,
  Validation,
}
import com.digitalasset.daml.lf.value.{ContractIdVersion, Value}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

// A contract created earlier is fetched here via unpack_template / unpack_interface: the resulting
// transaction node is a Node.Unpack (a Node.Fetch with no informees), never a root/replayable command.
class UnpackTest extends AnyWordSpec with Matchers with Inside with SuppressingLogging {

  implicit private val parserParameters: ParserParameters[this.type] =
    ParserParameters(
      defaultPackageId = Ref.PackageId.assertFromString("-unpack-engine-test-package-"),
      languageVersion = LanguageVersion.v2_dev,
    )

  private val pkgId = parserParameters.defaultPackageId
  private val pkg = p"""
    metadata ( '-unpack-engine-test-' : '1.0.0' )

    module M {
      record @serializable MyUnit = {};
      interface (this: Iface) = {
        viewtype M:MyUnit;
        method getSecret: Text;
      };

      record @serializable Secret = { p: Party, secret: Text };
      template (this: Secret) = {
        precondition True;
        signatories Cons @Party [M:Secret {p} this] (Nil @Party);
        observers Nil @Party;
        implements M:Iface {
          view = M:MyUnit {};
          method getSecret = M:Secret {secret} this;
        };
      };

      record @serializable Helper = { p: Party };
      template (this: Helper) = {
        precondition True;
        signatories Cons @Party [M:Helper {p} this] (Nil @Party);
        observers Nil @Party;

        choice ReadSecret (self) (cid: ContractId M:Secret) : Text,
          controllers Cons @Party [M:Helper {p} this] (Nil @Party)
          to ubind s: M:Secret <- unpack_template @M:Secret cid in upure @Text (M:Secret {secret} s);

        choice ReadSecretI (self) (cid: ContractId M:Iface) : Text,
          controllers Cons @Party [M:Helper {p} this] (Nil @Party)
          to ubind s: M:Iface <- unpack_interface @M:Iface cid in upure @Text (call_method @M:Iface getSecret s);
      };
    }
  """

  private val alice = Ref.Party.assertFromString("Alice")
  private val participantId = Ref.ParticipantId.assertFromString("participant")
  private val submissionSeed = Hash.hashPrivateKey("UnpackEngineTest")
  private val let = Time.Timestamp.now()

  private val secretTplId = Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("M:Secret"))
  private val helperTplId = Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("M:Helper"))

  private val secretCid = Value.ContractId.V1(Hash.hashPrivateKey("unpack-engine-test-secret"))
  private val secretPayload = Value.ValueRecord(
    None,
    ImmArray(None -> Value.ValueParty(alice), None -> Value.ValueText("42")),
  )
  private val secretContract = TransactionBuilder.fatContractInstanceWithDummyDefaults(
    version = SerializationVersion.StableVersions.max,
    packageName = pkg.pkgName,
    template = secretTplId,
    arg = secretPayload,
    signatories = List(alice),
    contractId = secretCid,
  )

  private def newEngine(): Engine = {
    val engine = new Engine(Engine.DevConfig, loggerFactory)
    engine.preloadPackage(pkgId, pkg).consume(lookupHandler()) shouldBe Right(())
    engine
  }

  private def submit(choiceName: String) = {
    val command = ApiCommand.CreateAndExercise(
      helperTplId.toRef,
      Value.ValueRecord(None, ImmArray(None -> Value.ValueParty(alice))),
      Ref.ChoiceName.assertFromString(choiceName),
      Value.ValueContractId(secretCid),
    )
    newEngine()
      .submit(
        packageMap = Map(pkgId -> (pkg.pkgName, pkg.pkgVersion)),
        packagePreference = Set(pkgId),
        submitters = Set(alice),
        cmds = ApiCommands(ImmArray(command), let, "unpack-engine-test"),
        participantId = participantId,
        submissionSeed = submissionSeed,
        contractIdVersion = ContractIdVersion.V1,
        interpretationConfig = InterpretationConfig.Dev,
        prefetchKeys = Seq.empty,
      )
      .consume(lookupHandler(pcs = { case `secretCid` => secretContract }))
  }

  "unpack_template" should {
    "produce an Unpack node and return the payload" in {
      inside(submit("ReadSecret")) { case Right((tx, _)) =>
        inside(tx.nodes.values.collectFirst { case node: Node.Fetch => node }) {
          case Some(Node.Unpack(coid, packageName, templateId, interfaceId, _)) =>
            coid shouldBe secretCid
            packageName shouldBe pkg.pkgName
            templateId shouldBe secretTplId
            interfaceId shouldBe None
        }

        val Some(exercise: Node.Exercise) = tx.nodes.collectFirst {
          case (_, exercise: Node.Exercise) => exercise
        }: @unchecked
        exercise.exerciseResult shouldBe Some(Value.ValueText("42"))
      }
    }

    "validate" in {
      inside(submit("ReadSecret")) { case Right((tx, meta)) =>
        val ntx = SubmittedTransaction(Normalization.normalizeTx(tx))
        val replayed = newEngine()
          .replayAndCollectMetrics(
            submitters = Set(alice),
            tx = ntx,
            ledgerEffectiveTime = let,
            participantId = participantId,
            preparationTime = meta.preparationTime,
            submissionSeed = submissionSeed,
            contractIdVersion = ContractIdVersion.V1,
            interpretationConfig = InterpretationConfig.Dev,
          )
          .consume(lookupHandler(pcs = { case `secretCid` => secretContract }))
        inside(replayed) { case Right((rtx, _, _)) =>
          Validation.isReplayedBy(ntx, rtx) shouldBe Right(())
        }
      }
    }
  }

  "unpack_interface" should {
    "produce an Unpack node and return the payload" in {
      inside(submit("ReadSecretI")) { case Right((tx, _)) =>
        inside(tx.nodes.values.collectFirst { case node: Node.Fetch => node }) {
          case Some(Node.Unpack(coid, packageName, templateId, interfaceId, _)) =>
            coid shouldBe secretCid
            packageName shouldBe pkg.pkgName
            // The unpacked node always records the resolved concrete template, never the interface.
            templateId shouldBe secretTplId
            interfaceId shouldBe None
        }

        val Some(exercise: Node.Exercise) = tx.nodes.collectFirst {
          case (_, exercise: Node.Exercise) => exercise
        }: @unchecked
        exercise.exerciseResult shouldBe Some(Value.ValueText("42"))
      }
    }

    "validate" in {
      inside(submit("ReadSecretI")) { case Right((tx, meta)) =>
        val ntx = SubmittedTransaction(Normalization.normalizeTx(tx))
        val replayed = newEngine()
          .replayAndCollectMetrics(
            submitters = Set(alice),
            tx = ntx,
            ledgerEffectiveTime = let,
            participantId = participantId,
            preparationTime = meta.preparationTime,
            submissionSeed = submissionSeed,
            contractIdVersion = ContractIdVersion.V1,
            interpretationConfig = InterpretationConfig.Dev,
            packageResolution = Map(pkg.pkgName -> pkgId),
          )
          .consume(lookupHandler(pcs = { case `secretCid` => secretContract }))
        inside(replayed) { case Right((rtx, _, _)) =>
          Validation.isReplayedBy(ntx, rtx) shouldBe Right(())
        }
      }
    }
  }
}
