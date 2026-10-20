// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.command.{ApiCommand, ApiCommands}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine.Result.lookupHandler
import com.digitalasset.daml.lf.interpretation.InterpretationConfig
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.testing.snapshot.Snapshot
import com.digitalasset.daml.lf.transaction.{
  ContractInstanceCoder,
  CreationTime,
  FatContractInstance,
  Node,
}
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.daml.lf.value.{ContractIdVersion, Value}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.{Files, Path}
import java.util.Comparator
import scala.jdk.CollectionConverters.*
import scala.util.Using

class SnapshotRecorderTest extends AnyWordSpec with Matchers with Inside with SuppressingLogging {

  implicit private val parserParameters: ParserParameters[this.type] =
    ParserParameters(
      defaultPackageId = Ref.PackageId.assertFromString("-pkg-"),
      languageVersion = LanguageVersion.v2_dev,
    )

  private val pkgId = parserParameters.defaultPackageId
  private val pkg = p"""
    metadata ( '-pkg-' : '1.0.0' )

    module M {
      record @serializable T = { party: Party };

      template (this: T) = {
        precondition True;
        signatories Cons @Party [M:T {party} this] (Nil @Party);
        observers Nil @Party;

        choice @nonConsuming Touch (self) (arg: Unit) : Unit,
          controllers Cons @Party [M:T {party} this] (Nil @Party)
          to upure @Unit ();

        key @Party (M:T {party} this) (\(p: Party) -> Cons @Party [p] (Nil @Party));
      };
    }
  """

  private val alice = Ref.Party.assertFromString("Alice")
  private val participantId = Ref.ParticipantId.assertFromString("participant")
  private val submissionSeed = Hash.hashPrivateKey("SnapshotRecorderTest")
  private val let = Time.Timestamp.now()

  private val templateRef = Ref.TypeConRef.assertFromString("#-pkg-:M:T")

  private def submit(
      engine: Engine,
      command: ApiCommand,
      contract: Option[FatContractInstance],
  ) =
    engine
      .submit(
        packageMap = Map(pkgId -> (pkg.pkgName -> pkg.metadata.version)),
        packagePreference = Set(pkgId),
        submitters = Set(alice),
        cmds = ApiCommands(ImmArray(command), let, "snapshot-recorder-test"),
        participantId = participantId,
        submissionSeed = submissionSeed,
        contractIdVersion = ContractIdVersion.V2,
        interpretationConfig = InterpretationConfig.Dev,
        prefetchKeys = Seq.empty,
      )
      .consume(
        lookupHandler(
          pcs = contract.map(c => c.contractId -> c).toMap,
          keys =
            contract.flatMap(c => c.contractKeyWithMaintainers.map(_.globalKey -> Vector(c))).toMap,
        )
      )

  private def withSnapshotDir[A](body: Path => A): A = {
    val dir = Files.createTempDirectory("snapshot-recorder-test")
    try body(dir)
    finally
      Using.resource(Files.walk(dir))(_.sorted(Comparator.reverseOrder()).forEach(Files.delete(_)))
  }

  "Engine.submit" should {
    "record what replay needs" in withSnapshotDir { snapshotDir =>
      val engine = new Engine(Engine.DevConfig.copy(snapshotDir = Some(snapshotDir)), loggerFactory)
      engine.preloadPackage(pkgId, pkg).consume(lookupHandler()) shouldBe Right(())

      val create = ApiCommand.Create(
        templateRef,
        Value.ValueRecord(None, ImmArray(None -> Value.ValueParty(alice))),
      )
      val createNode = inside(submit(engine, create, None)) { case Right((tx, _)) =>
        inside(tx.nodes.values.toList) { case List(node: Node.Create) => node }
      }
      val contract = FatContractInstance.fromCreateNode(
        createNode.copy(coid =
          ContractId.V1.assertBuild(Hash.hashPrivateKey("contract"), Bytes.assertFromString("00"))
        ),
        CreationTime.CreatedAt(let),
        Bytes.Empty,
      )
      val touch = ApiCommand.ExerciseByKey(
        templateRef,
        Value.ValueParty(alice),
        Ref.ChoiceName.assertFromString("Touch"),
        Value.ValueUnit,
      )
      submit(engine, touch, Some(contract)) shouldBe a[Right[?, ?]]

      val entries = Using.resource(
        Files.newInputStream(snapshotDir.resolve("snapshot-participant.bin"))
      ) { in =>
        Iterator
          .continually(Snapshot.SubmissionEntry.parseDelimitedFrom(in))
          .takeWhile(_ != null)
          .map(_.getTransaction)
          .toList
      }
      inside(entries) { case List(createEntry, touchEntry) =>
        Bytes.fromByteString(createEntry.getSubmissionSeed) shouldBe submissionSeed.bytes
        Bytes.fromByteString(createEntry.getTransactionSeed) shouldBe
          Hash.deriveTransactionSeed(submissionSeed, participantId, let).bytes
        createEntry.getContractIdVersion shouldBe Snapshot.ContractIdVersion.CONTRACT_ID_VERSION_V2
        createEntry.getPackageResolutionMap.asScala shouldBe Map("-pkg-" -> "-pkg-")
        createEntry.getUsedPackagesList.asScala shouldBe List("-pkg-")
        createEntry.getInputContractsList shouldBe empty
        createEntry.getKeyLookupsList shouldBe empty

        touchEntry.getInputContractsList.asScala.toList.map(
          ContractInstanceCoder.decodeFatContractInstance(_).map(_.contractId)
        ) shouldBe List(Right(contract.contractId))
        touchEntry.getKeyLookupsList.asScala.toList.map(
          _.getContractIdsList.asScala.toList.map(b => Bytes.fromByteString(b))
        ) shouldBe List(List(contract.contractId.toBytes))
      }
    }
  }
}
