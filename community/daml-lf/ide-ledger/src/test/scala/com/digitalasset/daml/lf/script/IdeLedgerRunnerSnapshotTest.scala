// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package script

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLoggingContext}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.command.ApiCommand
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine.TransactionCoder
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.speedy.{Compiler, MachineLogger}
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.testing.snapshot.Snapshot
import com.digitalasset.daml.lf.transaction.{ContractInstanceCoder, Node, TransactionOuterClass}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.{Files, Path}
import java.util.Comparator
import scala.jdk.CollectionConverters.*
import scala.util.Using

class IdeLedgerRunnerSnapshotTest extends AnyWordSpec with Matchers with Inside {

  implicit private val parserParameters: ParserParameters[this.type] =
    ParserParameters(
      defaultPackageId = Ref.PackageId.assertFromString("-pkg-"),
      languageVersion = LanguageVersion.v2_dev,
    )

  implicit private val loggingContext: NamedLoggingContext =
    NamedLoggingContext(NamedLoggerFactory.root, TraceContext.empty)

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
  private val compiledPackages =
    PureCompiledPackages.assertBuild(Map(pkgId -> pkg), Compiler.Config.Dev)

  private val alice = Ref.Party.assertFromString("Alice")
  private val seed = Hash.hashPrivateKey("IdeLedgerRunnerSnapshotTest")
  private val packageResolution = Map(pkg.pkgName -> pkgId)

  private def readEntries(file: Path): List[Snapshot.TransactionEntry] =
    Using.resource(Files.newInputStream(file)) { in =>
      Iterator
        .continually(Snapshot.SubmissionEntry.parseDelimitedFrom(in))
        .takeWhile(_ != null)
        .map(_.getTransaction)
        .toList
    }

  private def submit(
      ledger: IdeLedger,
      command: ApiCommand,
      snapshotDir: Path,
  ): IdeLedger =
    IdeLedgerRunner
      .submit(
        compiledPackages = compiledPackages,
        disclosures = Iterable.empty,
        ledger = IdeLedgerRunner.ScriptLedgerApi(ledger),
        committers = Set(alice),
        readAs = Set.empty,
        commands = List(command),
        location = None,
        seed = seed,
        machineLogger = MachineLogger(),
        interpretationConfig = interpretation.InterpretationConfig.DefaultForTesting,
        packageResolution = packageResolution,
        snapshotDir = Some(snapshotDir),
      )
      .resolve()
      .fold(err => fail(err.toString), _.result.newLedger)

  private def contractId(bytes: com.google.protobuf.ByteString): ContractId =
    ContractId.fromBytes(Bytes.fromByteString(bytes)).fold(fail(_), identity)

  private def withSnapshotDir[A](body: Path => A): A = {
    val dir = Files.createTempDirectory("ide-ledger-snapshot-test")
    try body(dir)
    finally
      Using.resource(Files.walk(dir))(_.sorted(Comparator.reverseOrder()).forEach(Files.delete(_)))
  }

  "IdeLedgerRunner.submit" should {
    "record what replay needs" in withSnapshotDir { snapshotDir =>
      val templateRef = Ref.TypeConRef.assertFromString("#-pkg-:M:T")
      val create = ApiCommand.Create(
        templateRef,
        Value.ValueRecord(None, ImmArray(None -> Value.ValueParty(alice))),
      )
      val touch = ApiCommand.ExerciseByKey(
        templateRef,
        Value.ValueParty(alice),
        Ref.ChoiceName.assertFromString("Touch"),
        Value.ValueUnit,
      )
      val ledger = submit(IdeLedger.initialLedger(Time.Timestamp.Epoch), create, snapshotDir)
      val committed = ledger.ledgerData.activeContracts.toList
      submit(ledger, touch, snapshotDir)

      inside(readEntries(snapshotDir.resolve("snapshot-ide-ledger.bin"))) {
        case List(createEntry, touchEntry) =>
          createEntry.getSubmissionSeed shouldBe empty
          Bytes.fromByteString(createEntry.getTransactionSeed) shouldBe seed.bytes
          createEntry.getContractIdVersion shouldBe
            Snapshot.ContractIdVersion.CONTRACT_ID_VERSION_V1
          createEntry.getPackageResolutionMap.asScala shouldBe Map("-pkg-" -> "-pkg-")
          createEntry.getUsedPackagesList.asScala shouldBe List("-pkg-")
          createEntry.getInputContractsList shouldBe empty
          createEntry.getKeyLookupsList shouldBe empty

          val tx = TransactionCoder
            .decodeTransaction(
              TransactionOuterClass.Transaction.parseFrom(createEntry.getRawTransaction)
            )
            .fold(err => fail(err.errorMessage), identity)
          val created = tx.nodes.values.collect { case create: Node.Create => create.coid }.toList
          val inputs = touchEntry.getInputContractsList.asScala.toList.map(bytes =>
            ContractInstanceCoder
              .decodeFatContractInstance(bytes)
              .fold(err => fail(err.errorMessage), identity)
          )
          inside((created, committed, inputs)) {
            case (List(local: ContractId.V1), List(cid: ContractId.V1), List(input)) =>
              local.isLocal shouldBe true
              cid.isAbsolute shouldBe true
              cid.discriminator shouldBe local.discriminator
              input.contractId shouldBe cid
          }
          touchEntry.getKeyLookupsList.asScala.toList
            .map(_.getContractIdsList.asScala.toList.map(contractId)) shouldBe List(committed)
      }
    }
  }
}
