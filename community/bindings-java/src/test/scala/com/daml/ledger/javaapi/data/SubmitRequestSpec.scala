// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import com.daml.ledger.api.v2.CommandSubmissionServiceOuterClass.SubmitRequest as SubmitRequestProto
import com.daml.ledger.javaapi.data.Generators.*
import com.daml.ledger.javaapi.data.codegen.Update
import com.google.protobuf.ByteString
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import java.time.{Duration, Instant}
import java.util.{Optional, List as JList}

class SubmitRequestSpec extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSize = 1, sizeRange = 3)

  "SubmitRequest.fromProto" should "convert Protoc-generated instances to data instances" in forAll(
    commandsGen.map(SubmitRequestProto.newBuilder().setCommands(_).build)
  ) { request =>
    val commands = SubmitRequest.fromProto(request)
    // Guard against fromProto not preserving a field
    SubmitRequest.toProto(commands) shouldEqual request
    SubmitRequest.fromProto(SubmitRequest.toProto(commands)) shouldEqual commands
  }

  "CommandsSubmission and UpdateSubmission copy methods" should "set every field and hide the access token" in {
    val templateId = new Identifier("package", "Module", "Template")
    val update = new Update.CreateUpdate[String, String](
      new CreateCommand(templateId, new DamlRecord(JList.of[DamlRecord.Field]())),
      _ => "created",
      id => id,
    )
    val userId = "user"
    val commandId = "command"
    val synchronizerId = "synchronizer"
    val workflowId = "workflow"
    val actAs = JList.of("alice", "bob")
    val readAs = JList.of("carol")
    val minLedgerTimeAbs = Instant.ofEpochSecond(1000)
    val minLedgerTimeRel = Duration.ofSeconds(5)
    val deduplicationDuration = Duration.ofMinutes(1)
    val accessToken = "secret-token"
    val disclosedContracts = JList.of(
      new DisclosedContract(
        ByteString.copyFromUtf8("blob"),
        synchronizerId,
        Optional.of(templateId),
        Optional.of("contract"),
      )
    )
    val packageIdSelectionPreference = JList.of("package-preference")
    val prefetchContractKeys = JList.of(new PrefetchContractKey(templateId, new Text("key")))
    val accountId = "account"

    val commandsSubmission = CommandsSubmission
      .create(userId, commandId, Optional.of(synchronizerId), JList.of[Command]())
      .withCommands(update.commands())
      .withWorkflowId(workflowId)
      .withActAs(actAs)
      .withReadAs(readAs)
      .withMinLedgerTimeAbs(minLedgerTimeAbs)
      .withMinLedgerTimeRel(minLedgerTimeRel)
      .withDeduplicationDuration(deduplicationDuration)
      .withAccessToken(accessToken)
      .withDisclosedContracts(disclosedContracts)
      .withPackageIdSelectionPreference(packageIdSelectionPreference)
      .withPrefetchContractKeys(prefetchContractKeys)
      .withAccountId(accountId)

    commandsSubmission.getUserId shouldBe userId
    commandsSubmission.getCommandId shouldBe commandId
    commandsSubmission.getSynchronizerId shouldBe Optional.of(synchronizerId)
    commandsSubmission.getCommands shouldBe update.commands()
    commandsSubmission.getWorkflowId shouldBe Optional.of(workflowId)
    commandsSubmission.getActAs shouldBe actAs
    commandsSubmission.getReadAs shouldBe readAs
    commandsSubmission.getMinLedgerTimeAbs shouldBe Optional.of(minLedgerTimeAbs)
    commandsSubmission.getMinLedgerTimeRel shouldBe Optional.of(minLedgerTimeRel)
    commandsSubmission.getDeduplicationDuration shouldBe Optional.of(deduplicationDuration)
    commandsSubmission.getAccessToken shouldBe Optional.of(accessToken)
    commandsSubmission.getDisclosedContracts shouldBe disclosedContracts
    commandsSubmission.getPackageIdSelectionPreference shouldBe packageIdSelectionPreference
    commandsSubmission.getPrefetchContractKeys shouldBe prefetchContractKeys
    commandsSubmission.getAccountId shouldBe Optional.of(accountId)
    commandsSubmission.getSubmissionId shouldBe Optional.empty()

    val updateSubmission = UpdateSubmission
      .create(userId, commandId, update)
      .withSynchronizerId(synchronizerId)
      .withWorkflowId(workflowId)
      .withActAs(actAs)
      .withReadAs(readAs)
      .withMinLedgerTimeAbs(Optional.of(minLedgerTimeAbs))
      .withMinLedgerTimeRel(Optional.of(minLedgerTimeRel))
      .withDeduplicationDuration(Optional.of(deduplicationDuration))
      .withAccessToken(Optional.of(accessToken))
      .withDisclosedContracts(disclosedContracts)
      .withPackageIdSelectionPreference(packageIdSelectionPreference)
      .withPrefetchContractKeys(prefetchContractKeys)
      .withAccountId(accountId)

    updateSubmission.toCommandsSubmission shouldBe commandsSubmission

    commandsSubmission.toString should not include (accessToken)
    commandsSubmission.toString should include("accessToken=Optional[****]")

    val offset = Long.box(42L)
    val original = CommandsSubmission
      .create(userId, commandId, Optional.of(synchronizerId), update.commands())
    val commandsWithOffset = original
      .withDeduplicationOffset(offset)
      .withActAs("alice")
    commandsWithOffset.getDeduplicationOffset shouldBe Optional.of(offset)
    commandsWithOffset.getActAs shouldBe JList.of("alice")
    original.getDeduplicationOffset shouldBe Optional.empty()
    original.getActAs shouldBe JList.of[String]()

    val updateWithOffset = UpdateSubmission
      .create(userId, commandId, update)
      .withDeduplicationOffset(Optional.of(offset))
      .withActAs("alice")
    updateWithOffset.getDeduplicationOffset shouldBe Optional.of(offset)
    updateWithOffset.getActAs shouldBe JList.of("alice")

    a[CommandsSubmission.RedundantDeduplicationSpecification] should be thrownBy
      updateWithOffset.withDeduplicationDuration(Optional.of(deduplicationDuration))
    a[CommandsSubmission.RedundantDeduplicationSpecification] should be thrownBy
      updateSubmission.withDeduplicationOffset(Optional.of(offset))
    updateSubmission
      .withDeduplicationDuration(Optional.empty())
      .withDeduplicationOffset(Optional.of(offset))
      .getDeduplicationOffset shouldBe Optional.of(offset)
  }
}
