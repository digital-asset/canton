// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledger.api.benchtool.submission

import com.daml.ledger.javaapi.data.Party
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.NoAuthPlugin
import com.digitalasset.canton.ledger.api.benchtool.BenchtoolSandboxFixture
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig.FooSubmissionConfig.UserId
import com.digitalasset.canton.ledger.api.benchtool.services.LedgerApiServices
import com.digitalasset.canton.ledger.api.benchtool.submission.{
  CompletionsObserver,
  ObservedCompletions,
}
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.{AppendedClues, Checkpoints, OptionValues}

import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}

class WeightedUserIdsAndSubmittersITSpec
    extends BenchtoolSandboxFixture
    with AppendedClues
    with OptionValues
    with Checkpoints {
  registerPlugin(NoAuthPlugin(loggerFactory))
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  private val timeout: FiniteDuration = 2.minutes

  "benchtool" should {
    "populate participant with contracts using specified user-ids and submitters" in { env =>
      import env.*

      val submissionConfig = WorkflowConfig.FooSubmissionConfig(
        numberOfInstances = 100,
        numberOfObservers = 1,
        numberOfExtraSubmitters = 3,
        uniqueParties = false,
        instanceDistribution = List(
          WorkflowConfig.FooSubmissionConfig.ContractDescription(
            template = "Foo1",
            weight = 1,
            payloadSizeBytes = 0,
          )
        ),
        nonConsumingExercises = None,
        consumingExercises = None,
        userIds = List(
          UserId(
            userId = "App-1",
            weight = 90,
          ),
          UserId(
            userId = "App-2",
            weight = 10,
          ),
        ),
      )
      (for {
        (apiServices, allocatedParties, fooSubmission) <- benchtoolFooSubmissionFixture(
          submissionConfig,
          maxInFlightCommands = 10,
        )
        lastOffsetO <- fooSubmission.performSubmission(submissionConfig = submissionConfig)
        lastOffset = lastOffsetO.value
        completionsApp1 <- observeCompletions(
          parties = List(allocatedParties.signatory),
          apiServices = apiServices,
          userId = "App-1",
          endOffsetInclusive = lastOffset,
        )
        completionsApp2 <- observeCompletions(
          parties = List(allocatedParties.signatory),
          apiServices = apiServices,
          userId = "App-2",
          endOffsetInclusive = lastOffset,
        )
        completionsApp1Submitter0 <- observeCompletions(
          parties = List(allocatedParties.extraSubmitters.head),
          apiServices = apiServices,
          userId = "App-1",
          endOffsetInclusive = lastOffset,
        )
        completionsApp1Submitter1 <- observeCompletions(
          parties = List(allocatedParties.extraSubmitters(1)),
          apiServices = apiServices,
          userId = "App-1",
          endOffsetInclusive = lastOffset,
        )
      } yield {
        val cp = new Checkpoint
        // App only filters
        cp(discard(completionsApp1.completions.size shouldBe 92))
        cp(discard(completionsApp2.completions.size shouldBe 8))
        // App and party filters
        cp(
          discard(
            completionsApp1Submitter0.completions.size shouldBe completionsApp1.completions.size
          )
        )
        cp(discard(completionsApp1Submitter0.completions.size shouldBe 92))
        cp(discard(completionsApp1Submitter1.completions.size shouldBe 8))
        cp.reportAll()
        succeed
      }).futureValue(timeout = PatienceConfiguration.Timeout(timeout))
    }
  }

  private def observeCompletions(
      parties: List[Party],
      userId: String,
      apiServices: LedgerApiServices,
      endOffsetInclusive: Long,
  ): Future[ObservedCompletions] = {
    val observer = CompletionsObserver(endOffsetInclusive = Some(endOffsetInclusive))
    apiServices.commandCompletionService.completions(
      config = WorkflowConfig.StreamConfig.CompletionsStreamConfig(
        name = "dummy-name",
        parties = parties.map(_.getValue),
        userId = userId,
        beginOffsetExclusive = None,
        endOffsetInclusive = Some(endOffsetInclusive),
        objectives = None,
        timeoutO = None,
        maxItemCount = None,
      ),
      observer = observer,
    )
  }

}
