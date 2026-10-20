// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.submission

import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig.FooSubmissionConfig
import com.digitalasset.canton.ledger.api.benchtool.submission.foo.RandomPartySelecting

import scala.concurrent.{ExecutionContext, Future}

class FooSubmission(
    submitter: CommandSubmitter,
    maxInFlightCommands: Int,
    submissionBatchSize: Int,
    allocatedParties: AllocatedParties,
    names: Names,
    randomnessProvider: RandomnessProvider,
) {

  // Command generation and command submission run concurrently (see `maxInFlightCommands`), so they
  // must not share a randomness provider: otherwise their draws interleave depending on stream
  // demand, and e.g. the user-id distribution stops being reproducible for a given seed.
  private val submissionRandomnessProvider: RandomnessProvider = randomnessProvider.split()

  def performSubmission(submissionConfig: FooSubmissionConfig)(implicit
      ec: ExecutionContext
  ): Future[Option[Long]] = {
    val (divulgerCmds, divulgeesToDivulgerKeyMap) = FooDivulgerCommandGenerator
      .makeCreateDivulgerCommands(
        divulgingParty = allocatedParties.signatory,
        allDivulgees = allocatedParties.divulgees,
      )
    val partySelecting =
      new RandomPartySelecting(
        config = submissionConfig,
        allocatedParties = allocatedParties,
        generationRandomnessProvider = randomnessProvider,
        submissionRandomnessProvider = submissionRandomnessProvider,
      )
    for {
      _ <-
        if (divulgerCmds.nonEmpty) {
          require(
            divulgeesToDivulgerKeyMap.nonEmpty,
            "Map from divulgees to Divulger contract keys must be non empty.",
          )
          submitter.submitSingleBatch(
            commandId = "divulgence-setup",
            actAs = Seq(allocatedParties.signatory) ++ allocatedParties.divulgees,
            commands = divulgerCmds,
          )
        } else {
          Future.unit
        }
      generator: CommandGenerator = new FooCommandGenerator(
        config = submissionConfig,
        divulgeesToDivulgerKeyMap = divulgeesToDivulgerKeyMap,
        names = names,
        allocatedParties = allocatedParties,
        partySelecting = partySelecting,
        randomnessProvider = randomnessProvider,
        submissionRandomnessProvider = submissionRandomnessProvider,
      )
      lastOffsetO <- submitter
        .generateAndSubmit(
          generator = generator,
          config = submissionConfig,
          baseActAs = List(allocatedParties.signatory) ++ allocatedParties.divulgees,
          maxInFlightCommands = maxInFlightCommands,
          submissionBatchSize = submissionBatchSize,
        )
    } yield lastOffsetO
  }
}
