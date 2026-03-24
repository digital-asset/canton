// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.modelbased

import com.digitalasset.canton.integration.util.PartiesAllocator
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.NamedLoggingContext
import com.digitalasset.canton.testing.modelbased.ast.Concrete
import com.digitalasset.canton.testing.modelbased.checker.{
  PropertyChecker,
  PropertyCheckerResultAssertions,
}
import com.digitalasset.canton.testing.modelbased.generators.{ConcreteGenerators, Shrinker}
import com.digitalasset.canton.testing.modelbased.projections.Projections
import com.digitalasset.canton.testing.modelbased.runner.{CantonInterpreter, ReferenceInterpreter}
import com.digitalasset.canton.testing.modelbased.solver.SymbolicSolver.KeyMode
import com.digitalasset.canton.testing.modelbased.syntax.Pretty
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.language.LanguageVersion

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.DurationInt

final class ModelBasedCantonIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with PropertyCheckerResultAssertions {

  import ModelBasedCantonIntegrationTest.*

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .addConfigTransforms(
        ConfigTransforms.enableAlphaVersionSupport*
      )
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
      }

  private val numParticipants = environmentDefinition.baseConfig.participants.size
  private val numParties = 3
  private val numPackages = 1

  private val generators =
    new ConcreteGenerators(
      languageVersion = LanguageVersion.v2_dev,
      readOnlyRollbacks = ProtocolVersion.dev.isDev,
      // TODO(#30398): change to NUCK once NUCK state machine is implemented
      keyMode = KeyMode.UniqueContractKeys,
    )

  "The canton interpreter" should {
    "produce projections consistent with the reference interpreter" onlyRunWith ProtocolVersion.dev in {
      implicit env =>
        import env.*

        val cantonInterpreter = CantonInterpreter.initializeAndUpload(
          participants = participants.all.toIndexedSeq,
          synchronizerId = daId,
          allocateParties =
            (ps, newParties, targetTopology) => PartiesAllocator(ps)(newParties, targetTopology),
        )

        val generator =
          generators.validScenarioGenerator(numParties, numPackages, numParticipants)

        val result = PropertyChecker
          .checkProperty(
            generate = () => generator.generate(size = 50, distinctKeyToContractRatio = 0.3),
            shrink = Shrinker.shrinkScenario,
            property = (scenario: Concrete.Scenario, cancelled: AtomicBoolean) =>
              runAndCompare(cantonInterpreter, scenario, cancelled),
            // The CI nightly job has a 60 minutes timeout, we leave a 10 minutes buffer
            timeout = 50.minutes,
            // We evaluate as many samples as possible within the allotted time.
            maxSamples = Int.MaxValue,
            // scenarios of size 50 take long to generate so we generate them in parallel
            // but only use two cores as we want to leave some CPU to the property evaluation.
            sampleBufferSize = 100,
            generatorParallelism = 2,
          )
        logger.info(result.summary)
        result.assertPassed(Pretty.prettyScenario)
    }
  }
}

object ModelBasedCantonIntegrationTest {

  /** Runs a scenario against both interpreters and checks that all projections agree. Returns
    * Right(()) on success, Left(errorMessage) on failure.
    */
  private def runAndCompare(
      cantonInterpreter: CantonInterpreter,
      scenario: Concrete.Scenario,
      cancelled: AtomicBoolean,
  )(implicit loggingContext: NamedLoggingContext): Either[String, Unit] = {
    val referenceResult = ReferenceInterpreter(loggingContext.loggerFactory)
      .runAndProject(scenario)(loggingContext.traceContext)
    val cantonResult = cantonInterpreter.runAndProject(scenario, cancelled)

    for {
      referenceProjections <- referenceResult.left.map(e => s"Reference interpreter error: $e")
      cantonProjections <- cantonResult.left.map(e => s"Canton interpreter error: $e")
      _ <- compareProjections(referenceProjections, cantonProjections)
    } yield ()
  }

  /** Compares projections from the reference interpreter and Canton. For each party:
    *   - all Canton participants should see the same projection
    *   - that projection should match the reference interpreter's projection
    */
  private def compareProjections(
      referenceProjections: Map[Projections.PartyId, Projections.Projection],
      cantonProjections: Map[
        Projections.PartyId,
        Map[Concrete.ParticipantId, Projections.Projection],
      ],
  ): Either[String, Unit] = {
    val allParties = referenceProjections.keySet
    val errors = allParties.toList.sorted.flatMap { party =>
      val refProj = referenceProjections.get(party)
      val cantonParticipantProjs = cantonProjections.getOrElse(party, Map.empty)

      // Check that all Canton participants agree
      val cantonValues = cantonParticipantProjs.values.toList
      val participantDisagreements =
        if (cantonValues.distinct.sizeIs > 1) {
          val details = cantonParticipantProjs
            .map { case (pid, proj) =>
              s"  participant $pid: ${Pretty.prettyProjection(proj)}"
            }
            .mkString("\n")
          List(s"Party $party: Canton participants disagree:\n$details")
        } else Nil

      // Check that Canton matches the reference interpreter
      val referenceDisagreements = (refProj, cantonValues.headOption) match {
        case (Some(ref), Some(canton)) if ref != canton =>
          List(
            s"Party $party: reference and Canton projections differ:\n" +
              s"  reference: ${Pretty.prettyProjection(ref)}\n" +
              s"  canton:    ${Pretty.prettyProjection(canton)}"
          )
        case (Some(_), None) =>
          List(s"Party $party: has reference projection but no Canton projection")
        case (None, Some(_)) =>
          List(s"Party $party: has Canton projection but no reference projection")
        case _ => Nil
      }

      participantDisagreements ++ referenceDisagreements
    }

    if (errors.isEmpty) Right(())
    else Left(errors.mkString("\n"))
  }
}
