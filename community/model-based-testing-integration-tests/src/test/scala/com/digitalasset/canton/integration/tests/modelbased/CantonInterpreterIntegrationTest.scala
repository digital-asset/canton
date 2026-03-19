// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.modelbased

import com.daml.logging.LoggingContext
import com.digitalasset.canton.annotations.NuckTest
import com.digitalasset.canton.integration.util.PartiesAllocator
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.testing.modelbased.ast.Concrete
import com.digitalasset.canton.testing.modelbased.generators.{ConcreteGenerators, Shrinker}
import com.digitalasset.canton.testing.modelbased.projections.Projections
import com.digitalasset.canton.testing.modelbased.runner.{CantonInterpreter, ReferenceInterpreter}
import com.digitalasset.canton.testing.modelbased.solver.SymbolicSolver.KeyMode
import com.digitalasset.canton.testing.modelbased.syntax.Pretty
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.language.LanguageVersion

import scala.concurrent.duration.DurationInt

@NuckTest
final class CantonInterpreterIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  import CantonInterpreterIntegrationTest.*

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

        val scenario =
          generators
            .validScenarioGenerator(numParties, numPackages, numParticipants)
            .generate(size = 50, distinctKeyToContractRatio = 0.3)
        logger.info(s"Running scenario:\n${Pretty.prettyScenario(scenario)}")

        runAndCompare(cantonInterpreter, scenario) match {
          case Right(()) => succeed
          case Left(error) =>
            logger.error(s"Comparison failed: $error")
            logger.error("Shrinking scenario...")
            val shrinkResult =
              Shrinker.shrinkToFailure(
                scenario,
                error,
                runAndCompare(cantonInterpreter, _),
                timeout = 3.minutes,
              )(Shrinker.shrinkScenario)
            logger.error(shrinkResult.summary)
            logger.error(s"Shrunk scenario:\n${Pretty.prettyScenario(shrinkResult.value)}")
            logger.error(s"Original scenario:\n${Pretty.prettyScenario(scenario)}")
            fail(shrinkResult.error)
        }
    }
  }
}

object CantonInterpreterIntegrationTest {

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val referenceInterpreter: ReferenceInterpreter = ReferenceInterpreter()

  /** Runs a scenario against both interpreters and checks that all projections agree. Returns
    * Right(()) on success, Left(errorMessage) on failure.
    */
  private def runAndCompare(
      cantonInterpreter: CantonInterpreter,
      scenario: Concrete.Scenario,
  ): Either[String, Unit] = {
    val referenceResult = referenceInterpreter.runAndProject(scenario)
    val cantonResult = cantonInterpreter.runAndProject(scenario)

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
