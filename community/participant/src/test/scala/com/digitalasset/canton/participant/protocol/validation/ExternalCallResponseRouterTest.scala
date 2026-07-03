// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.data.{TransactionView, ViewPosition}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.validation.ExternalCallResponseRouter.ViewWithHostedParties
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.ExampleTransactionFactory.{signatory, submitter}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext, LfPartyId}
import com.digitalasset.daml.lf.data.Bytes
import com.digitalasset.daml.lf.transaction.ExternalCallResult

/** Direct unit tests for [[ExternalCallResponseRouter]]: the [[ExternalCallResponseRouter.check]]
  * validation suite and its collaborators ([[ExternalCallResponseRouter.Result]],
  * [[ExternalCallValidationRoutes]]).
  *
  * These exercise the router in isolation -- without going through
  * [[TransactionConfirmationResponsesFactory]] -- and assert on the router's own outputs
  * ([[ExternalCallValidationOccurrence]]s, [[ExternalCallValidationRoutes]] and per-party
  * [[ExternalCallConsistencyChecker.Inconsistency]]s) rather than on assembled
  * `ConfirmationResponse`s. Verdict merging / response assembly remains covered by
  * [[TransactionConfirmationResponsesFactoryExternalCallTest]].
  */
final class ExternalCallResponseRouterTest
    extends BaseTestWordSpec
    with HasExecutionContext
    with ExternalCallValidationTestUtil {

  protected val factory: ExampleTransactionFactory =
    new ExampleTransactionFactory(versionOverride = Some(ProtocolVersion.dev))()

  private val externalCallKey: DAMLe.ExternalCallKey =
    DAMLe.ExternalCallKey.fromResult(externalCallResult)

  /** Resolves only `hostedParties` as hosted. */
  private def hostingTopologySnapshot(hostedParties: Set[LfPartyId]): TopologySnapshot = {
    val snapshot = mock[TopologySnapshot]
    when(snapshot.canConfirm(any[ParticipantId], any[Set[LfPartyId]])(anyTraceContext))
      .thenAnswer { (_: ParticipantId, parties: Set[LfPartyId]) =>
        FutureUnlessShutdown.pure(parties.intersect(hostedParties))
      }
    snapshot
  }

  private def runCheck(
      sut: ExternalCallResponseRouter,
      views: Seq[(ViewPosition, TransactionView)],
      conformanceErrors: Seq[ModelConformanceChecker.Error] = Seq.empty,
      topologySnapshot: TopologySnapshot = identityTopologySnapshot,
      runValidation: Boolean = true,
  ): ExternalCallResponseRouter.Result =
    sut
      .check(
        requestId,
        views.map { case (viewPosition, view) => viewPosition -> participantView(view) }.toMap,
        topologySnapshot,
        FutureUnlessShutdown.pure(conformanceErrors),
        runValidation,
      )
      .futureValueUS

  private def hostedView(
      viewPosition: ViewPosition,
      view: TransactionView,
      hostedConfirmingParties: Set[LfPartyId],
  ): ViewWithHostedParties =
    ViewWithHostedParties(viewPosition, participantView(view), hostedConfirmingParties)

  /** The recorded-consistency input of [[ExternalCallResponseRouter.validationOccurrences]], as
    * [[ExternalCallResponseRouter.check]] computes it.
    */
  private def recordedConsistencyOf(
      viewsWithHostedParties: Seq[ViewWithHostedParties]
  ): ExternalCallConsistencyChecker.Result =
    ExternalCallConsistencyChecker.check(
      viewsWithHostedParties.map(view => view.viewPosition -> view.view).toMap,
      viewsWithHostedParties.flatMap(_.hostedConfirmingParties).toSet,
    )

  /** Two views recording the same external call with disagreeing outputs, both seen by `submitter`.
    */
  private def conflictingViews(
      confirmers: Set[LfPartyId] = Set(submitter, signatory)
  ): Seq[(ViewPosition, TransactionView)] = {
    val example = factory.MultipleRoots
    val left = withExternalCallResults(
      withConfirmers(example.rootViews(4), confirmers),
      Seq(externalCallViewResult(exerciseIndex = 0, externalCallResult, Set(submitter))),
    )
    val right = withExternalCallResults(
      withConfirmers(example.rootViews(5), confirmers),
      Seq(externalCallViewResult(exerciseIndex = 1, otherExternalCallOutput, Set(submitter))),
    )
    Seq(leftViewPosition -> left, rightViewPosition -> right)
  }

  private def hostedConflictingViews(
      hostedConfirmingParties: Set[LfPartyId]
  ): Seq[ViewWithHostedParties] =
    conflictingViews().map { case (viewPosition, view) =>
      hostedView(viewPosition, view, hostedConfirmingParties)
    }

  "ExternalCallResponseRouter check" should {
    "short-circuit to the empty result when no view records external-call results" in {
      val example = factory.MultipleRoots
      val confirmers = Set(submitter, signatory)
      val view = withConfirmers(example.rootViews(4), confirmers)
      val validator = new RecordingExternalCallValidator(Map.empty)
      val failingTopologySnapshot = {
        val snapshot = mock[TopologySnapshot]
        when(snapshot.canConfirm(any[ParticipantId], any[Set[LfPartyId]])(anyTraceContext))
          .thenAnswer { (_: ParticipantId, _: Set[LfPartyId]) =>
            fail("the fast path must not resolve hosted confirming parties")
          }
        snapshot
      }

      val result = externalCallRouter(validator)
        .check(
          requestId,
          Map(leftViewPosition -> participantView(view)),
          failingTopologySnapshot,
          // The fast path must not await the model-conformance errors either.
          FutureUnlessShutdown.failed(
            new RuntimeException("the fast path must not await the conformance errors")
          ),
          runValidation = true,
        )
        .futureValueUS

      result shouldBe ExternalCallResponseRouter.Result.empty
      validator.observed shouldBe empty
    }

    "check recorded consistency, alarm, and skip re-validation of disagreeing results" in {
      val validator = new RecordingExternalCallValidator(
        Map(
          externalCallKey -> ExternalCallValidator.UnableToValidate(
            "extension service is not configured"
          )
        )
      )
      val sut = externalCallRouter(validator)

      val result = assertRecordedDisagreementAlarms() {
        runCheck(sut, conflictingViews())
      }

      // The visible recorded disagreement rejects the checking party...
      result.recordedConsistency.hostedInconsistencies.keySet shouldBe Set(submitter)
      result.recordedConsistency.visibleInconsistencies should have size 1
      result
        .inconsistenciesForView(leftViewPosition, Set(submitter, signatory))
        .map(_._1) shouldBe Seq(submitter)

      // ...so the disagreeing occurrences produce no re-validation work.
      validator.observed shouldBe empty
      result.validationRoutes shouldBe ExternalCallValidationRoutes.empty
    }

    "alarm on visible disagreements that affect no hosted party" in {
      val validator = new RecordingExternalCallValidator(Map.empty)
      val sut = externalCallRouter(validator)

      val result = assertRecordedDisagreementAlarms() {
        runCheck(
          sut,
          conflictingViews(),
          topologySnapshot = hostingTopologySnapshot(Set.empty),
        )
      }

      result.recordedConsistency.hostedInconsistencies shouldBe empty
      result.recordedConsistency.visibleInconsistencies should have size 1
      // No hosted checking party, so nothing is re-validated.
      validator.observed shouldBe empty
      result.validationRoutes shouldBe ExternalCallValidationRoutes.empty
    }

    "skip re-validation when instructed while still checking and alarming" in {
      val example = factory.MultipleRoots
      val confirmers = Set(submitter, signatory)
      val undisputed = withExternalCallResults(
        withConfirmers(example.rootViews(3), confirmers),
        Seq(
          externalCallViewResult(
            exerciseIndex = 2,
            externalCallResult.copy(functionId = "undisputed-function"),
            Set(submitter),
          )
        ),
      )
      val validator = new RecordingExternalCallValidator(Map.empty)
      val sut = externalCallRouter(validator)

      val result = assertRecordedDisagreementAlarms() {
        runCheck(
          sut,
          conflictingViews() :+ (unrelatedViewPosition -> undisputed),
          runValidation = false,
        )
      }

      result.recordedConsistency.hostedInconsistencies.keySet shouldBe Set(submitter)
      // The undisputed result would be re-validated, but re-validation is switched off.
      validator.observed shouldBe empty
      result.validationRoutes shouldBe ExternalCallValidationRoutes.empty
    }

    "not re-validate keys whose recorded outputs disagree across views with disjoint checking parties" in {
      // The same call is recorded with disagreeing outputs in two views, each output checked by a
      // different party: neither party is recorded-inconsistent on its own, but the key has
      // disagreeing outputs across the request and is therefore not re-validated (independently
      // of which views end up being approved); the disagreement itself is alarmed.
      val example = factory.MultipleRoots
      val confirmers = Set(submitter, signatory)
      val left = withExternalCallResults(
        withConfirmers(example.rootViews(4), confirmers),
        Seq(externalCallViewResult(exerciseIndex = 0, externalCallResult, Set(submitter))),
      )
      val right = withExternalCallResults(
        withConfirmers(example.rootViews(5), confirmers),
        Seq(externalCallViewResult(exerciseIndex = 1, otherExternalCallOutput, Set(signatory))),
      )
      val validator = new RecordingExternalCallValidator(Map.empty)

      val result = assertRecordedDisagreementAlarms() {
        runCheck(
          externalCallRouter(validator),
          Seq(leftViewPosition -> left, rightViewPosition -> right),
        )
      }

      result.recordedConsistency.hostedInconsistencies shouldBe empty
      result.recordedConsistency.visibleInconsistencies should have size 1
      validator.observed shouldBe empty
      result.validationRoutes shouldBe ExternalCallValidationRoutes.empty
    }

    "route replay disagreements surfaced by the model-conformance errors" in {
      val views = conflictingViews()
      val leftViewHash = views.headOption.value._2.viewHash
      val disagreement = DAMLe.ExternalCallRecordedResultDisagreement(
        key = externalCallKey,
        outputs = Set(externalCallResult.output, otherExternalCallOutput.output),
      )
      val sut = externalCallRouter()

      val result = assertRecordedDisagreementAlarms() {
        runCheck(
          sut,
          views,
          conformanceErrors = Seq(ModelConformanceChecker.DAMLeError(disagreement, leftViewHash)),
        )
      }

      // The replay disagreement is routed to the checking party, so a model-conformance error
      // carrying it is routable and suppresses the would-be malformed model-conformance reject.
      val routableError = ModelConformanceChecker.DAMLeError(disagreement, leftViewHash)
      result.isRoutableModelConformanceError(routableError) shouldBe true

      // An unrelated disagreement is not routable.
      val unrelatedDisagreement = DAMLe.ExternalCallRecordedResultDisagreement(
        key = DAMLe.ExternalCallKey.fromResult(externalCallResult.copy(functionId = "unrelated")),
        outputs = Set(externalCallResult.output, otherExternalCallOutput.output),
      )
      result.isRoutableModelConformanceError(
        ModelConformanceChecker.DAMLeError(unrelatedDisagreement, leftViewHash)
      ) shouldBe false
    }
  }

  "ExternalCallResponseRouter local validation" should {
    "reject locally validated output mismatches for hosted checking parties" in {
      val example = factory.MultipleRoots
      val confirmers = Set(submitter, signatory)
      val view = withExternalCallResults(
        withConfirmers(example.rootViews(4), confirmers),
        Seq(externalCallViewResult(exerciseIndex = 0, externalCallResult, Set(submitter))),
      )
      val viewsWithHostedParties = Seq(hostedView(leftViewPosition, view, confirmers))
      val validator = new RecordingExternalCallValidator(
        Map(
          externalCallKey -> ExternalCallValidator.Mismatched(
            computedOutput = otherExternalCallOutput.output,
            recordedOutput = externalCallResult.output,
          )
        )
      )
      val sut = externalCallRouter(validator)

      val occurrences = sut.validationOccurrences(
        viewsWithHostedParties,
        recordedConsistencyOf(viewsWithHostedParties),
      )
      // The checking party is the only affected party for the single recorded call.
      val occurrence = occurrences.loneElement
      occurrence.viewPosition shouldBe leftViewPosition
      occurrence.result.result shouldBe externalCallResult
      occurrence.hostedCheckingParties shouldBe Set(submitter)

      val routes = sut.validateExternalCalls(occurrences).futureValueUS

      validator.observed shouldBe Seq(externalCallKey -> externalCallResult.output)
      routes.rejects.keySet shouldBe Set(submitter)
      val inconsistency = routes.rejects(submitter).loneElement
      inconsistency.key shouldBe externalCallKey
      inconsistency.outputs shouldBe Set(externalCallResult.output, otherExternalCallOutput.output)
      routes.abstains shouldBe empty

      // Routed to the affected view for the checking party only; the co-confirmer is left to approve.
      routes.rejectsForView(leftViewPosition, confirmers).map(_._1) shouldBe Seq(submitter)
    }

    "abstain when locally responsible validation cannot obtain comparable output bytes" in {
      val example = factory.MultipleRoots
      val confirmers = Set(submitter, signatory)
      val view = withExternalCallResults(
        withConfirmers(example.rootViews(4), confirmers),
        Seq(externalCallViewResult(exerciseIndex = 0, externalCallResult, Set(submitter))),
      )
      val viewsWithHostedParties = Seq(hostedView(leftViewPosition, view, confirmers))
      val validator = new RecordingExternalCallValidator(
        Map(
          externalCallKey -> ExternalCallValidator.UnableToValidate(
            "extension service is not configured"
          )
        )
      )
      val sut = externalCallRouter(validator)

      val occurrences = sut.validationOccurrences(
        viewsWithHostedParties,
        recordedConsistencyOf(viewsWithHostedParties),
      )
      val routes = sut.validateExternalCalls(occurrences).futureValueUS

      validator.observed shouldBe Seq(externalCallKey -> externalCallResult.output)
      routes.rejects shouldBe empty
      routes.abstains.keySet shouldBe Set(submitter)
      val abstain = routes.abstains(submitter).loneElement
      abstain.viewPosition shouldBe leftViewPosition
      abstain.reason should include("extension service is not configured")

      routes
        .abstainsForView(leftViewPosition, confirmers)
        .map(_._1) shouldBe Seq(submitter)
    }

    "scope local external-call validation abstains to the affected view" in {
      val example = factory.MultipleRoots
      val confirmers = Set(submitter)
      val left = withExternalCallResults(
        withConfirmers(example.rootViews(4), confirmers),
        Seq(externalCallViewResult(exerciseIndex = 0, externalCallResult, Set(submitter))),
      )
      val right = withConfirmers(example.rootViews(5), confirmers)
      val viewsWithHostedParties = Seq(
        hostedView(leftViewPosition, left, confirmers),
        hostedView(rightViewPosition, right, confirmers),
      )
      val validator = new RecordingExternalCallValidator(
        Map(
          externalCallKey -> ExternalCallValidator.UnableToValidate(
            "extension service is not configured"
          )
        )
      )
      val sut = externalCallRouter(validator)

      val occurrences = sut.validationOccurrences(
        viewsWithHostedParties,
        recordedConsistencyOf(viewsWithHostedParties),
      )
      // Only the left view carries an external call result.
      occurrences.loneElement.viewPosition shouldBe leftViewPosition

      val routes = sut.validateExternalCalls(occurrences).futureValueUS

      validator.observed shouldBe Seq(externalCallKey -> externalCallResult.output)
      routes.abstains(submitter).loneElement.viewPosition shouldBe leftViewPosition
      routes
        .abstainsForView(leftViewPosition, confirmers)
        .map(_._1) shouldBe Seq(submitter)
      // The abstain does not leak into the unaffected view.
      routes
        .abstainsForView(rightViewPosition, confirmers) shouldBe empty
    }

    "not locally validate external calls when no hosted confirmer is a checking party" in {
      val example = factory.MultipleRoots
      val view = withExternalCallResults(
        withConfirmers(example.rootViews(4), Set(submitter, signatory)),
        Seq(externalCallViewResult(exerciseIndex = 0, externalCallResult, Set(signatory))),
      )
      // signatory is the checking party but is not hosted as a confirmer here.
      val viewsWithHostedParties = Seq(hostedView(leftViewPosition, view, Set(submitter)))
      val validator = new RecordingExternalCallValidator(
        Map(
          externalCallKey -> ExternalCallValidator.Mismatched(
            computedOutput = otherExternalCallOutput.output,
            recordedOutput = externalCallResult.output,
          )
        )
      )
      val sut = externalCallRouter(validator)

      val occurrences = sut.validationOccurrences(
        viewsWithHostedParties,
        recordedConsistencyOf(viewsWithHostedParties),
      )
      occurrences shouldBe empty

      val routes = sut.validateExternalCalls(occurrences).futureValueUS
      validator.observed shouldBe empty
      routes.rejects shouldBe empty
      routes.abstains shouldBe empty
    }

    "deduplicate local external-call validation by semantic key and route the result to all occurrences" in {
      val example = factory.MultipleRoots
      val confirmers = Set(submitter, signatory)
      val left = withExternalCallResults(
        withConfirmers(example.rootViews(4), confirmers),
        Seq(externalCallViewResult(exerciseIndex = 0, externalCallResult, Set(submitter))),
      )
      val right = withExternalCallResults(
        withConfirmers(example.rootViews(5), confirmers),
        // Same semantic key and output as `left`, but a distinct occurrence (exerciseIndex 1).
        Seq(externalCallViewResult(exerciseIndex = 1, externalCallResult, Set(submitter))),
      )
      val viewsWithHostedParties = Seq(
        hostedView(leftViewPosition, left, confirmers),
        hostedView(rightViewPosition, right, confirmers),
      )
      val validator = new RecordingExternalCallValidator(
        Map(
          externalCallKey -> ExternalCallValidator.Mismatched(
            computedOutput = otherExternalCallOutput.output,
            recordedOutput = externalCallResult.output,
          )
        )
      )
      val sut = externalCallRouter(validator)

      val occurrences = sut.validationOccurrences(
        viewsWithHostedParties,
        recordedConsistencyOf(viewsWithHostedParties),
      )
      occurrences.map(_.viewPosition).toSet shouldBe Set(leftViewPosition, rightViewPosition)

      val routes = sut.validateExternalCalls(occurrences).futureValueUS

      // The validator is consulted exactly once for the shared semantic key.
      validator.observed shouldBe Seq(externalCallKey -> externalCallResult.output)
      routes.rejects.keySet shouldBe Set(submitter)
      val inconsistency = routes.rejects(submitter).loneElement
      inconsistency.occurrences.map(_.viewPosition) shouldBe Set(
        leftViewPosition,
        rightViewPosition,
      )

      // The single validation result is routed to every affected view.
      routes.rejectsForView(leftViewPosition, confirmers).map(_._1) shouldBe Seq(submitter)
      routes.rejectsForView(rightViewPosition, confirmers).map(_._1) shouldBe Seq(submitter)
    }

    "exclude parties with recorded disagreements from re-validation" in {
      val viewsWithHostedParties = hostedConflictingViews(Set(submitter, signatory))

      // The recorded disagreement already rejects the checking party, so its occurrences produce
      // no re-validation work.
      val occurrences = externalCallRouter().validationOccurrences(
        viewsWithHostedParties,
        recordedConsistencyOf(viewsWithHostedParties),
      )
      occurrences shouldBe empty
    }
  }

  "ExternalCallResponseRouter recorded-disagreement routing" should {
    "not attribute external-call disagreements to unrelated hosted views" in {
      val example = factory.MultipleRoots
      val confirmers = Set(submitter, signatory)
      val unrelatedView = withConfirmers(example.rootViews(3), confirmers)
      val views = conflictingViews() :+ (unrelatedViewPosition -> unrelatedView)

      val result = assertRecordedDisagreementAlarms() {
        runCheck(externalCallRouter(), views)
      }

      // The conflicting views attribute the disagreement to the checking party.
      result
        .inconsistenciesForView(leftViewPosition, confirmers)
        .map(_._1) shouldBe Seq(submitter)
      // The unrelated view (no external call results) is not attributed any disagreement.
      result.inconsistenciesForView(unrelatedViewPosition, confirmers) shouldBe empty
    }

    "emit external-call disagreement inconsistencies for every affected view" in {
      val example = factory.MultipleRoots
      val confirmers = Set(submitter, signatory)
      val firstCall = externalCallResult.copy(functionId = "function-a")
      val secondCall = externalCallResult.copy(functionId = "function-b")
      val firstKey = DAMLe.ExternalCallKey.fromResult(firstCall)
      val secondKey = DAMLe.ExternalCallKey.fromResult(secondCall)

      def view(
          baseView: TransactionView,
          result: ExternalCallResult,
          exerciseIndex: Int,
      ): TransactionView =
        withExternalCallResults(
          withConfirmers(baseView, confirmers),
          Seq(externalCallViewResult(exerciseIndex = exerciseIndex, result, Set(submitter))),
        )

      val views = Seq(
        leftViewPosition -> view(example.rootViews(4), firstCall, 0),
        rightViewPosition -> view(
          example.rootViews(5),
          firstCall.copy(output = Bytes.fromStringUtf8("other-a")),
          1,
        ),
        unrelatedViewPosition -> view(example.rootViews(4), secondCall, 2),
        secondRightViewPosition -> view(
          example.rootViews(5),
          secondCall.copy(output = Bytes.fromStringUtf8("other-b")),
          3,
        ),
      )

      // One visible disagreement per distinct external call, both alarmed and routed to the
      // checking party.
      val result = assertRecordedDisagreementAlarms(count = 2) {
        runCheck(externalCallRouter(), views)
      }

      result.recordedConsistency.visibleInconsistencies should have size 2
      result.recordedConsistency.hostedInconsistencies.keySet shouldBe Set(submitter)
      result.recordedConsistency.hostedInconsistencies(submitter) should have size 2

      // Each affected view rejects the checking party with the inconsistency for its own call.
      val expectedKeyByView = Map(
        leftViewPosition -> firstKey,
        rightViewPosition -> firstKey,
        unrelatedViewPosition -> secondKey,
        secondRightViewPosition -> secondKey,
      )
      expectedKeyByView.foreach { case (viewPosition, expectedKey) =>
        val routed = result.inconsistenciesForView(viewPosition, confirmers)
        routed.map(_._1) shouldBe Seq(submitter)
        routed.loneElement._2.key shouldBe expectedKey
      }
    }

    "route recorded external-call replay ambiguity for disjoint checking parties" in {
      val example = factory.MultipleRoots
      val confirmers = Set(submitter, signatory)
      val left = withExternalCallResults(
        withConfirmers(example.rootViews(4), confirmers),
        Seq(externalCallViewResult(exerciseIndex = 0, externalCallResult, Set(submitter))),
      )
      val right = withExternalCallResults(
        withConfirmers(example.rootViews(5), confirmers),
        // Same semantic key, different output, seen by a disjoint checking party.
        Seq(externalCallViewResult(exerciseIndex = 1, otherExternalCallOutput, Set(signatory))),
      )
      val leftViewHash = left.viewHash
      val disagreement = DAMLe.ExternalCallRecordedResultDisagreement(
        key = externalCallKey,
        outputs = Set(externalCallResult.output, otherExternalCallOutput.output),
      )

      // No single hosted party sees both outputs, so the hosted consistency check finds nothing --
      // but the disagreement is still globally visible and therefore alarmed.
      val result = assertRecordedDisagreementAlarms() {
        runCheck(
          externalCallRouter(),
          Seq(leftViewPosition -> left, rightViewPosition -> right),
          conformanceErrors = Seq(ModelConformanceChecker.DAMLeError(disagreement, leftViewHash)),
        )
      }

      result.recordedConsistency.hostedInconsistencies shouldBe empty
      result.recordedConsistency.visibleInconsistencies should have size 1

      // The replay disagreement routes to each checking party for the occurrence they can see.
      result.replayDisagreementInconsistencies.keySet shouldBe Set(submitter, signatory)
      result
        .replayDisagreementInconsistencies(submitter)
        .loneElement
        ._2
        .occurrences
        .map(_.viewPosition) shouldBe Set(leftViewPosition)
      result
        .replayDisagreementInconsistencies(signatory)
        .loneElement
        ._2
        .occurrences
        .map(_.viewPosition) shouldBe Set(rightViewPosition)

      // Integrated per-view routing: left -> submitter, right -> signatory.
      result
        .inconsistenciesForView(leftViewPosition, confirmers)
        .map(_._1) shouldBe Seq(submitter)
      result
        .inconsistenciesForView(rightViewPosition, confirmers)
        .map(_._1) shouldBe Seq(signatory)
    }
  }
}
