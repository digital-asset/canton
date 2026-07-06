// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.{TransactionView, ViewPosition}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.ExampleTransactionFactory.{signatory, submitter}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext, LfPartyId}
import com.digitalasset.daml.lf.data.Bytes
import com.digitalasset.daml.lf.transaction.ExternalCallResult

/** Direct unit tests for [[ExternalCallResponseRouter]] and its routing collaborators
  * ([[ExternalCallRoutingContext]], [[ExternalCallValidationRoutes]]).
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

  protected val factory =
    new ExampleTransactionFactory(versionOverride = Some(ProtocolVersion.dev))()

  private def router(
      externalCallValidator: ExternalCallValidator = matchingExternalCallValidator
  ): ExternalCallResponseRouter =
    new ExternalCallResponseRouter(
      externalCallValidator,
      PositiveInt.tryCreate(8),
      loggerFactory,
    )

  private val externalCallKey = DAMLe.ExternalCallKey.fromResult(externalCallResult)

  private def hostedView(
      viewPosition: ViewPosition,
      view: TransactionView,
      hostedConfirmingParties: Set[LfPartyId],
  ): ViewWithHostedParties =
    ViewWithHostedParties(viewPosition, validationResult(view), hostedConfirmingParties)

  private def routingContext(
      viewsWithHostedParties: Seq[ViewWithHostedParties],
      recordedDisagreements: Seq[DAMLe.ExternalCallRecordedResultDisagreement] = Seq.empty,
  ): ExternalCallRoutingContext =
    new ExternalCallRoutingContext(
      recordedDisagreements,
      viewsWithHostedParties,
      viewsWithHostedParties.map(view => view.viewPosition -> view.validationResult).toMap,
    )

  /** Mirrors the factory's `approvingViewPositions`: every hosted confirming party approves. */
  private def approvingAll(
      viewsWithHostedParties: Seq[ViewWithHostedParties]
  ): Map[ViewPosition, Set[LfPartyId]] =
    viewsWithHostedParties.map(view => view.viewPosition -> view.hostedConfirmingParties).toMap

  /** Two views recording the same external call with disagreeing outputs, both seen by `submitter`.
    */
  private def conflictingViews(
      hostedConfirmingParties: Set[LfPartyId]
  ): Seq[ViewWithHostedParties] = {
    val example = factory.MultipleRoots
    val confirmers = Set(submitter, signatory)
    val left = withExternalCallResults(
      withConfirmers(example.rootViews(4), confirmers),
      Seq(externalCallViewResult(exerciseIndex = 0, externalCallResult, Set(submitter))),
    )
    val right = withExternalCallResults(
      withConfirmers(example.rootViews(5), confirmers),
      Seq(externalCallViewResult(exerciseIndex = 1, otherExternalCallResult, Set(submitter))),
    )
    Seq(
      hostedView(leftViewPosition, left, hostedConfirmingParties),
      hostedView(rightViewPosition, right, hostedConfirmingParties),
    )
  }

  "ExternalCallResponseRouter local validation" should {
    "reject locally validated output mismatches for otherwise approving hosted checking parties" in {
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
            computedOutput = otherExternalCallResult.output,
            recordedOutput = externalCallResult.output,
          )
        )
      )
      val sut = router(validator)

      val occurrences = sut.validationOccurrences(
        viewsWithHostedParties,
        approvingAll(viewsWithHostedParties),
        routingContext(viewsWithHostedParties),
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
      inconsistency.outputs shouldBe Set(externalCallResult.output, otherExternalCallResult.output)
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
      val sut = router(validator)

      val occurrences = sut.validationOccurrences(
        viewsWithHostedParties,
        approvingAll(viewsWithHostedParties),
        routingContext(viewsWithHostedParties),
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
      val sut = router(validator)

      val occurrences = sut.validationOccurrences(
        viewsWithHostedParties,
        approvingAll(viewsWithHostedParties),
        routingContext(viewsWithHostedParties),
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
            computedOutput = otherExternalCallResult.output,
            recordedOutput = externalCallResult.output,
          )
        )
      )
      val sut = router(validator)

      val occurrences = sut.validationOccurrences(
        viewsWithHostedParties,
        approvingAll(viewsWithHostedParties),
        routingContext(viewsWithHostedParties),
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
            computedOutput = otherExternalCallResult.output,
            recordedOutput = externalCallResult.output,
          )
        )
      )
      val sut = router(validator)

      val occurrences = sut.validationOccurrences(
        viewsWithHostedParties,
        approvingAll(viewsWithHostedParties),
        routingContext(viewsWithHostedParties),
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

    "prefer recorded external-call disagreements over local external-call validation" in {
      val viewsWithHostedParties = conflictingViews(Set(submitter, signatory))
      val routing = routingContext(viewsWithHostedParties)
      val validator = new RecordingExternalCallValidator(
        Map(
          externalCallKey -> ExternalCallValidator.UnableToValidate(
            "extension service is not configured"
          )
        )
      )
      val sut = router(validator)

      // The visible recorded disagreement already rejects the checking party.
      routing.recordedConsistencyResult.hostedInconsistencies.keySet shouldBe Set(submitter)
      routing.recordedConsistencyResult.visibleInconsistencies should have size 1
      routing
        .inconsistenciesForView(leftViewPosition, Set(submitter, signatory))
        .map(_._1) shouldBe Seq(submitter)

      // Hence the locally-responsible party is already rejected and produces no validation work...
      val occurrences = sut.validationOccurrences(
        viewsWithHostedParties,
        approvingAll(viewsWithHostedParties),
        routing,
      )
      occurrences shouldBe empty

      val routes = sut.validateExternalCalls(occurrences).futureValueUS
      // ...so the local external-call validator is never invoked.
      validator.observed shouldBe empty
      routes.rejects shouldBe empty
      routes.abstains shouldBe empty

      // The visible disagreement is still alarmed.
      assertRecordedDisagreementAlarms() {
        sut.reportVisibleRecordedDisagreementAlarms(requestId, routing.recordedConsistencyResult)
      }
    }
  }

  "ExternalCallResponseRouter recorded-disagreement routing" should {
    "not attribute external-call disagreements to unrelated hosted views" in {
      val example = factory.MultipleRoots
      val confirmers = Set(submitter, signatory)
      val unrelatedView = withConfirmers(example.rootViews(3), confirmers)
      val viewsWithHostedParties =
        conflictingViews(confirmers) :+
          hostedView(unrelatedViewPosition, unrelatedView, confirmers)
      val routing = routingContext(viewsWithHostedParties)

      // The conflicting views attribute the disagreement to the checking party.
      routing
        .inconsistenciesForView(leftViewPosition, confirmers)
        .map(_._1) shouldBe Seq(submitter)
      // The unrelated view (no external call results) is not attributed any disagreement.
      routing.inconsistenciesForView(unrelatedViewPosition, confirmers) shouldBe empty
    }

    "emit external-call disagreement responses for every affected view" in {
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

      val viewsWithHostedParties = Seq(
        hostedView(leftViewPosition, view(example.rootViews(4), firstCall, 0), confirmers),
        hostedView(
          rightViewPosition,
          view(example.rootViews(5), firstCall.copy(output = Bytes.fromStringUtf8("other-a")), 1),
          confirmers,
        ),
        hostedView(unrelatedViewPosition, view(example.rootViews(4), secondCall, 2), confirmers),
        hostedView(
          secondRightViewPosition,
          view(example.rootViews(5), secondCall.copy(output = Bytes.fromStringUtf8("other-b")), 3),
          confirmers,
        ),
      )
      val routing = routingContext(viewsWithHostedParties)

      // One visible disagreement per distinct external call, both routed to the checking party.
      routing.recordedConsistencyResult.visibleInconsistencies should have size 2
      routing.recordedConsistencyResult.hostedInconsistencies.keySet shouldBe Set(submitter)
      routing.recordedConsistencyResult.hostedInconsistencies(submitter) should have size 2

      // Each affected view rejects the checking party with the inconsistency for its own call.
      val expectedKeyByView = Map(
        leftViewPosition -> firstKey,
        rightViewPosition -> firstKey,
        unrelatedViewPosition -> secondKey,
        secondRightViewPosition -> secondKey,
      )
      expectedKeyByView.foreach { case (viewPosition, expectedKey) =>
        val routed = routing.inconsistenciesForView(viewPosition, confirmers)
        routed.map(_._1) shouldBe Seq(submitter)
        routed.loneElement._2.key shouldBe expectedKey
      }

      assertRecordedDisagreementAlarms(count = 2) {
        router().reportVisibleRecordedDisagreementAlarms(
          requestId,
          routing.recordedConsistencyResult,
        )
      }
    }

    "route recorded external-call result disagreements by checking party" in {
      val confirmers = Set(submitter, signatory)
      val viewsWithHostedParties = conflictingViews(confirmers)
      val leftViewHash =
        viewsWithHostedParties.head.validationResult.view.unwrap.viewHash
      val disagreement = DAMLe.ExternalCallRecordedResultDisagreement(
        key = externalCallKey,
        outputs = Set(externalCallResult.output, otherExternalCallResult.output),
      )
      val routing = routingContext(viewsWithHostedParties, Seq(disagreement))

      // The recorded disagreement is attributed to the checking party only.
      routing
        .inconsistenciesForView(leftViewPosition, confirmers)
        .map(_._1) shouldBe Seq(submitter)

      // A model-conformance error carrying this disagreement is routable, so the factory
      // suppresses the would-be malformed model-conformance reject.
      val routableError = ModelConformanceChecker.DAMLeError(disagreement, leftViewHash)
      routing.isRoutableModelConformanceError(routableError) shouldBe true

      // An unrelated disagreement is not routable.
      val unrelatedDisagreement = DAMLe.ExternalCallRecordedResultDisagreement(
        key = DAMLe.ExternalCallKey.fromResult(externalCallResult.copy(functionId = "unrelated")),
        outputs = Set(externalCallResult.output, otherExternalCallResult.output),
      )
      routing.isRoutableModelConformanceError(
        ModelConformanceChecker.DAMLeError(unrelatedDisagreement, leftViewHash)
      ) shouldBe false
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
        Seq(externalCallViewResult(exerciseIndex = 1, otherExternalCallResult, Set(signatory))),
      )
      val viewsWithHostedParties = Seq(
        hostedView(leftViewPosition, left, confirmers),
        hostedView(rightViewPosition, right, confirmers),
      )
      val disagreement = DAMLe.ExternalCallRecordedResultDisagreement(
        key = externalCallKey,
        outputs = Set(externalCallResult.output, otherExternalCallResult.output),
      )
      val routing = routingContext(viewsWithHostedParties, Seq(disagreement))

      // No single hosted party sees both outputs, so the visible (hosted) consistency check finds
      // nothing -- but the disagreement is still globally visible and therefore alarmed.
      routing.recordedConsistencyResult.hostedInconsistencies shouldBe empty
      routing.recordedConsistencyResult.visibleInconsistencies should have size 1

      // The replay disagreement routes to each checking party for the occurrence they can see.
      val routed = ExternalCallResponseRouter.recordedExternalCallDisagreementInconsistencies(
        Seq(disagreement),
        viewsWithHostedParties,
      )
      routed.keySet shouldBe Set(submitter, signatory)
      routed(submitter).loneElement._2.occurrences.map(_.viewPosition) shouldBe Set(
        leftViewPosition
      )
      routed(signatory).loneElement._2.occurrences.map(_.viewPosition) shouldBe Set(
        rightViewPosition
      )

      // Integrated per-view routing: left -> submitter, right -> signatory.
      routing
        .inconsistenciesForView(leftViewPosition, confirmers)
        .map(_._1) shouldBe Seq(submitter)
      routing
        .inconsistenciesForView(rightViewPosition, confirmers)
        .map(_._1) shouldBe Seq(signatory)
    }
  }
}
