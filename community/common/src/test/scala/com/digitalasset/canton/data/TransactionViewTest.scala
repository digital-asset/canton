// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{HashOps, Salt, TestSalt}
import com.digitalasset.canton.data.ViewParticipantData.InvalidViewParticipantData
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.v30.ActionDescription.FetchActionDescription
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  LfPackageId,
  LfPartyId,
  LfVersioned,
  ProtoDeserializationError,
  ProtocolVersionChecksAnyWordSpec,
}
import com.digitalasset.daml.lf.data.Bytes
import com.digitalasset.daml.lf.transaction.ExternalCallResult
import com.digitalasset.daml.lf.value.Value.VersionedValue
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.ListSet

class TransactionViewTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAnyWordSpec {

  private val factory = new ExampleTransactionFactory()()

  private val hashOps: HashOps = factory.cryptoOps

  private val contractArg: VersionedValue = ExampleTransactionFactory.defaultVersionedValue

  private val cantonContractIdVersion: CantonContractIdV1Version = CantonContractIdVersion.maxV1
  private val createdId: LfContractId =
    cantonContractIdVersion.fromDiscriminator(
      ExampleTransactionFactory.lfHash(3),
      ExampleTransactionFactory.unicum(0),
    )
  private val absoluteId: LfContractId = ExampleTransactionFactory.suffixedId(0, 0)
  private val otherAbsoluteId: LfContractId = ExampleTransactionFactory.suffixedId(1, 1)
  private val salt: Salt = factory.transactionSalt
  private val nodeSeed: LfHash = ExampleTransactionFactory.lfHash(1)

  private val defaultPackagePreference = Set(ExampleTransactionFactory.packageId)
  private val externalCallResult: ExternalCallResult =
    ExternalCallResult(
      extensionId = "extension",
      functionId = "function",
      config = Bytes.fromStringUtf8("config"),
      input = Bytes.fromStringUtf8("input"),
      output = Bytes.fromStringUtf8("output"),
    )
  private val externalCallCheckingParties =
    Set(ExampleTransactionFactory.submitter, ExampleTransactionFactory.signatory)

  private def viewExternalCallResult(
      exerciseIndex: Int,
      result: ExternalCallResult = externalCallResult,
      callIndex: Int = 0,
      checkingParties: Set[LfPartyId] = externalCallCheckingParties,
  ): ViewParticipantData.ViewExternalCallResult =
    ViewParticipantData.ViewExternalCallResult(
      result,
      NonNegativeInt.tryCreate(exerciseIndex),
      NonNegativeInt.tryCreate(callIndex),
      checkingParties,
    )

  private def externalCallResultProto(
      exerciseIndex: Int = 7,
      callIndex: Int = 1,
  ): v32.ViewExternalCallResult =
    v32.ViewExternalCallResult(
      extensionId = externalCallResult.extensionId,
      functionId = externalCallResult.functionId,
      config = externalCallResult.config.toByteString,
      input = externalCallResult.input.toByteString,
      output = externalCallResult.output.toByteString,
      exerciseIndex = exerciseIndex,
      callIndex = callIndex,
      checkingParties = externalCallCheckingParties.toSeq.sorted,
    )

  /** Traverses all unblinded subviews `v1, v2, v3, ...` in pre-order and yields `f(...f(f(z, v1), *
    * v2)..., vn)`
    */
  private def viewsInPreOrder(main: TransactionView): Seq[TransactionView] = {
    val builder = Seq.newBuilder[TransactionView]
    def go(view: TransactionView): Unit = {
      builder += view
      view.subviews.unblindedElements.foreach(go)
    }
    go(main)
    builder.result()
  }

  private val defaultActionDescription: ActionDescription =
    ActionDescription.tryFromLfActionNode(
      ExampleTransactionFactory.createNode(createdId, contractArg),
      Some(ExampleTransactionFactory.lfHash(5)),
      defaultPackagePreference,
    )

  private val exerciseActionDescription: ActionDescription =
    ActionDescription.tryFromLfActionNode(
      ExampleTransactionFactory.exerciseNodeWithoutChildren(absoluteId),
      Some(nodeSeed),
      defaultPackagePreference,
    )

  private def exerciseCoreInputs: Map[LfContractId, GenContractInstance] =
    Map(absoluteId -> ExampleContractFactory.build(overrideContractId = Some(absoluteId)))

  forEvery(factory.standardHappyCases) { example =>
    s"The views of $example" when {

      forEvery(example.viewWithSubviews.zipWithIndex) { case ((view, subviews), index) =>
        s"processing $index-th view" can {
          "be folded" in {
            viewsInPreOrder(view) should equal(subviews)
          }

          "be flattened" in {
            view.flatten should equal(subviews)
          }
        }
      }
    }
  }

  "A view" when {
    val firstSubviewIndex = TransactionSubviews.indices(1).head.toString

    "a child view has the same view common data" must {
      val view = factory.SingleExercise(seed = ExampleTransactionFactory.lfHash(3)).view0
      val subViews = TransactionSubviews(Seq(view))(testedProtocolVersion, factory.cryptoOps)
      "reject creation" in {
        TransactionView.create(hashOps)(
          view.viewCommonData,
          view.viewParticipantData,
          subViews,
          testedProtocolVersion,
        ) shouldEqual Left(
          s"The subview with index $firstSubviewIndex has equal viewCommonData to a parent."
        )
      }
    }

    "a child view has package preferences not in the parent" must {

      val unexpectedPackage = LfPackageId.assertFromString("u1")
      val view = factory.SingleExercise(seed = ExampleTransactionFactory.lfHash(3)).view0

      "reject creation if child exercise based view is different from its parent" in {

        val subview =
          TransactionView.Optics.viewParticipantDataUnsafe
            .modify { vpd =>
              val actionDescription = vpd.tryUnwrap.actionDescription.toProtoV30
              actionDescription.getExercise.withPackagePreference(Seq(unexpectedPackage))
              val exercise = actionDescription.withExercise(
                actionDescription.getExercise.withPackagePreference(Seq(unexpectedPackage))
              )
              vpd.tryUnwrap.copy(actionDescription = ActionDescription.fromProtoV30(exercise).value)
            }(view)

        val subViews = TransactionSubviews(Seq(subview))(testedProtocolVersion, factory.cryptoOps)

        TransactionView
          .create(hashOps)(
            view.viewCommonData,
            view.viewParticipantData,
            subViews,
            testedProtocolVersion,
          )
          .left
          .value shouldBe s"Detected unexpected exercise package preference: $unexpectedPackage at $firstSubviewIndex"
      }

      "reject creation if child fetch based view is different from its parent" in {

        val subview =
          TransactionView.Optics.viewParticipantDataUnsafe
            .modify { vpd =>
              val actionDescription = vpd.tryUnwrap.actionDescription.toProtoV30
              val ex = actionDescription.getExercise
              val fetch = actionDescription.withFetch(
                FetchActionDescription(
                  inputContractId = ex.inputContractId,
                  actors = ex.actors,
                  byKey = false,
                  templateId = s"$unexpectedPackage:module:template",
                  interfaceId = Some("ifPkg:module:template"),
                )
              )
              vpd.tryUnwrap.copy(actionDescription = ActionDescription.fromProtoV30(fetch).value)
            }(view)

        val subViews = TransactionSubviews(Seq(subview))(testedProtocolVersion, factory.cryptoOps)

        TransactionView
          .create(hashOps)(
            view.viewCommonData,
            view.viewParticipantData,
            subViews,
            testedProtocolVersion,
          )
          .left
          .value shouldBe s"Detected unexpected fetch package preference: $unexpectedPackage at $firstSubviewIndex"
      }

    }

    s"has resolved keys with PV$testedProtocolVersion" must {

      def keyTest(
          parentCoreInput: Option[GenContractInstance] = None,
          childCoreInput: Option[GenContractInstance] = None,
          resolution: Option[Map[LfGlobalKey, LfVersioned[
            KeyResolutionWithMaintainers
          ]]] = None,
      ): Either[String, Unit] = {

        val keyResolution = resolution.getOrElse(
          Seq(parentCoreInput, childCoreInput).flatten
            .groupBy(_.contractKeyWithMaintainers.value)
            .map { case (k, c) =>
              k.globalKey ->
                LfVersioned(
                  c.map(_.inst.version).toSet.loneElement,
                  KeyResolutionWithMaintainers(
                    contracts = c.map(_.contractId),
                    maintainers = k.maintainers,
                  ),
                )
            }
        )

        val base = factory
          .SingleExercise(seed = ExampleTransactionFactory.lfHash(3))
          .view0

        def addViewContracts(
            coreInputO: Option[GenContractInstance]
        ): TransactionView => TransactionView =
          TransactionView.Optics.viewParticipantDataUnsafe
            .andThen(MerkleTree.Optics.unblinded[ViewParticipantData])
            .andThen(ViewParticipantData.Optics.coreInputsUnsafe)
            .modify(coreInputs =>
              coreInputs ++ coreInputO.toList.map { coreInput =>
                coreInput.contractId -> InputContract(
                  coreInput,
                  consumed = false,
                )
              }.toMap
            )

        val addKeyResolution: TransactionView => TransactionView =
          TransactionView.Optics.viewParticipantDataUnsafe
            .andThen(MerkleTree.Optics.unblinded[ViewParticipantData])
            .andThen(ViewParticipantData.Optics.keyResolutionUnsafe)
            .replace(keyResolution)

        val parent = (addViewContracts(parentCoreInput) andThen addKeyResolution)(base)

        val childBase = TransactionView.Optics.viewCommonDataUnsafe
          .andThen(MerkleTree.Optics.unblinded[ViewCommonData])
          .modify(_.copy(salt = TestSalt.generateSalt(1)))
          .apply(base)

        val child =
          addViewContracts(childCoreInput)(childBase)

        val subViews = TransactionSubviews(Seq(child))(
          testedProtocolVersion,
          factory.cryptoOps,
        )

        TransactionView
          .create(hashOps)(
            parent.viewCommonData,
            parent.viewParticipantData,
            subViews,
            testedProtocolVersion,
          )
          .map(_ => ())

      }

      // Will be enabled from ProtocolVersion.v36
      if (testedProtocolVersion >= ProtocolVersion.v36) {

        "allow the parent view to reference input contracts from this view or any subview - same key" in {

          val key: Option[LfGlobalKeyWithMaintainers] =
            Some(ExampleContractFactory.buildKeyWithMaintainers())

          val parentCoreInput = ExampleContractFactory.build(keyOpt = key)
          val childCoreInput = ExampleContractFactory.build(keyOpt = key)

          keyTest(
            Some(parentCoreInput),
            Some(childCoreInput),
          ) shouldBe Either.unit
        }

        "allow the parent view to reference input contracts from this view or any subview - different key" in {

          def key(): Option[LfGlobalKeyWithMaintainers] =
            Some(ExampleContractFactory.buildKeyWithMaintainers())

          val parentCoreInput = ExampleContractFactory.build(keyOpt = key())
          val childCoreInput = ExampleContractFactory.build(keyOpt = key())

          keyTest(
            Some(parentCoreInput),
            Some(childCoreInput),
          ) shouldBe Either.unit

        }

        "disallow the view if a referenced contract is not an input contract" in {

          val expected = ExampleContractFactory.buildContractId()
          val resolution = Map(
            ExampleContractFactory.buildKeyWithMaintainers().globalKey ->
              LfVersioned(
                LfSerializationVersion.V2,
                KeyResolutionWithMaintainers(
                  contracts = Seq(expected),
                  maintainers = Set(ExampleTransactionFactory.signatory),
                ),
              )
          )
          inside(keyTest(resolution = Some(resolution))) { case Left(error) =>
            error should (include regex s"Failed to find key resolution contract.*${expected.coid}")
          }
        }

        "disallow the view if the referenced contract does not have a key" in {

          val parentCoreInput = ExampleContractFactory.build()
          val key = ExampleTransactionFactory.globalKeyWithMaintainers().unversioned.globalKey
          val resolution = Map(
            key ->
              LfVersioned(
                LfSerializationVersion.V2,
                KeyResolutionWithMaintainers(
                  contracts = Seq(parentCoreInput.contractId),
                  maintainers = Set(ExampleTransactionFactory.signatory),
                ),
              )
          )
          inside(keyTest(parentCoreInput = Some(parentCoreInput), resolution = Some(resolution))) {
            case Left(error) =>
              error should (include regex s"Contract ${parentCoreInput.contractId.coid} resolved for key.*does not have a matching key in the contract instance")
          }
        }

        "disallow the view if the referenced contract has a mismatching key" in {

          val resolutionKey = ExampleContractFactory.buildKeyWithMaintainers()
          val contractKey = ExampleContractFactory.buildKeyWithMaintainers()
          val parentCoreInput = ExampleContractFactory.build(keyOpt = Some(contractKey))

          val resolution = Map(
            resolutionKey.globalKey ->
              LfVersioned(
                LfSerializationVersion.V2,
                KeyResolutionWithMaintainers(
                  contracts = Seq(parentCoreInput.contractId),
                  maintainers = Set(ExampleTransactionFactory.signatory),
                ),
              )
          )
          inside(keyTest(parentCoreInput = Some(parentCoreInput), resolution = Some(resolution))) {
            case Left(error) =>
              error should (include regex s"Contract ${parentCoreInput.contractId.coid} resolved for key.*does not have a matching key in the contract instance")
          }
        }

      }
    }

  }

  "A view participant data" when {

    def create(
        actionDescription: ActionDescription = defaultActionDescription,
        consumed: Set[LfContractId] = Set.empty,
        coreInputs: Map[LfContractId, GenContractInstance] = Map.empty,
        createdIds: Seq[LfContractId] = Seq(createdId),
        archivedInSubviews: Set[LfContractId] = Set.empty,
        keyResolution: Map[LfGlobalKey, LfVersioned[KeyResolutionWithMaintainers]] = Map.empty,
        externalCallResults: Seq[ViewParticipantData.ViewExternalCallResult] = Seq.empty,
    ): Either[String, ViewParticipantData] = {

      val created = createdIds.map { id =>
        val contract = ExampleContractFactory.build(overrideContractId = Some(id))
        CreatedContract.tryCreate(contract, consumed.contains(id), rolledBack = false)
      }
      val coreInputs2 = coreInputs.transform { (id, contract) =>
        InputContract(contract, consumed.contains(id))
      }

      ViewParticipantData
        .create(hashOps)(
          coreInputs2,
          created,
          archivedInSubviews,
          keyResolution,
          actionDescription,
          RollbackContextFactory(testedProtocolVersion).empty,
          salt,
          externalCallResults,
          testedProtocolVersion,
        )
        .flatMap { data =>
          // Return error message if root action is not valid
          Either
            .catchOnly[InvalidViewParticipantData](data.rootAction)
            .bimap(ex => ex.message, _ => data)
        }
    }

    "a contract is created twice" must {
      "reject creation" in {
        create(createdIds = Seq(createdId, createdId)).left.value should
          startWith regex "createdCore contains the contract id .* multiple times at indices 0, 1"
      }
    }
    "a structural invariant is violated via the test-only Optics" must {
      // Optics/copy deliberately bypass the invariant checks, so
      // an invalid instance can be constructed without throwing and is rejected only by `validated`.
      "construct without throwing yet be rejected by validated" in {
        val valid = create().value
        val cid = valid.createdCore.loneElement.contract.contractId
        val invalid =
          ViewParticipantData.Optics.createdCoreUnsafe.modify(created => created ++ created)(valid)
        invalid.createdCore should have size 2
        invalid
          .validated(testedProtocolVersion)
          .left
          .value shouldBe s"createdCore contains the contract id $cid multiple times at indices 0, 1"
      }
    }
    "a used contract has an inconsistent id" must {
      "reject creation" in {
        val usedContract = ExampleContractFactory.build(overrideContractId = Some(otherAbsoluteId))
        create(coreInputs = Map(absoluteId -> usedContract)).left.value should startWith(
          "Inconsistent ids for used contract: "
        )
      }
    }
    "an overlap between archivedInSubview and coreCreated" must {
      "reject creation" in {
        create(
          createdIds = Seq(createdId),
          archivedInSubviews = Set(createdId),
        ).left.value should startWith(
          "Contract created in a subview are also created in the core: "
        )
      }
    }
    "an overlap between archivedInSubview and coreInputs" must {
      "reject creation" in {
        val usedContract = ExampleContractFactory.build(overrideContractId = Some(absoluteId))
        create(
          coreInputs = Map(absoluteId -> usedContract),
          archivedInSubviews = Set(absoluteId),
        ).left.value should startWith("Contracts created in a subview overlap with core inputs: ")
      }
    }
    "the created contract of the root action is not declared first" must {
      "reject creation" in {
        create(createdIds = Seq.empty).left.value should startWith(
          "No created core contracts declared for a view that creates contract"
        )
      }
      "reject creation with other contract ids" in {
        val otherCantonId =
          cantonContractIdVersion.fromDiscriminator(
            ExampleTransactionFactory.lfHash(3),
            ExampleTransactionFactory.unicum(1),
          )
        create(createdIds = Seq(otherCantonId, createdId)).left.value should startWith(
          show"View with root action Create $createdId declares $otherCantonId as first created core contract."
        )
      }
    }
    "the used contract of the root action is not declared" must {

      "reject creation with exercise action" in {
        create(
          actionDescription = ActionDescription.tryFromLfActionNode(
            ExampleTransactionFactory.exerciseNodeWithoutChildren(absoluteId),
            Some(nodeSeed),
            defaultPackagePreference,
          )
        ).left.value should startWith(
          show"Input contract $absoluteId of the Exercise root action is not declared as core input."
        )
      }

      "reject creation with fetch action" in {

        create(
          actionDescription = ActionDescription.tryFromLfActionNode(
            ExampleTransactionFactory.fetchNode(
              absoluteId,
              Set(ExampleTransactionFactory.submitter),
            ),
            None,
            defaultPackagePreference,
          )
        ).left.value should startWith(
          show"Input contract $absoluteId of the Fetch root action is not declared as core input."
        )
      }

    }

    "external call results have duplicate occurrence identities" must {
      "reject creation" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
        create(
          actionDescription = exerciseActionDescription,
          coreInputs = exerciseCoreInputs,
          externalCallResults = Seq(
            viewExternalCallResult(exerciseIndex = 7, callIndex = 1),
            viewExternalCallResult(
              exerciseIndex = 7,
              callIndex = 1,
              result = externalCallResult.copy(functionId = "other-function"),
            ),
          ),
        ).left.value shouldBe
          "externalCallResults contains duplicate occurrence (exercise index 7, call index 1)"
      }
    }

    "external call results on a non-exercise root action" must {
      "reject creation" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
        create(
          externalCallResults = Seq(viewExternalCallResult(exerciseIndex = 7))
        ).left.value shouldBe "External call results require an exercise root action"
      }
    }

    "the same external call is recorded with conflicting outputs in one view" must {
      // Distinct occurrences share a semantic key but record different outputs: the participant
      // data is well-formed on its own; the disagreement is caught when the view is validated.
      "reject the view as malformed without leaking the payloads" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
        val vpd = create(
          actionDescription = exerciseActionDescription,
          coreInputs = exerciseCoreInputs,
          externalCallResults = Seq(
            viewExternalCallResult(exerciseIndex = 7, callIndex = 0),
            viewExternalCallResult(
              exerciseIndex = 7,
              callIndex = 1,
              result = externalCallResult.copy(output = Bytes.fromStringUtf8("other-output")),
            ),
          ),
        ).value

        val commonData =
          factory.SingleExercise(seed = ExampleTransactionFactory.lfHash(3)).view0.viewCommonData
        val subviews = TransactionSubviews(Seq.empty)(testedProtocolVersion, factory.cryptoOps)

        val error = TransactionView
          .create(hashOps)(commonData, vpd, subviews, testedProtocolVersion)
          .left
          .value
        error should startWith(
          "externalCallResults records conflicting outputs for the same external call:"
        )
        error should not include externalCallResult.output.toHexString
        error should not include Bytes.fromStringUtf8("other-output").toHexString
        error should not include externalCallResult.config.toHexString
        error should not include externalCallResult.input.toHexString
      }
    }

    "the same external call is recorded with conflicting outputs across a view and its subview" must {
      // The aggregation spans the whole subtree (`flatten`): a key recorded in the parent core
      // and again, differently, in a subview core is a disagreement even though neither view's
      // participant data conflicts on its own.
      "reject the parent view as malformed" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
        val view = factory.SingleExercise(seed = ExampleTransactionFactory.lfHash(3)).view0
        def withCall(output: String): TransactionView =
          TransactionView.Optics.viewParticipantDataUnsafe.modify(vpd =>
            vpd.tryUnwrap.copy(externalCallResults =
              Seq(
                viewExternalCallResult(
                  exerciseIndex = 7,
                  callIndex = 0,
                  result = externalCallResult.copy(output = Bytes.fromStringUtf8(output)),
                )
              )
            )
          )(view)

        val parent = withCall("output")
        val child = withCall("other-output")
        val subviews = TransactionSubviews(Seq(child))(testedProtocolVersion, factory.cryptoOps)

        val error = TransactionView
          .create(hashOps)(
            parent.viewCommonData,
            parent.viewParticipantData,
            subviews,
            testedProtocolVersion,
          )
          .left
          .value
        error should startWith(
          "externalCallResults records conflicting outputs for the same external call:"
        )
        error should not include externalCallResult.output.toHexString
        error should not include Bytes.fromStringUtf8("other-output").toHexString
      }
    }

    "external call results on a non-dev protocol version" must {
      "reject creation" onlyRunWithOrLessThan ProtocolVersion.v35 in {
        create(
          actionDescription = exerciseActionDescription,
          coreInputs = exerciseCoreInputs,
          externalCallResults = Seq(viewExternalCallResult(exerciseIndex = 7)),
        ).left.value shouldBe
          s"External call results are supported only from protocol version ${ProtocolVersion.dev} onwards"
      }
    }

    "deserialized" must {

      "reconstruct unkeyed view participant data" in {

        val usedContract = ExampleContractFactory.build(
          overrideContractId = Some(absoluteId)
        )
        val vpd = create(
          consumed = Set(absoluteId),
          createdIds = Seq(createdId),
          coreInputs = Map(absoluteId -> usedContract),
          archivedInSubviews = Set(otherAbsoluteId),
        ).value

        ViewParticipantData
          .fromByteString(testedProtocolVersion, (hashOps, testedProtocolVersion))(
            vpd.getCryptographicEvidence
          )
          .map(_.unwrap) shouldBe Right(Right(vpd))
      }

      "reconstruct the original keyed view participant data" onlyRunWithOrGreaterThan ProtocolVersion.v36 in {

        val key = ExampleTransactionFactory.globalKeyWithMaintainers()

        val usedContract = ExampleContractFactory.build(
          overrideContractId = Some(absoluteId),
          keyOpt = Some(key.unversioned),
        )
        val vpd = create(
          consumed = Set(absoluteId),
          createdIds = Seq(createdId),
          coreInputs = Map(absoluteId -> usedContract),
          archivedInSubviews = Set(otherAbsoluteId),
          keyResolution = Map(
            ExampleTransactionFactory.defaultGlobalKey ->
              LfVersioned(
                key.version,
                KeyResolutionWithMaintainers(
                  Vector(usedContract.contractId),
                  key.unversioned.maintainers,
                ),
              )
          ),
        ).value

        ViewParticipantData
          .fromByteString(testedProtocolVersion, (hashOps, testedProtocolVersion))(
            vpd.getCryptographicEvidence
          )
          .map(_.unwrap) shouldBe Right(Right(vpd))
      }

      "reconstruct dev external call results" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
        val vpd = create(
          actionDescription = exerciseActionDescription,
          coreInputs = exerciseCoreInputs,
          externalCallResults = Seq(
            viewExternalCallResult(
              exerciseIndex = 7,
              callIndex = 1,
              checkingParties = externalCallCheckingParties,
            )
          ),
        ).value

        ViewParticipantData
          .fromByteString(testedProtocolVersion, (hashOps, testedProtocolVersion))(
            vpd.getCryptographicEvidence
          )
          .map(_.unwrap) shouldBe Right(Right(vpd))
      }

      "reconstruct dev keyed external call results" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
        val key = ExampleTransactionFactory.globalKeyWithMaintainers()

        val usedContract = ExampleContractFactory.build(
          overrideContractId = Some(absoluteId),
          keyOpt = Some(key.unversioned),
        )
        val vpd = create(
          actionDescription = exerciseActionDescription,
          consumed = Set(absoluteId),
          createdIds = Seq(createdId),
          coreInputs = Map(absoluteId -> usedContract),
          archivedInSubviews = Set(otherAbsoluteId),
          keyResolution = Map(
            ExampleTransactionFactory.defaultGlobalKey ->
              LfVersioned(
                key.version,
                KeyResolutionWithMaintainers(
                  Vector(usedContract.contractId),
                  key.unversioned.maintainers,
                ),
              )
          ),
          externalCallResults = Seq(
            viewExternalCallResult(
              exerciseIndex = 7,
              callIndex = 1,
              checkingParties = externalCallCheckingParties,
            )
          ),
        ).value

        ViewParticipantData
          .fromByteString(testedProtocolVersion, (hashOps, testedProtocolVersion))(
            vpd.getCryptographicEvidence
          )
          .map(_.unwrap) shouldBe Right(Right(vpd))
      }

      "serialize external call checking parties canonically" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
        val vpd = create(
          actionDescription = exerciseActionDescription,
          coreInputs = exerciseCoreInputs,
          externalCallResults = Seq(
            viewExternalCallResult(
              exerciseIndex = 7,
              callIndex = 1,
              checkingParties = ListSet(
                ExampleTransactionFactory.submitter,
                ExampleTransactionFactory.signatory,
              ),
            )
          ),
        ).value

        val reorderedVpd = vpd.copy(
          externalCallResults = Seq(
            viewExternalCallResult(
              exerciseIndex = 7,
              callIndex = 1,
              checkingParties = ListSet(
                ExampleTransactionFactory.signatory,
                ExampleTransactionFactory.submitter,
              ),
            )
          )
        )

        vpd.getCryptographicEvidence shouldBe reorderedVpd.getCryptographicEvidence
      }

      "reject an external call result with negative exercise_index" in {
        ViewParticipantData.ViewExternalCallResult
          .fromProtoV32(externalCallResultProto(exerciseIndex = -1))
          .left
          .value should matchPattern {
          case ProtoDeserializationError.InvariantViolation(Some("exercise_index"), _) =>
        }
      }

      "reject an external call result with negative call_index" in {
        ViewParticipantData.ViewExternalCallResult
          .fromProtoV32(externalCallResultProto(callIndex = -1))
          .left
          .value should matchPattern {
          case ProtoDeserializationError.InvariantViolation(Some("call_index"), _) =>
        }
      }
    }
  }
}
