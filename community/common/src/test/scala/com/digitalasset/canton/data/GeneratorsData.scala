// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.functor.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{Salt, TestHash}
import com.digitalasset.canton.data.ActionDescription.{
  CreateActionDescription,
  ExerciseActionDescription,
  FetchActionDescription,
}
import com.digitalasset.canton.data.MerkleTree.VersionedMerkleTree
import com.digitalasset.canton.data.ViewPosition.{MerklePathElement, MerkleSeqIndex}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.{GeneratorsTopology, ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.collection.SeqUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{GeneratorsLf, LfInterfaceId, LfPackageId, LfPartyId, LfVersioned}
import com.digitalasset.daml.lf.data.Bytes
import com.digitalasset.daml.lf.transaction.{CreationTime, ExternalCallResult}
import com.digitalasset.daml.lf.value.Value.ValueInt64
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.EitherValues.*

import scala.util.Random

final class GeneratorsData(
    protocolVersion: ProtocolVersion,
    generatorsLf: GeneratorsLf,
    generatorsProtocol: GeneratorsProtocol,
    generatorsTopology: GeneratorsTopology,
) {
  import com.digitalasset.canton.Generators.*
  import generatorsLf.*
  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.data.GeneratorsDataTime.*
  import com.digitalasset.canton.ledger.api.GeneratorsApi.*
  import generatorsTopology.*
  import generatorsProtocol.*

  // If this pattern match is not exhaustive anymore, update the generator below
  {
    ((_: MerklePathElement) match {
      case _: ViewPosition.MerkleSeqIndex => ()
      case _: ViewPosition.MerkleSeqIndexFromRoot =>
        () // This one is excluded because it is not made to be serialized
    }).discard
  }
  implicit val merklePathElementArg: Arbitrary[MerklePathElement] = Arbitrary(
    Arbitrary.arbitrary[MerkleSeqIndex]
  )

  implicit val viewPositionArb: Arbitrary[ViewPosition] = Arbitrary(
    boundedListGen[MerklePathElement].map(ViewPosition(_))
  )

  implicit val commonMetadataArb: Arbitrary[CommonMetadata] = Arbitrary(
    for {
      psid <- Arbitrary.arbitrary[PhysicalSynchronizerId]

      mediator <- Arbitrary.arbitrary[MediatorGroupRecipient]

      salt <- Arbitrary.arbitrary[Salt]
      uuid <- Gen.uuid

      hashOps = TestHash // Not used for serialization
    } yield CommonMetadata
      .create(hashOps)(
        psid,
        mediator,
        salt,
        uuid,
      )
  )

  implicit val participantMetadataArb: Arbitrary[ParticipantMetadata] = Arbitrary(
    for {
      ledgerTime <- Arbitrary.arbitrary[CantonTimestamp]
      preparationTime <- Arbitrary.arbitrary[CantonTimestamp]
      workflowIdO <- Gen.option(workflowIdArb.arbitrary)
      salt <- Arbitrary.arbitrary[Salt]

      hashOps = TestHash // Not used for serialization
    } yield ParticipantMetadata(hashOps)(
      ledgerTime,
      preparationTime,
      workflowIdO,
      salt,
      protocolVersion,
    )
  )

  implicit val submitterMetadataArb: Arbitrary[SubmitterMetadata] = Arbitrary(
    for {
      actAs <- nonEmptySet(lfPartyIdArb).arbitrary
      userId <- userIdArb.arbitrary
      commandId <- commandIdArb.arbitrary
      submittingParticipant <- Arbitrary.arbitrary[ParticipantId]
      salt <- Arbitrary.arbitrary[Salt]
      submissionId <- Gen.option(ledgerSubmissionIdArb.arbitrary)
      dedupPeriod <- Arbitrary.arbitrary[DeduplicationPeriod]
      maxSequencingTime <- Arbitrary.arbitrary[CantonTimestamp]
      externalAuthorization <- Gen.option(Arbitrary.arbitrary[ExternalAuthorization])
    } yield SubmitterMetadata(
      actAs,
      userId,
      commandId,
      submittingParticipant,
      salt,
      submissionId,
      dedupPeriod,
      maxSequencingTime,
      externalAuthorization,
      hashOps = TestHash, // Not used for serialization
      protocolVersion,
    )
  )

  implicit val viewConfirmationParametersArb: Arbitrary[ViewConfirmationParameters] = Arbitrary(
    for {
      informees <- boundedSetGen[LfPartyId]
      viewConfirmationParameters <- boundedListGen(quorumArb(informees.toSeq).arbitrary)
        .map(ViewConfirmationParameters.tryCreate(informees, _))
    } yield viewConfirmationParameters
  )

  def quorumArb(informees: Seq[LfPartyId]): Arbitrary[Quorum] = Arbitrary(
    for {
      confirmersWeights <- Gen
        .containerOfN[Seq, PositiveInt](informees.size, Arbitrary.arbitrary[PositiveInt])

      random = new Random()
      shuffledInformees = SeqUtil.randomSubsetShuffle(
        informees.toIndexedSeq,
        informees.size,
        random,
      )

      confirmers = shuffledInformees.zip(confirmersWeights).toMap
      threshold <- Arbitrary.arbitrary[NonNegativeInt]
    } yield Quorum(confirmers, threshold)
  )

  implicit val viewCommonDataArb: Arbitrary[ViewCommonData] = Arbitrary(
    for {
      viewConfirmationParameters <- Arbitrary.arbitrary[ViewConfirmationParameters]
      salt <- Arbitrary.arbitrary[Salt]
      hashOps = TestHash // Not used for serialization
    } yield ViewCommonData.tryCreate(hashOps)(
      viewConfirmationParameters,
      salt,
      protocolVersion,
    )
  )

  private val viewExternalCallResultsGen: Gen[Seq[ViewParticipantData.ViewExternalCallResult]] =
    if (protocolVersion >= ProtocolVersion.dev) {
      boundedListGen {
        for {
          extensionId <- Gen.identifier
          functionId <- Gen.identifier
          config <- Gen.alphaStr
          input <- Gen.alphaStr
          output <- Gen.alphaStr
          exerciseIndex <- Gen.choose(1, 10).map(NonNegativeInt.tryCreate)
          checkingParties <- boundedSetGen[LfPartyId]
        } yield (
          ExternalCallResult(
            extensionId = extensionId,
            functionId = functionId,
            config = Bytes.fromStringUtf8(config),
            input = Bytes.fromStringUtf8(input),
            output = Bytes.fromStringUtf8(output),
          ),
          exerciseIndex,
          checkingParties,
        )
      }.map(results =>
        results.zipWithIndex.map { case ((result, exerciseIndex, checkingParties), callIndex) =>
          ViewParticipantData.ViewExternalCallResult(
            result = result,
            exerciseIndex = exerciseIndex,
            callIndex = NonNegativeInt.tryCreate(callIndex),
            checkingParties = checkingParties,
          )
        }
      )
    } else Gen.const(Seq.empty)

  val createViewParticipantDataArb: Arbitrary[ViewParticipantData] = Arbitrary(
    for {
      seed <- Arbitrary.arbitrary[LfHash]
      rollbackContext <- rollbackContextArb.arbitrary
      contract <- generatorsProtocol
        .contractInstanceArb(genTime = Arbitrary.arbitrary[CreationTime.CreatedAt])
        .arbitrary
      salt <- Arbitrary.arbitrary[Salt]
      createdContractRolledBack <- createdContractRolledBackArb.arbitrary
      createdContract = CreatedContract.tryCreate(
        contract,
        consumedInCore = false,
        rolledBack = createdContractRolledBack,
      )
      actionDescription = CreateActionDescription(contract.contractId, seed)
    } yield ViewParticipantData.tryCreate(TestHash)(
      coreInputs = Map.empty,
      createdCore = Seq(createdContract),
      createdInSubviewArchivedInCore = Set.empty,
      keyResolution = Map.empty,
      actionDescription = actionDescription,
      rollbackContext = rollbackContext,
      salt = salt,
      externalCallResults = Seq.empty,
      protocolVersion = protocolVersion,
    )
  )

  val exerciseViewParticipantDataArb: Arbitrary[ViewParticipantData] = Arbitrary(
    for {

      templateId <- Arbitrary.arbitrary[LfTemplateId]

      choice <- Arbitrary.arbitrary[LfChoiceName]

      interfaceId <- Gen.option(Arbitrary.arbitrary[LfInterfaceId])

      packagePreference <- boundedSetGen[LfPackageId]

      // We consider only this specific value because the goal is not exhaustive testing of LF (de)serialization
      chosenValue <- Gen.long.map(ValueInt64.apply)

      version <- Arbitrary.arbitrary[LfSerializationVersion]

      actors <- boundedSetGen[LfPartyId]
      seed <- Arbitrary.arbitrary[LfHash]
      targetContract <- generatorsProtocol
        .contractInstanceArb(genTime = Arbitrary.arbitrary[CreationTime.CreatedAt])
        .arbitrary

      byKey <-
        if (targetContract.contractKeyWithMaintainers.isDefined) byKeyArb.arbitrary
        else Gen.const(false)

      otherContracts <- boundedListGen(
        generatorsProtocol
          .contractInstanceArb(genTime = Arbitrary.arbitrary[CreationTime.CreatedAt])
          .arbitrary
      )

      inputContracts = targetContract :: otherContracts

      coreInputs <- Gen
        .listOfN(inputContracts.size, Gen.oneOf(true, false))
        .map(consumed => inputContracts.zip(consumed).map(InputContract.apply tupled))

      createdCore <-
        boundedListGen(
          Gen.zip(
            generatorsProtocol
              .contractInstanceArb(genTime = Arbitrary.arbitrary[CreationTime.CreatedAt])
              .arbitrary,
            Gen.oneOf(true, false),
            createdContractRolledBackArb.arbitrary,
          )
        )
          .map(_.map(CreatedContract.tryCreate tupled))

      createdInSubviewArchivedInCore <- boundedSetGen[LfContractId]

      keyResolution <-
        if (protocolVersion >= ProtocolVersion.v35) {
          val keyedInputs =
            coreInputs.map(c => (c.contract.contractKeyWithMaintainers, c)).collect {
              case (Some(key), contract) => key -> contract
            }
          Gen.someOf(keyedInputs).map {
            _.groupMapReduce(_._1.globalKey) { case (k, c) =>
              LfVersioned(
                c.contract.inst.version,
                KeyResolutionWithMaintainers(Vector(c.contractId), k.maintainers),
              )
            }((kr1, kr2) =>
              LfVersioned(
                kr1.version,
                kr1.unversioned
                  .copy(contracts = kr1.unversioned.contracts ++ kr2.unversioned.contracts),
              )
            )
          }
        } else Gen.const(Map.empty[LfGlobalKey, LfVersioned[KeyResolutionWithMaintainers]])

      rollbackContext <- rollbackContextArb.arbitrary
      salt <- Arbitrary.arbitrary[Salt]
      externalCallResults <- viewExternalCallResultsGen
      failed <- Gen.oneOf(true, false)

      actionDescription = ExerciseActionDescription.tryCreate(
        targetContract.contractId,
        templateId,
        choice,
        interfaceId,
        packagePreference,
        LfVersioned(version, chosenValue),
        actors,
        byKey,
        seed,
        failed,
      )

    } yield ViewParticipantData.tryCreate(TestHash)(
      coreInputs = coreInputs.map(contract => (contract.contractId, contract)).toMap,
      createdCore = createdCore,
      createdInSubviewArchivedInCore = createdInSubviewArchivedInCore,
      keyResolution = keyResolution,
      actionDescription = actionDescription,
      rollbackContext = rollbackContext,
      salt = salt,
      externalCallResults = externalCallResults,
      protocolVersion = protocolVersion,
    )
  )

  val fetchViewParticipantDataArb: Arbitrary[ViewParticipantData] = Arbitrary(
    for {
      contract <- generatorsProtocol
        .contractInstanceArb(genTime = Arbitrary.arbitrary[CreationTime.CreatedAt])
        .arbitrary
      actors <- boundedSetGen[LfPartyId]
      byKey <-
        if (contract.contractKeyWithMaintainers.isDefined) byKeyArb.arbitrary else Gen.const(false)
      templateId <- Arbitrary.arbitrary[LfTemplateId]
      interfaceId <- Gen.option(Arbitrary.arbitrary[LfInterfaceId])
      rollbackContext <- rollbackContextArb.arbitrary
      salt <- Arbitrary.arbitrary[Salt]

      keyResolution = contract.contractKeyWithMaintainers match {
        case Some(key) if byKey =>
          Map(
            key.globalKey -> LfVersioned(
              contract.inst.version,
              KeyResolutionWithMaintainers(Vector(contract.contractId), key.maintainers),
            )
          )
        case _ => Map.empty[LfGlobalKey, LfVersioned[KeyResolutionWithMaintainers]]
      }

      actionDescription = FetchActionDescription(
        contract.contractId,
        actors,
        byKey,
        templateId,
        interfaceId,
      )

    } yield ViewParticipantData.tryCreate(TestHash)(
      coreInputs = Map(contract.contractId -> InputContract(contract, consumed = false)),
      createdCore = Seq.empty,
      createdInSubviewArchivedInCore = Set.empty,
      keyResolution = keyResolution,
      actionDescription = actionDescription,
      rollbackContext = rollbackContext,
      salt = salt,
      externalCallResults = Seq.empty,
      protocolVersion = protocolVersion,
    )
  )

  implicit val viewParticipantDataArb: Arbitrary[ViewParticipantData] = Arbitrary {
    Gen.oneOf(
      createViewParticipantDataArb.arbitrary,
      exerciseViewParticipantDataArb.arbitrary,
      fetchViewParticipantDataArb.arbitrary,
    )
  }

  // If this pattern match is not exhaustive anymore, update the generator below
  {
    ((_: ViewType) match {
      case ViewType.TransactionViewType => ()
      case _: ViewType.ReassignmentViewType => ()
      case _: ViewTypeTest => () // Only for tests, so we don't use it in the generator
    }).discard
  }
  implicit val viewTypeArb: Arbitrary[ViewType] = Arbitrary(
    Gen.oneOf[ViewType](
      ViewType.TransactionViewType,
      ViewType.AssignmentViewType,
      ViewType.UnassignmentViewType,
    )
  )

  private val transactionViewWithEmptyTransactionSubviewArb: Arbitrary[TransactionView] = Arbitrary(
    for {
      viewCommonData <- viewCommonDataArb.arbitrary
      viewParticipantData <- viewParticipantDataArb.arbitrary
      hashOps = TestHash
      emptySubviews = TransactionSubviews.empty(
        protocolVersion,
        hashOps,
      ) // empty TransactionSubviews
    } yield TransactionView.tryCreate(hashOps)(
      viewCommonData = viewCommonData,
      viewParticipantData =
        viewParticipantData.blindFully, // The view participant data in an informee tree must be blinded
      subviews = emptySubviews,
      protocolVersion,
    )
  )

  implicit val transactionViewArb: Arbitrary[TransactionView] = Arbitrary(
    for {
      viewCommonData <- viewCommonDataArb.arbitrary
      viewParticipantData <- viewParticipantDataArb.arbitrary
      hashOps = TestHash
      transactionViewWithEmptySubview <-
        transactionViewWithEmptyTransactionSubviewArb.arbitrary
      subviews = TransactionSubviews
        .apply(Seq(transactionViewWithEmptySubview))(protocolVersion, hashOps)
    } yield TransactionView.tryCreate(hashOps)(
      viewCommonData = viewCommonData,
      viewParticipantData = viewParticipantData,
      subviews = subviews,
      protocolVersion,
    )
  )

  private val transactionViewForInformeeTreeArb: Arbitrary[TransactionView] = Arbitrary(
    for {
      viewCommonData <- viewCommonDataArb.arbitrary
      viewParticipantData <- viewParticipantDataArb.arbitrary
      hashOps = TestHash
      transactionViewWithEmptySubview <-
        transactionViewWithEmptyTransactionSubviewArb.arbitrary
      subviews = TransactionSubviews
        .apply(Seq(transactionViewWithEmptySubview))(protocolVersion, hashOps)
    } yield TransactionView.tryCreate(hashOps)(
      viewCommonData = viewCommonData,
      viewParticipantData =
        viewParticipantData.blindFully, // The view participant data in an informee tree must be blinded
      subviews = subviews,
      protocolVersion,
    )
  )

  implicit val fullInformeeTreeArb: Arbitrary[FullInformeeTree] = Arbitrary(
    for {
      submitterMetadata <- submitterMetadataArb.arbitrary
      commonData <- commonMetadataArb.arbitrary
      participantData <- participantMetadataArb.arbitrary
      rootViews <- transactionViewForInformeeTreeArb.arbitrary
      hashOps = TestHash
      rootViewsMerkleSeq = MerkleSeq.fromSeq(hashOps, protocolVersion)(Seq(rootViews))
      genTransactionTree = GenTransactionTree
        .tryCreate(hashOps)(
          submitterMetadata,
          commonData,
          participantData.blindFully, // The view participant data in an informee tree must be blinded
          rootViews = rootViewsMerkleSeq,
        )
    } yield FullInformeeTree.tryCreate(tree = genTransactionTree, protocolVersion)
  )

  // here we want to test the (de)serialization of the MerkleSeq and we use SubmitterMetadata as the VersionedMerkleTree.
  // other VersionedMerkleTree types are tested in their respective tests
  implicit val merkleSeqArb: Arbitrary[MerkleSeq[VersionedMerkleTree[?]]] =
    Arbitrary(
      for {
        submitterMetadataSeq <- boundedListGen[SubmitterMetadata]
      } yield MerkleSeq.fromSeq(TestHash, protocolVersion)(submitterMetadataSeq)
    )

  private val sourceProtocolVersion = Source(protocolVersion)
  private val targetProtocolVersion = Target(protocolVersion)

  implicit val reassignmentIdArb: Arbitrary[ReassignmentId] = Arbitrary {
    val hexChars: Seq[Char] = "0123456789abcdefABCDEF".toIndexedSeq
    Gen.stringOfN(32, Gen.oneOf(hexChars)).map { payload =>
      ReassignmentId.tryCreate(s"00$payload")
    }
  }

  implicit val reassignmentSubmitterMetadataArb: Arbitrary[ReassignmentSubmitterMetadata] =
    Arbitrary(
      for {
        submitter <- Arbitrary.arbitrary[LfPartyId]
        userId <- userIdArb.arbitrary.map(_.unwrap)
        submittingParticipant <- Arbitrary.arbitrary[ParticipantId]
        commandId <- commandIdArb.arbitrary.map(_.unwrap)
        submissionId <- Gen.option(ledgerSubmissionIdArb.arbitrary)
        workflowId <- Gen.option(workflowIdArb.arbitrary.map(_.unwrap))

      } yield ReassignmentSubmitterMetadata(
        submitter,
        submittingParticipant,
        commandId,
        submissionId,
        userId,
        workflowId,
      )
    )

  implicit val assignmentCommonDataArb: Arbitrary[AssignmentCommonData] = Arbitrary(
    for {
      salt <- Arbitrary.arbitrary[Salt]
      sourcePsid <- Arbitrary.arbitrary[Source[PhysicalSynchronizerId]]
      targetPsid <- Arbitrary.arbitrary[Target[PhysicalSynchronizerId]]

      targetMediator <- Arbitrary.arbitrary[MediatorGroupRecipient]

      stakeholders <- Arbitrary.arbitrary[Stakeholders]

      uuid <- Gen.uuid

      submitterMetadata <- Arbitrary.arbitrary[ReassignmentSubmitterMetadata]
      reassigningParticipants <- boundedSetGen[ParticipantId]
      unassignmentTs <- Arbitrary.arbitrary[CantonTimestamp]

      hashOps = TestHash // Not used for serialization

    } yield AssignmentCommonData
      .create(hashOps)(
        salt,
        sourcePsid,
        targetPsid,
        targetMediator,
        stakeholders,
        uuid,
        submitterMetadata,
        reassigningParticipants,
        unassignmentTs,
      )
  )

  implicit val unassignmentCommonData: Arbitrary[UnassignmentCommonData] = Arbitrary(
    for {
      salt <- Arbitrary.arbitrary[Salt]
      sourceSynchronizerId <- Arbitrary.arbitrary[Source[PhysicalSynchronizerId]]

      sourceMediator <- Arbitrary.arbitrary[MediatorGroupRecipient]

      stakeholders <- Arbitrary.arbitrary[Stakeholders]
      reassigningParticipants <- boundedSetGen[ParticipantId]

      uuid <- Gen.uuid

      submitterMetadata <- Arbitrary.arbitrary[ReassignmentSubmitterMetadata]

      hashOps = TestHash // Not used for serialization

    } yield UnassignmentCommonData
      .create(hashOps)(
        salt,
        sourceSynchronizerId,
        sourceMediator,
        stakeholders,
        reassigningParticipants,
        uuid,
        submitterMetadata,
        sourceProtocolVersion,
      )
  )

  implicit val assignmentViewArb: Arbitrary[AssignmentView] = Arbitrary(
    for {
      salt <- Arbitrary.arbitrary[Salt]
      contracts <- Arbitrary.arbitrary[ContractsReassignmentBatch]
      reassignmentId <- Arbitrary.arbitrary[ReassignmentId]
      hashOps = TestHash // Not used for serialization

    } yield AssignmentView
      .create(hashOps)(
        salt,
        reassignmentId,
        contracts,
        targetProtocolVersion,
      )
      .value
  )

  implicit val unassignmentViewArb: Arbitrary[UnassignmentView] = Arbitrary(
    for {
      salt <- Arbitrary.arbitrary[Salt]

      contracts <- Arbitrary.arbitrary[ContractsReassignmentBatch]

      targetSynchronizerId <- Arbitrary
        .arbitrary[Target[PhysicalSynchronizerId]]
        .map(_.map(_.copy(protocolVersion = protocolVersion)))
      targetTimestamp <- Arbitrary.arbitrary[Target[CantonTimestamp]]

      hashOps = TestHash // Not used for serialization

    } yield UnassignmentView
      .create(hashOps)(
        salt,
        contracts,
        targetSynchronizerId,
        targetTimestamp,
        sourceProtocolVersion,
      )
  )

  implicit val assignViewTreeArb: Arbitrary[AssignmentViewTree] = Arbitrary(
    for {
      commonData <- assignmentCommonDataArb.arbitrary
      assignmentView <- assignmentViewArb.arbitrary
      hash = TestHash
    } yield AssignmentViewTree(
      commonData,
      assignmentView.blindFully,
      Target(protocolVersion),
      hash,
    )
  )

  implicit val unassignmentViewTreeArb: Arbitrary[UnassignmentViewTree] = Arbitrary(
    for {
      commonData <- unassignmentCommonData.arbitrary
      unassignmentView <- unassignmentViewArb.arbitrary
      hash = TestHash
    } yield UnassignmentViewTree(
      commonData,
      unassignmentView.blindFully,
      Source(protocolVersion),
      hash,
    )
  )

  implicit val unassignmentDataArb: Arbitrary[UnassignmentData] = Arbitrary(
    for {
      submitterMetadata <- Arbitrary.arbitrary[ReassignmentSubmitterMetadata]
      contracts <- Arbitrary.arbitrary[ContractsReassignmentBatch]
      reassigningParticipants <- boundedSetGen[ParticipantId]
      sourceSynchronizer <- Arbitrary.arbitrary[PhysicalSynchronizerId].map(Source(_))
      targetSynchronizer <- Arbitrary.arbitrary[PhysicalSynchronizerId].map(Target(_))
      targetTimestamp <- Arbitrary.arbitrary[Target[CantonTimestamp]]
      unassignmentTs <- Arbitrary.arbitrary[CantonTimestamp]
    } yield UnassignmentData(
      submitterMetadata,
      contracts,
      reassigningParticipants,
      sourceSynchronizer,
      targetSynchronizer,
      targetTimestamp,
      unassignmentTs,
    )
  )

  private val fullyBlindedTransactionViewWithEmptyTransactionSubviewArb
      : Arbitrary[TransactionView] = Arbitrary(
    for {
      viewCommonData <- viewCommonDataArb.arbitrary
      viewParticipantData <- viewParticipantDataArb.arbitrary
      hashOps = TestHash
      emptySubviews = TransactionSubviews.empty(
        protocolVersion,
        hashOps,
      ) // empty TransactionSubviews
    } yield TransactionView.tryCreate(hashOps)(
      viewCommonData = viewCommonData.blindFully,
      viewParticipantData = viewParticipantData.blindFully,
      subviews = emptySubviews.blindFully,
      protocolVersion,
    )
  )

  private var unblindedSubviewHashesForLightTransactionTree: Seq[SubviewReferenceAndKey] = _

  private val transactionViewForLightTransactionTreeArb: Arbitrary[TransactionView] = Arbitrary(
    for {
      viewCommonData <- viewCommonDataArb.arbitrary
      viewParticipantData <- viewParticipantDataArb.arbitrary
      hashOps = TestHash
      transactionViewWithEmptySubview <-
        fullyBlindedTransactionViewWithEmptyTransactionSubviewArb.arbitrary
      subviews = TransactionSubviews
        .apply(Seq(transactionViewWithEmptySubview))(protocolVersion, hashOps)
      subviewHashes = subviews.trySubviewHashes
      pureCrypto = ExampleTransactionFactory.pureCrypto
      // TODO(#32393): test the (de)serialization of a LightTransactionTree with ciphertextIDs instead of view hashes
      subviewReferencesAndKeys = subviewHashes.map { viewHash =>
        SubviewReferenceAndKey(
          ByViewHash(viewHash),
          pureCrypto.generateSecureRandomness(pureCrypto.defaultSymmetricKeyScheme.keySizeInBytes),
        )
      }
    } yield {
      unblindedSubviewHashesForLightTransactionTree = subviewReferencesAndKeys
      TransactionView.tryCreate(hashOps)(
        viewCommonData = viewCommonData,
        viewParticipantData = viewParticipantData,
        subviews =
          subviews.blindFully, // only a single view in a LightTransactionTree can be unblinded
        protocolVersion,
      )
    }
  )

  implicit val lightTransactionViewTreeArb: Arbitrary[LightTransactionViewTree] = Arbitrary(
    for {
      submitterMetadata <- submitterMetadataArb.arbitrary
      commonData <- commonMetadataArb.arbitrary
      participantData <- participantMetadataArb.arbitrary
      rootViews <- transactionViewForLightTransactionTreeArb.arbitrary
      hashOps = TestHash
      rootViewsMerkleSeq = MerkleSeq.fromSeq(hashOps, protocolVersion)(Seq(rootViews))
      genTransactionTree = GenTransactionTree
        .tryCreate(hashOps)(
          submitterMetadata,
          commonData,
          participantData,
          rootViews = rootViewsMerkleSeq,
        )
    } yield LightTransactionViewTree.tryCreate(
      tree = genTransactionTree,
      unblindedSubviewHashesForLightTransactionTree,
      protocolVersion,
    )
  )

}
