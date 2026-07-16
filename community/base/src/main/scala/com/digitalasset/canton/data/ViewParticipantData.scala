// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.ActionDescription.{
  CreateActionDescription,
  ExerciseActionDescription,
  FetchActionDescription,
}
import com.digitalasset.canton.data.ViewParticipantData.{
  InvalidSerializationVersion,
  InvalidViewParticipantData,
  RootAction,
}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.{v30, v31, *}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.util.EitherUtil
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{
  LfCommand,
  LfCreateCommand,
  LfExerciseByKeyCommand,
  LfExerciseCommand,
  LfFetchByKeyCommand,
  LfFetchCommand,
  LfPackageId,
  LfPartyId,
  LfVersioned,
  ProtoDeserializationError,
  checked,
}
import com.digitalasset.daml.lf.data.Bytes
import com.digitalasset.daml.lf.transaction.ExternalCallResult
import com.digitalasset.nonempty.NonEmpty
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import monocle.Lens
import monocle.macros.GenLens

import scala.math.Ordered.orderingToOrdered

/** Information concerning every '''participant''' involved in processing the underlying view.
  *
  * @param coreInputs
  *   [[LfContractId]] used by the core of the view and not assigned by a Create node in the view or
  *   its subviews, independently of whether the creation is rolled back. Every contract id is
  *   mapped to its contract instances and their meta-information. Contracts are marked as being
  *   [[InputContract.consumed]] iff they are consumed in the core of the view.
  * @param createdCore
  *   associates contract ids of Create nodes in the core of the view to the corresponding contract
  *   instance. The elements are ordered in execution order.
  * @param createdInSubviewArchivedInCore
  *   The contracts that are created in subviews and archived in the core. The archival has the same
  *   rollback scope as the view. For [[com.digitalasset.canton.protocol.WellFormedTransaction]]s,
  *   the creation therefore is not rolled back either as the archival can only refer to non-rolled
  *   back creates.
  * @param keyResolution
  *   Post PV35 contains key, maintainers and ordered contract ids for keys that are used to resolve
  *   a key to one or more contract ids. These keys will be ones referenced in nodes for which
  *   [[com.digitalasset.daml.lf.transaction.Node.Action.byKey]] is true. A used contract that has a
  *   key but which is not queried is not included. The contract id ordering applies to all
  *   contracts used in the view or its subviews so may contain contract ids not in [[coreInputs]].
  * @param actionDescription
  *   The description of the root action of the view
  * @param rollbackContext
  *   The rollback context of the root action of the view.
  * @param externalCallResults
  *   External call results recorded by exercise nodes in the core of this view.
  *
  * The primary constructor does not check object invariants; obtain validated instances through
  * [[ViewParticipantData.create]] / [[ViewParticipantData.tryCreate]] or deserialization, which run
  * [[ViewParticipantData.validated]].
  */
final case class ViewParticipantData private (
    coreInputs: Map[LfContractId, InputContract],
    createdCore: Seq[CreatedContract],
    createdInSubviewArchivedInCore: Set[LfContractId],
    keyResolution: Map[LfGlobalKey, LfVersioned[KeyResolutionWithMaintainers]],
    actionDescription: ActionDescription,
    rollbackContext: RollbackContext,
    salt: Salt,
    externalCallResults: Seq[ViewParticipantData.ViewExternalCallResult],
)(
    hashOps: HashOps,
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      ViewParticipantData.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[ViewParticipantData](hashOps)
    with HasProtocolVersionedWrapper[ViewParticipantData]
    with ProtocolVersionedMemoizedEvidence {

  def supportsExternalCallResults: Boolean =
    representativeProtocolVersion >= ViewParticipantData.protocolVersionRepresentativeFor(
      ProtocolVersion.dev
    )

  def validated(protocolVersion: ProtocolVersion): Either[String, this.type] =
    for {
      _ <- checkCreatedCoreDistinct
      _ <- checkCoreInputs
      _ <- checkSubviewCoreOverlap
      _ <- checkExternalCallResults
      _ <- checkLegacyKeyResolution
      _ <- checkMaxSerializationVersion(protocolVersion)
      _ <- checkKeyResolution(protocolVersion)
      _ <- rootActionE
    } yield this

  private def checkDistinct[A](vals: Seq[A])(message: A => String): Either[String, Unit] = {
    val seen = scala.collection.mutable.Set.empty[A]
    vals.collectFirst { case v if !seen.add(v) => message(v) }.toLeft(())
  }

  private def checkCreatedCoreDistinct: Either[String, Unit] = {
    val createdIds = createdCore.map(_.contract.contractId)
    checkDistinct(createdIds) { id =>
      val indices = createdIds.zipWithIndex.collect {
        case (createdId, idx) if createdId == id => idx
      }
      s"createdCore contains the contract id $id multiple times at indices ${indices.mkString(", ")}"
    }
  }

  private def checkCoreInputs: Either[String, Unit] =
    coreInputs.toList
      .traverse { case (id, usedContract) =>
        for {
          _ <- Either.cond(
            id == usedContract.contractId,
            (),
            s"Inconsistent ids for used contract: $id and ${usedContract.contractId}",
          )
          _ <- Either.cond(
            !createdInSubviewArchivedInCore.contains(id),
            (),
            s"Contracts created in a subview overlap with core inputs: $id",
          )
        } yield ()
      }
      .map(_ => ())

  private def checkSubviewCoreOverlap: Either[String, Unit] = {
    val createdIds = createdCore.map(_.contract.contractId).toSet
    val transientOverlap = createdInSubviewArchivedInCore intersect createdIds
    Either.cond(
      transientOverlap.isEmpty,
      (),
      s"Contract created in a subview are also created in the core: $transientOverlap",
    )
  }

  private def checkExternalCallResults: Either[String, Unit] =
    if (externalCallResults.isEmpty) Right(())
    else
      for {
        _ <- Either.cond(
          supportsExternalCallResults,
          (),
          s"External call results are supported only from protocol version ${ProtocolVersion.dev} onwards",
        )
        _ <- actionDescription match {
          case _: ExerciseActionDescription => Right(())
          case _ => Left("External call results require an exercise root action")
        }
        _ <- checkDistinct(
          externalCallResults.map(result => (result.exerciseIndex, result.callIndex))
        ) { case (exerciseIndex, callIndex) =>
          s"externalCallResults contains duplicate occurrence (exercise index ${exerciseIndex.unwrap}, call index ${callIndex.unwrap})"
        }
      } yield ()

  private def legacyIsAssignedKeyInconsistent(
      keyWithResolution: (LfGlobalKey, LfVersioned[KeyResolutionWithMaintainers])
  ): Boolean = {
    val (key, LfVersioned(_, keyResolution)) = keyWithResolution
    keyResolution.contracts.exists { (cid: LfContractId) =>
      val inconsistent = for {
        inputContract <- coreInputs.get(cid)
        declaredKey <- inputContract.contract.metadata.maybeKey
      } yield declaredKey != key
      inconsistent.getOrElse(true)
    }
  }

  private def checkLegacyKeyResolution: Either[String, Unit] =
    if (
      representativeProtocolVersion <=
        ViewParticipantData.protocolVersionRepresentativeFor(ProtocolVersion.v34)
    ) {
      val keyInconsistencies = keyResolution.filter(legacyIsAssignedKeyInconsistent)
      Either.cond(
        keyInconsistencies.isEmpty,
        (),
        show"Inconsistencies for resolved keys: $keyInconsistencies",
      )
    } else Right(())

  private def checkMaxSerializationVersion(
      protocolVersion: ProtocolVersion
  ): Either[String, Unit] = {
    val contracts = coreInputs.values.map(_.contract) ++ createdCore.map(_.contract)
    val maxSerializationVersion =
      com.digitalasset.canton.version.LfSerializationVersionToProtocolVersions
        .maxSerializationVersionForProtocolVersion(protocolVersion)
    val invalid = contracts
      .filter(_.inst.version > maxSerializationVersion)
      .map(c => c.contractId -> c.inst.version)
      .toMap
    NonEmpty
      .from(invalid)
      .toLeft(())
      .leftMap(invalidContracts =>
        InvalidSerializationVersion(invalidContracts, protocolVersion).getMessage
      )
  }

  private def checkKeyResolution(protocolVersion: ProtocolVersion): Either[String, Unit] =
    Either.cond(
      !(protocolVersion < ProtocolVersion.v35 && keyResolution.nonEmpty),
      (),
      s"Keys not supported in $protocolVersion, but found ${keyResolution.size} keys.",
    )

  private lazy val rootActionE: Either[String, RootAction] =
    actionDescription match {
      case CreateActionDescription(contractId, _seed) =>
        for {
          createdContract <- createdCore.headOption.toRight(
            show"No created core contracts declared for a view that creates contract $contractId at the root"
          )
          _ <- Either.cond(
            createdContract.contract.contractId == contractId,
            (),
            show"View with root action Create $contractId declares ${createdContract.contract.contractId} as first created core contract.",
          )
        } yield {
          val metadata = createdContract.contract.metadata
          val contractInst = createdContract.contract.inst
          RootAction(
            LfCreateCommand(
              templateId = contractInst.templateId,
              argument = contractInst.createArg,
            ),
            metadata.signatories,
            failed = false,
            packageIdPreference = Set.empty,
          )
        }

      case ExerciseActionDescription(
            inputContractId,
            templateId,
            choice,
            interfaceId,
            packagePreference,
            chosenValue,
            actors,
            byKey,
            _seed,
            failed,
          ) =>
        for {
          inputContract <- coreInputs
            .get(inputContractId)
            .toRight(
              show"Input contract $inputContractId of the Exercise root action is not declared as core input."
            )
          cmd <-
            if (byKey)
              inputContract.contract.metadata.maybeKey
                .map(_.key)
                .toRight("Flag byKey set on an exercise of a contract without key.")
                .map(key =>
                  LfExerciseByKeyCommand(
                    templateId = templateId,
                    contractKey = key,
                    choiceId = choice,
                    argument = chosenValue.unversioned,
                  ): LfCommand
                )
            else
              Right(
                LfExerciseCommand(
                  templateId = templateId,
                  interfaceId = interfaceId,
                  contractId = inputContractId,
                  choiceId = choice,
                  argument = chosenValue.unversioned,
                ): LfCommand
              )
        } yield RootAction(cmd, actors, failed, packagePreference)

      case fetch @ FetchActionDescription(
            inputContractId,
            actors,
            byKey,
            templateId,
            interfaceId,
          ) =>
        for {
          inputContract <- coreInputs
            .get(inputContractId)
            .toRight(
              show"Input contract $inputContractId of the Fetch root action is not declared as core input."
            )
          cmd <-
            if (byKey)
              inputContract.contract.metadata.maybeKey
                .map(_.key)
                .toRight("Flag byKey set on a fetch of a contract without key.")
                .map(key => LfFetchByKeyCommand(templateId = templateId, key = key): LfCommand)
            else
              Right(
                LfFetchCommand(
                  templateId = templateId,
                  interfaceId = interfaceId,
                  coid = inputContractId,
                ): LfCommand
              )
        } yield RootAction(
          cmd,
          actors,
          failed = false,
          packageIdPreference = fetch.packagePreference,
        )
    }

  /** The root action of the view, reconstructed from [[actionDescription]] and the core inputs /
    * created core.
    */
  lazy val rootAction: RootAction =
    rootActionE.valueOr(err => throw InvalidViewParticipantData(err))

  @transient override protected lazy val companionObj: ViewParticipantData.type =
    ViewParticipantData

  private def tryToProtoV30RollbackContext: Option[v30.ViewParticipantData.RollbackContext] =
    rollbackContext match {
      case pathRollbackContext: PathRollbackContext =>
        if (pathRollbackContext.isEmpty) None else Some(pathRollbackContext.toProtoV30)
      case _ =>
        throw new IllegalStateException(
          s"Unexpected rollback context type ${rollbackContext.getClass} in ViewParticipantData"
        )
    }

  private[ViewParticipantData] def toProtoV30: v30.ViewParticipantData = v30.ViewParticipantData(
    coreInputs = coreInputs.values.map(_.toProtoV30).toSeq,
    createdCore = createdCore.map(_.toProtoV30),
    createdInSubviewArchivedInCore = createdInSubviewArchivedInCore.toSeq.map(_.toProtoPrimitive),
    resolvedKeys = Seq.empty, // Always empty, see checkKeyResolution
    actionDescription = Some(actionDescription.toProtoV30),
    rollbackContext = checked(tryToProtoV30RollbackContext),
    salt = Some(salt.toProtoV30),
  )

  private[ViewParticipantData] def toProtoV31: v31.ViewParticipantData = v31.ViewParticipantData(
    coreInputs = coreInputs.values.map(_.toProtoV30).toSeq,
    createdCore = createdCore.map(_.toProtoV30),
    createdInSubviewArchivedInCore = createdInSubviewArchivedInCore.toSeq.map(_.toProtoPrimitive),
    resolvedKeys = keyResolution.toList.map { case (k, v) =>
      KeyResolutionWithMaintainers.toProtoV31(k, v)
    },
    actionDescription = Some(actionDescription.toProtoV31),
    rollbackContext = checked(tryToProtoV30RollbackContext),
    salt = Some(salt.toProtoV30),
  )

  private[ViewParticipantData] def toProtoV32: v32.ViewParticipantData = v32.ViewParticipantData(
    coreInputs = coreInputs.values.map(_.toProtoV30).toSeq,
    createdCore = createdCore.map(_.toProtoV31),
    createdInSubviewArchivedInCore = createdInSubviewArchivedInCore.toSeq.map(_.toProtoPrimitive),
    resolvedKeys = keyResolution.toList.map { case (k, v) =>
      KeyResolutionWithMaintainers.toProtoV31(k, v)
    },
    actionDescription = Some(actionDescription.toProtoV31),
    salt = Some(salt.toProtoV30),
    externalCallResults = externalCallResults.map(_.toProtoV32),
    rolledBack = rollbackContext.inRollback,
  )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def hashPurpose: HashPurpose = HashPurpose.ViewParticipantData

  override protected def pretty: Pretty[ViewParticipantData] = prettyOfClass(
    paramIfNonEmpty("core inputs", _.coreInputs),
    paramIfNonEmpty("created core", _.createdCore),
    paramIfNonEmpty("created in subview, archived in core", _.createdInSubviewArchivedInCore),
    paramIfNonEmpty("resolved keys", _.keyResolution),
    param("action description", _.actionDescription),
    paramIfTrue("rolled back", _.rollbackContext.inRollback),
    param("salt", _.salt),
    paramIfNonEmpty(
      "external call results",
      _.externalCallResults.map(result =>
        s"${result.result.extensionId}:${result.result.functionId}@${result.exerciseIndex.unwrap}.${result.callIndex.unwrap}".unquoted
      ),
    ),
  )

  /** DO NOT USE IN PRODUCTION, as it does not necessarily check object invariants. */
  @VisibleForTesting
  def copy(
      coreInputs: Map[LfContractId, InputContract] = this.coreInputs,
      createdCore: Seq[CreatedContract] = this.createdCore,
      createdInSubviewArchivedInCore: Set[LfContractId] = this.createdInSubviewArchivedInCore,
      keyResolution: Map[LfGlobalKey, LfVersioned[KeyResolutionWithMaintainers]] =
        this.keyResolution,
      actionDescription: ActionDescription = this.actionDescription,
      rollbackContext: RollbackContext = this.rollbackContext,
      salt: Salt = this.salt,
      externalCallResults: Seq[ViewParticipantData.ViewExternalCallResult] =
        this.externalCallResults,
  ): ViewParticipantData =
    ViewParticipantData(
      coreInputs,
      createdCore,
      createdInSubviewArchivedInCore,
      keyResolution,
      actionDescription,
      rollbackContext,
      salt,
      externalCallResults,
    )(hashOps, representativeProtocolVersion, None)
}

object ViewParticipantData
    extends VersioningCompanionContextMemoization[ViewParticipantData, (HashOps, ProtocolVersion)] {
  override val name: String = "ViewParticipantData"

  // Inline context helper
  private def ic[C1, C2, P, R](f: (C1, C2, P) => R)(c: (C1, C2), p: P): R = {
    val (c1, c2) = c
    f(c1, c2, p)
  }

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.ViewParticipantData)(
      supportedProtoVersionMemoized(_)(ic(fromProtoV30)),
      _.toProtoV30,
    ),
    ProtoVersion(31) -> VersionedProtoCodec(ProtocolVersion.v35)(v31.ViewParticipantData)(
      supportedProtoVersionMemoized(_)(ic(fromProtoV31)),
      _.toProtoV31,
    ),
    ProtoVersion(32) -> VersionedProtoCodec(ProtocolVersion.v36)(
      v32.ViewParticipantData
    )(
      supportedProtoVersionMemoized(_)(ic(fromProtoV32)),
      _.toProtoV32,
    ),
    // Temporary proto version, not backed by protobuf message used to allow
    // constructor validation for external call to switch on representative protocol version.
    ProtoVersion(33) -> VersionedProtoCodec(ProtocolVersion.dev)(
      v32.ViewParticipantData
    )(
      supportedProtoVersionMemoized(_)(ic(fromProtoV32)),
      _.toProtoV32,
    ),
  )

  /** Like [[create]], but throws InvalidViewParticipantData instead of returning `Left` when the
    * object invariants do not hold.
    */
  @throws[InvalidViewParticipantData]
  def tryCreate(hashOps: HashOps)(
      coreInputs: Map[LfContractId, InputContract],
      createdCore: Seq[CreatedContract],
      createdInSubviewArchivedInCore: Set[LfContractId],
      keyResolution: Map[LfGlobalKey, LfVersioned[KeyResolutionWithMaintainers]],
      actionDescription: ActionDescription,
      rollbackContext: RollbackContext,
      salt: Salt,
      externalCallResults: Seq[ViewExternalCallResult],
      protocolVersion: ProtocolVersion,
  ): ViewParticipantData =
    create(hashOps)(
      coreInputs,
      createdCore,
      createdInSubviewArchivedInCore,
      keyResolution,
      actionDescription,
      rollbackContext,
      salt,
      externalCallResults,
      protocolVersion,
    ).valueOr(err => throw InvalidViewParticipantData(err))

  /** Creates a ViewParticipantData.
    *
    * Yields `Left(...)` if `validated` fails on the created instance.
    */
  def create(hashOps: HashOps)(
      coreInputs: Map[LfContractId, InputContract],
      createdCore: Seq[CreatedContract],
      createdInSubviewArchivedInCore: Set[LfContractId],
      keyResolution: Map[LfGlobalKey, LfVersioned[KeyResolutionWithMaintainers]],
      actionDescription: ActionDescription,
      rollbackContext: RollbackContext,
      salt: Salt,
      externalCallResults: Seq[ViewExternalCallResult],
      protocolVersion: ProtocolVersion,
  ): Either[String, ViewParticipantData] =
    ViewParticipantData(
      coreInputs,
      createdCore,
      createdInSubviewArchivedInCore,
      keyResolution,
      actionDescription,
      rollbackContext,
      salt,
      externalCallResults,
    )(hashOps, protocolVersionRepresentativeFor(protocolVersion), deserializedFrom = None)
      .validated(protocolVersion)

  private def fromProtoV30(
      hashOps: HashOps,
      protocolVersion: ProtocolVersion,
      dataP: v30.ViewParticipantData,
  )(
      bytes: ByteString
  ): ParsingResult[ViewParticipantData] = {
    val v30.ViewParticipantData(
      saltP,
      coreInputsP,
      createdCoreP,
      createdInSubviewArchivedInCoreP,
      resolvedKeysP,
      actionDescriptionP,
      rbContextP,
    ) = dataP

    for {
      actionDescription <- ProtoConverter
        .required("action_description", actionDescriptionP)
        .flatMap(ActionDescription.fromProtoV30)
      rollbackContext <- PathRollbackContext
        .fromProtoV30(rbContextP)
        .leftMap(_.inField("rollback_context"))
      _ <- EitherUtil.condUnit( // Invariant violation, see checkKeyResolution
        resolvedKeysP.isEmpty,
        InvariantViolation(Some("resolved-keys"), "Unexpected contract keys"),
      )
      createdCore <- createdCoreP.traverse(CreatedContract.fromProtoV30)
      viewParticipantData <- fromProto(
        hashOps,
        Map.empty,
        actionDescription,
        rollbackContext,
        createdCore,
        protocolVersion,
        bytes,
      )(
        saltP,
        coreInputsP,
        createdInSubviewArchivedInCoreP,
        Seq.empty,
      )
    } yield {
      viewParticipantData
    }
  }

  private def fromProtoV31(
      hashOps: HashOps,
      protocolVersion: ProtocolVersion,
      dataP: v31.ViewParticipantData,
  )(
      bytes: ByteString
  ): ParsingResult[ViewParticipantData] = {
    val v31.ViewParticipantData(
      saltP,
      coreInputsP,
      createdCoreP,
      createdInSubviewArchivedInCoreP,
      resolvedKeysP,
      actionDescriptionP,
      rbContextP,
    ) = dataP

    for {
      keyResolution <- resolvedKeysP.traverse(KeyResolutionWithMaintainers.fromProtoV31)
      actionDescription <- ProtoConverter
        .required("action_description", actionDescriptionP)
        .flatMap(ActionDescription.fromProtoV31)
      rollbackContext <- PathRollbackContext
        .fromProtoV30(rbContextP)
        .leftMap(_.inField("rollback_context"))
      createdCore <- createdCoreP.traverse(CreatedContract.fromProtoV30)
      viewParticipantData <- fromProto(
        hashOps,
        keyResolution.toMap,
        actionDescription,
        rollbackContext,
        createdCore,
        protocolVersion,
        bytes,
      )(
        saltP,
        coreInputsP,
        createdInSubviewArchivedInCoreP,
        Seq.empty,
      )
    } yield viewParticipantData
  }

  private def fromProtoV32(
      hashOps: HashOps,
      protocolVersion: ProtocolVersion,
      dataP: v32.ViewParticipantData,
  )(
      bytes: ByteString
  ): ParsingResult[ViewParticipantData] = {
    val v32.ViewParticipantData(
      saltP,
      coreInputsP,
      createdCoreP,
      createdInSubviewArchivedInCoreP,
      resolvedKeysP,
      actionDescriptionP,
      externalCallResultsP,
      rolledBackP,
    ) = dataP

    for {
      keyResolution <- resolvedKeysP.traverse(KeyResolutionWithMaintainers.fromProtoV31)
      actionDescription <- ProtoConverter
        .required("action_description", actionDescriptionP)
        .flatMap(ActionDescription.fromProtoV31)
      externalCallResults <- externalCallResultsP.traverse(ViewExternalCallResult.fromProtoV32)
      createdCore <- createdCoreP.traverse(CreatedContract.fromProtoV31)
      viewParticipantData <- fromProto(
        hashOps,
        keyResolution.toMap,
        actionDescription,
        NoPathRollbackContext(rolledBackP),
        createdCore,
        protocolVersion,
        bytes,
      )(
        saltP,
        coreInputsP,
        createdInSubviewArchivedInCoreP,
        externalCallResults,
      )
    } yield viewParticipantData
  }

  private def fromProto(
      hashOps: HashOps,
      keyResolution: Map[LfGlobalKey, LfVersioned[KeyResolutionWithMaintainers]],
      actionDescription: ActionDescription,
      rollbackContext: RollbackContext,
      createdCore: Seq[CreatedContract],
      protocolVersion: ProtocolVersion,
      bytes: ByteString,
  )(
      saltP: Option[com.digitalasset.canton.crypto.v30.Salt],
      coreInputsP: Seq[v30.InputContract],
      createdInSubviewArchivedInCoreP: Seq[String],
      externalCallResults: Seq[ViewExternalCallResult],
  ): ParsingResult[ViewParticipantData] =
    for {
      coreInputsSeq <- coreInputsP.traverse(InputContract.fromProtoV30)
      coreInputs = coreInputsSeq.view
        .map(inputContract => inputContract.contract.contractId -> inputContract)
        .toMap
      createdInSubviewArchivedInCore <- createdInSubviewArchivedInCoreP
        .traverse(ProtoConverter.parseLfContractId)
      salt <- ProtoConverter
        .parseRequired(Salt.fromProtoV30, "salt", saltP)
        .leftMap(_.inField("salt"))
      viewParticipantData <- ViewParticipantData(
        coreInputs = coreInputs,
        createdCore = createdCore,
        createdInSubviewArchivedInCore = createdInSubviewArchivedInCore.toSet,
        keyResolution = keyResolution,
        actionDescription = actionDescription,
        rollbackContext = rollbackContext,
        salt = salt,
        externalCallResults = externalCallResults,
      )(
        hashOps,
        protocolVersionRepresentativeFor(protocolVersion),
        deserializedFrom = Some(bytes),
      ).validated(protocolVersion)
        .leftMap(ProtoDeserializationError.OtherError.apply)
    } yield viewParticipantData

  final case class RootAction(
      command: LfCommand,
      authorizers: Set[LfPartyId],
      failed: Boolean,
      packageIdPreference: Set[LfPackageId],
  )

  /** Indicates an attempt to create an invalid [[ViewParticipantData]]. */
  final case class InvalidViewParticipantData(message: String) extends RuntimeException(message)

  final case class InvalidSerializationVersion(
      invalid: NonEmpty[Map[LfContractId, LfSerializationVersion]],
      protocolVersion: ProtocolVersion,
  ) extends RuntimeException(
        s"ViewParticipantData contains contracts with serialization versions not supported by protocol version $protocolVersion: $invalid"
      )

  /** External-call result recorded in this view's core.
    *
    * @param exerciseIndex
    *   Zero-based index of the exercise node in this view's core traversal.
    * @param callIndex
    *   Zero-based index of the external call result on that exercise node.
    * @param checkingParties
    *   Node-level confirming parties responsible for checking this result.
    */
  final case class ViewExternalCallResult(
      result: ExternalCallResult,
      exerciseIndex: NonNegativeInt,
      callIndex: NonNegativeInt,
      checkingParties: Set[LfPartyId],
  ) {
    private[ViewParticipantData] def toProtoV32: v32.ViewExternalCallResult =
      v32.ViewExternalCallResult(
        extensionId = result.extensionId,
        functionId = result.functionId,
        config = result.config.toByteString,
        input = result.input.toByteString,
        output = result.output.toByteString,
        exerciseIndex = exerciseIndex.unwrap,
        callIndex = callIndex.unwrap,
        checkingParties = checkingParties.toSeq.sorted,
      )
  }

  object ViewExternalCallResult {
    def fromProtoV32(
        resultP: v32.ViewExternalCallResult
    ): ParsingResult[ViewExternalCallResult] = {
      val v32.ViewExternalCallResult(
        extensionId,
        functionId,
        config,
        input,
        output,
        exerciseIndexP,
        callIndexP,
        checkingPartiesP,
      ) = resultP
      for {
        exerciseIndex <- ProtoConverter.parseNonNegativeInt("exercise_index", exerciseIndexP)
        callIndex <- ProtoConverter.parseNonNegativeInt("call_index", callIndexP)
        checkingParties <- checkingPartiesP
          .traverse(ProtoConverter.parseLfPartyId(_, field = "checking_parties"))
      } yield ViewExternalCallResult(
        result = ExternalCallResult(
          extensionId = extensionId,
          functionId = functionId,
          config = Bytes.fromByteString(config),
          input = Bytes.fromByteString(input),
          output = Bytes.fromByteString(output),
        ),
        exerciseIndex = exerciseIndex,
        callIndex = callIndex,
        checkingParties = checkingParties.toSet,
      )
    }
  }

  /** DO NOT USE IN PRODUCTION, as it does not necessarily check object invariants. */
  @VisibleForTesting
  object Optics {
    val coreInputsUnsafe: Lens[ViewParticipantData, Map[LfContractId, InputContract]] =
      GenLens.apply[ViewParticipantData](_.coreInputs)
    val createdCoreUnsafe: Lens[ViewParticipantData, Seq[CreatedContract]] =
      GenLens.apply[ViewParticipantData](_.createdCore)
    val actionDescriptionUnsafe: Lens[ViewParticipantData, ActionDescription] =
      GenLens.apply[ViewParticipantData](_.actionDescription)
    val saltUnsafe: Lens[ViewParticipantData, Salt] =
      GenLens.apply[ViewParticipantData](_.salt)
    val externalCallResultsUnsafe: Lens[ViewParticipantData, Seq[ViewExternalCallResult]] =
      GenLens.apply[ViewParticipantData](_.externalCallResults)
    val keyResolutionUnsafe
        : Lens[ViewParticipantData, Map[LfGlobalKey, LfVersioned[KeyResolutionWithMaintainers]]] =
      GenLens.apply[ViewParticipantData](_.keyResolution)
  }

}
