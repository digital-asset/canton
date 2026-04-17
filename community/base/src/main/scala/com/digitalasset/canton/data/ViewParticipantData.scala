// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.ActionDescription.{
  CreateActionDescription,
  ExerciseActionDescription,
  FetchActionDescription,
  LookupByKeyActionDescription,
}
import com.digitalasset.canton.data.ViewParticipantData.{InvalidViewParticipantData, RootAction}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.{v30, *}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{
  ProtoConverter,
  ProtocolVersionedMemoizedEvidence,
  SerializationCheckFailed,
}
import com.digitalasset.canton.version.{ProtoVersion, *}
import com.digitalasset.canton.{
  LfCommand,
  LfCreateCommand,
  LfExerciseByKeyCommand,
  LfExerciseCommand,
  LfFetchByKeyCommand,
  LfFetchCommand,
  LfLookupByKeyCommand,
  LfPackageId,
  LfPartyId,
  LfVersioned,
  ProtoDeserializationError,
  checked,
}
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
  *   Specifies how to resolve [[com.digitalasset.daml.lf.engine.ResultNeedKey]] requests from DAMLe
  *   (resulting from e.g., fetchByKey, lookupByKey, queryByKey) when interpreting the view. The
  *   resolved contract IDs must be in the [[coreInputs]].
  * @param actionDescription
  *   The description of the root action of the view
  * @param rollbackContext
  *   The rollback context of the root action of the view.
  * @throws ViewParticipantData$.InvalidViewParticipantData
  *   if [[createdCore]] contains two elements with the same contract id, if
  *   [[coreInputs]]`(id).contractId != id` if [[createdInSubviewArchivedInCore]] overlaps with
  *   [[createdCore]]'s ids or [[coreInputs]] if [[coreInputs]] does not contain the resolved
  *   [[keyResolution]] pre pv35 empty, post pv35 holds the the maintainers of all keys used in the
  *   view. May reference input contracts in child views.
  * @throws com.digitalasset.canton.serialization.SerializationCheckFailed
  *   if this instance cannot be serialized
  */
final case class ViewParticipantData private (
    coreInputs: Map[LfContractId, InputContract],
    createdCore: Seq[CreatedContract],
    createdInSubviewArchivedInCore: Set[LfContractId],
    keyResolution: Map[LfGlobalKey, LfVersioned[KeyResolutionWithMaintainers]],
    actionDescription: ActionDescription,
    rollbackContext: RollbackContext,
    salt: Salt,
    pinnedData: Seq[PinnedDataNode] = Seq.empty, // CIP-draft-external-data-pinning
)(
    hashOps: HashOps,
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      ViewParticipantData.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[ViewParticipantData](hashOps)
    with HasProtocolVersionedWrapper[ViewParticipantData]
    with ProtocolVersionedMemoizedEvidence {
  {
    def requireDistinct[A](vals: Seq[A])(message: A => String): Unit = {
      val set = scala.collection.mutable.Set[A]()
      vals.foreach { v =>
        if (set(v)) throw InvalidViewParticipantData(message(v))
        else set += v
      }
    }

    val createdIds = createdCore.map(_.contract.contractId)
    requireDistinct(createdIds) { id =>
      val indices = createdIds.zipWithIndex.collect {
        case (createdId, idx) if createdId == id => idx
      }
      s"createdCore contains the contract id $id multiple times at indices ${indices.mkString(", ")}"
    }

    coreInputs.foreach { case (id, usedContract) =>
      if (id != usedContract.contractId)
        throw InvalidViewParticipantData(
          s"Inconsistent ids for used contract: $id and ${usedContract.contractId}"
        )

      if (createdInSubviewArchivedInCore.contains(id))
        throw InvalidViewParticipantData(
          s"Contracts created in a subview overlap with core inputs: $id"
        )
    }

    val transientOverlap = createdInSubviewArchivedInCore intersect createdIds.toSet
    if (transientOverlap.nonEmpty)
      throw InvalidViewParticipantData(
        s"Contract created in a subview are also created in the core: $transientOverlap"
      )

  }

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

  private def checkLegacyResolutionsReferenceInputContracts(): Unit = {
    val keyInconsistencies = keyResolution.filter(legacyIsAssignedKeyInconsistent)
    if (keyInconsistencies.nonEmpty) {
      throw InvalidViewParticipantData(
        show"Inconsistencies for resolved keys: $keyInconsistencies"
      )
    }
  }

  if (
    representativeProtocolVersion <= ViewParticipantData.protocolVersionRepresentativeFor(
      ProtocolVersion.v34
    )
  ) {
    checkLegacyResolutionsReferenceInputContracts()
  }

  val rootAction: RootAction =
    actionDescription match {
      case CreateActionDescription(contractId, _seed) =>
        val createdContract = createdCore.headOption.getOrElse(
          throw InvalidViewParticipantData(
            show"No created core contracts declared for a view that creates contract $contractId at the root"
          )
        )
        if (createdContract.contract.contractId != contractId)
          throw InvalidViewParticipantData(
            show"View with root action Create $contractId declares ${createdContract.contract.contractId} as first created core contract."
          )
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
        val inputContract = coreInputs.getOrElse(
          inputContractId,
          throw InvalidViewParticipantData(
            show"Input contract $inputContractId of the Exercise root action is not declared as core input."
          ),
        )

        val cmd = if (byKey) {
          val key = inputContract.contract.metadata.maybeKey
            .map(_.key)
            .getOrElse(
              throw InvalidViewParticipantData(
                "Flag byKey set on an exercise of a contract without key."
              )
            )
          LfExerciseByKeyCommand(
            templateId = templateId,
            contractKey = key,
            choiceId = choice,
            argument = chosenValue.unversioned,
          )
        } else {
          LfExerciseCommand(
            templateId = templateId,
            interfaceId = interfaceId,
            contractId = inputContractId,
            choiceId = choice,
            argument = chosenValue.unversioned,
          )
        }
        RootAction(cmd, actors, failed, packagePreference)

      case fetch @ FetchActionDescription(
            inputContractId,
            actors,
            byKey,
            templateId,
            interfaceId,
          ) =>
        val inputContract = coreInputs.getOrElse(
          inputContractId,
          throw InvalidViewParticipantData(
            show"Input contract $inputContractId of the Fetch root action is not declared as core input."
          ),
        )

        val cmd = if (byKey) {
          val key = inputContract.contract.metadata.maybeKey
            .map(_.key)
            .getOrElse(
              throw InvalidViewParticipantData(
                "Flag byKey set on a fetch of a contract without key."
              )
            )
          LfFetchByKeyCommand(templateId = templateId, key = key)
        } else {
          LfFetchCommand(templateId = templateId, interfaceId = interfaceId, coid = inputContractId)
        }
        RootAction(cmd, actors, failed = false, packageIdPreference = fetch.packagePreference)

      // This is created only maliciously
      case LookupByKeyActionDescription(LfVersioned(_version, key)) =>
        val LfVersioned(_, resolution) = keyResolution.getOrElse(
          key,
          throw InvalidViewParticipantData(
            show"Key $key of LookupByKey root action is not resolved."
          ),
        )
        val maintainers = (resolution.contracts, resolution.maintainers) match {
          case (Seq(), maintainers) => maintainers
          case (Seq(contractId), maintainers) if maintainers.isEmpty =>
            checked(coreInputs(contractId)).maintainers
          case _ =>
            throw new IllegalStateException(
              s"Invalid key resolution for LookupByKey: $keyResolution"
            )
        }
        RootAction(
          LfLookupByKeyCommand(templateId = key.templateId, contractKey = key.key),
          maintainers,
          failed = false,
          packageIdPreference = Set.empty,
        )
    }

  @transient override protected lazy val companionObj: ViewParticipantData.type =
    ViewParticipantData

  private[ViewParticipantData] def toProtoV30: v30.ViewParticipantData = v30.ViewParticipantData(
    coreInputs = coreInputs.values.map(_.toProtoV30).toSeq,
    createdCore = createdCore.map(_.toProtoV30),
    createdInSubviewArchivedInCore = createdInSubviewArchivedInCore.toSeq.map(_.toProtoPrimitive),
    resolvedKeys = keyResolution.map { case (k, LfVersioned(version, resolution)) =>
      v30.ViewParticipantData.ResolvedKey(
        key = Some(GlobalKeySerialization.assertToProtoV30(LfVersioned(version, k))),
        resolution = LegacyKeyResolutionWithMaintainers
          .tryFromNextGen(resolution)
          .asSerializable
          .toProtoOneOfV30,
      )
    }.toSeq,
    actionDescription = Some(actionDescription.toProtoV30),
    rollbackContext = if (rollbackContext.isEmpty) None else Some(rollbackContext.toProtoV30),
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
    rollbackContext = if (rollbackContext.isEmpty) None else Some(rollbackContext.toProtoV30),
    salt = Some(salt.toProtoV30),
    pinnedData = Seq.empty, // TODO: wire pinnedData field from ViewParticipantData case class
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
    param("rollback context", _.rollbackContext),
    param("salt", _.salt),
  )

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
  ): ViewParticipantData =
    ViewParticipantData(
      coreInputs,
      createdCore,
      createdInSubviewArchivedInCore,
      keyResolution,
      actionDescription,
      rollbackContext,
      salt,
    )(hashOps, representativeProtocolVersion, None)
}

object ViewParticipantData
    extends VersioningCompanionContextMemoization[ViewParticipantData, HashOps] {
  override val name: String = "ViewParticipantData"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.ViewParticipantData)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30,
    ),
    ProtoVersion(31) -> VersionedProtoCodec(ProtocolVersion.v35)(v31.ViewParticipantData)(
      supportedProtoVersionMemoized(_)(fromProtoV31),
      _.toProtoV31,
    ),
  )

  /** Creates a view participant data.
    *
    * @throws InvalidViewParticipantData
    *   if [[ViewParticipantData.createdCore]] contains two elements with the same contract id, if
    *   [[ViewParticipantData.coreInputs]]`(id).contractId != id` if
    *   [[ViewParticipantData.createdInSubviewArchivedInCore]] overlaps with
    *   [[ViewParticipantData.createdCore]]'s ids or [[ViewParticipantData.coreInputs]] if
    *   [[ViewParticipantData.coreInputs]] does not contain the resolved contract ids in
    *   [[ViewParticipantData.keyResolution]] if [[ViewParticipantData.createdCore]] creates a
    *   contract with a key that is not in [[ViewParticipantData.keyResolution]] if the
    *   [[ViewParticipantData.actionDescription]] is a
    *   [[com.digitalasset.canton.data.ActionDescription.CreateActionDescription]] and the created
    *   id is not the first contract ID in [[ViewParticipantData.createdCore]] if the
    *   [[ViewParticipantData.actionDescription]] is a
    *   [[com.digitalasset.canton.data.ActionDescription.ExerciseActionDescription]] or
    *   [[com.digitalasset.canton.data.ActionDescription.FetchActionDescription]] and the input
    *   contract is not in [[ViewParticipantData.coreInputs]]
    * @throws com.digitalasset.canton.serialization.SerializationCheckFailed
    *   if this instance cannot be serialized
    */
  @throws[SerializationCheckFailed[com.digitalasset.daml.lf.value.ValueCoder.EncodeError]]
  def tryCreate(hashOps: HashOps)(
      coreInputs: Map[LfContractId, InputContract],
      createdCore: Seq[CreatedContract],
      createdInSubviewArchivedInCore: Set[LfContractId],
      resolvedKeys: Map[LfGlobalKey, LfVersioned[KeyResolutionWithMaintainers]],
      actionDescription: ActionDescription,
      rollbackContext: RollbackContext,
      salt: Salt,
      protocolVersion: ProtocolVersion,
  ): ViewParticipantData =
    ViewParticipantData(
      coreInputs,
      createdCore,
      createdInSubviewArchivedInCore,
      resolvedKeys,
      actionDescription,
      rollbackContext,
      salt,
    )(hashOps, protocolVersionRepresentativeFor(protocolVersion), None)

  /** Creates a view participant data.
    *
    * Yields `Left(...)` if [[ViewParticipantData.createdCore]] contains two elements with the same
    * contract id, if [[ViewParticipantData.coreInputs]]`(id).contractId != id` if
    * [[ViewParticipantData.createdInSubviewArchivedInCore]] overlaps with
    * [[ViewParticipantData.createdCore]]'s ids or [[ViewParticipantData.coreInputs]] if
    * [[ViewParticipantData.coreInputs]] does not contain the resolved contract ids in
    * [[ViewParticipantData.keyResolution]] if [[ViewParticipantData.createdCore]] creates a
    * contract with a key that is not in [[ViewParticipantData.keyResolution]] if the
    * [[ViewParticipantData.actionDescription]] is a
    * [[com.digitalasset.canton.data.ActionDescription.CreateActionDescription]] and the created id
    * is not the first contract ID in [[ViewParticipantData.createdCore]] if the
    * [[ViewParticipantData.actionDescription]] is a
    * [[com.digitalasset.canton.data.ActionDescription.ExerciseActionDescription]] or
    * [[com.digitalasset.canton.data.ActionDescription.FetchActionDescription]] and the input
    * contract is not in [[ViewParticipantData.coreInputs]]
    */
  def create(hashOps: HashOps)(
      coreInputs: Map[LfContractId, InputContract],
      createdCore: Seq[CreatedContract],
      createdInSubviewArchivedInCore: Set[LfContractId],
      resolvedKeys: Map[LfGlobalKey, LfVersioned[KeyResolutionWithMaintainers]],
      actionDescription: ActionDescription,
      rollbackContext: RollbackContext,
      salt: Salt,
      protocolVersion: ProtocolVersion,
  ): Either[String, ViewParticipantData] =
    returnLeftWhenInitializationFails(
      ViewParticipantData.tryCreate(hashOps)(
        coreInputs,
        createdCore,
        createdInSubviewArchivedInCore,
        resolvedKeys,
        actionDescription,
        rollbackContext,
        salt,
        protocolVersion,
      )
    )

  private[this] def returnLeftWhenInitializationFails[A](initialization: => A): Either[String, A] =
    try {
      Right(initialization)
    } catch {
      case InvalidViewParticipantData(message) => Left(message)
      case SerializationCheckFailed(err) => Left(err.toString)
    }

  private def fromProtoV30(hashOps: HashOps, dataP: v30.ViewParticipantData)(
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
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      resolvedKeys <- resolvedKeysP
        .traverse { rkP =>
          for {
            keyP <- ProtoConverter.required("key", rkP.key)
            key <- GlobalKeySerialization.fromProtoV30(keyP)
            resolution <- LegacySerializableKeyResolution.fromProtoOneOfV30(rkP.resolution)
          } yield (key.unversioned, key.map(_ => resolution.tryToNextGen()))
        }
        .map(_.toMap)
      viewParticipantData <- fromProto(hashOps, resolvedKeys, actionDescription, rpv, bytes)(
        saltP,
        coreInputsP,
        createdCoreP,
        createdInSubviewArchivedInCoreP,
        rbContextP,
      )
    } yield {
      viewParticipantData
    }
  }

  private def fromProtoV31(hashOps: HashOps, dataP: v31.ViewParticipantData)(
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
      pinnedDataP,
    ) = dataP

    for {
      resolvedKeys <- resolvedKeysP.traverse(KeyResolutionWithMaintainers.fromProtoV31)
      actionDescription <- ProtoConverter
        .required("action_description", actionDescriptionP)
        .flatMap(ActionDescription.fromProtoV31)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(31))
      viewParticipantData <- fromProto(hashOps, resolvedKeys.toMap, actionDescription, rpv, bytes)(
        saltP,
        coreInputsP,
        createdCoreP,
        createdInSubviewArchivedInCoreP,
        rbContextP,
      )
    } yield viewParticipantData
  }

  private def fromProto(
      hashOps: HashOps,
      resolvedKeys: Map[LfGlobalKey, LfVersioned[KeyResolutionWithMaintainers]],
      actionDescription: ActionDescription,
      rpv: RepresentativeProtocolVersion[ViewParticipantData.type],
      bytes: ByteString,
  )(
      saltP: Option[com.digitalasset.canton.crypto.v30.Salt],
      coreInputsP: Seq[v30.InputContract],
      createdCoreP: Seq[v30.CreatedContract],
      createdInSubviewArchivedInCoreP: Seq[String],
      rollbackContextP: Option[v30.ViewParticipantData.RollbackContext],
  ): ParsingResult[ViewParticipantData] =
    for {
      coreInputsSeq <- coreInputsP.traverse(InputContract.fromProtoV30)
      coreInputs = coreInputsSeq.view
        .map(inputContract => inputContract.contract.contractId -> inputContract)
        .toMap
      createdCore <- createdCoreP.traverse(CreatedContract.fromProtoV30)
      createdInSubviewArchivedInCore <- createdInSubviewArchivedInCoreP
        .traverse(ProtoConverter.parseLfContractId)
      salt <- ProtoConverter
        .parseRequired(Salt.fromProtoV30, "salt", saltP)
        .leftMap(_.inField("salt"))
      rollbackContext <- RollbackContext
        .fromProtoV30(rollbackContextP)
        .leftMap(_.inField("rollbackContext"))
      viewParticipantData <- returnLeftWhenInitializationFails(
        ViewParticipantData(
          coreInputs = coreInputs,
          createdCore = createdCore,
          createdInSubviewArchivedInCore = createdInSubviewArchivedInCore.toSet,
          keyResolution = resolvedKeys,
          actionDescription = actionDescription,
          rollbackContext = rollbackContext,
          salt = salt,
        )(hashOps, rpv, Some(bytes))
      ).leftMap(ProtoDeserializationError.OtherError.apply)
    } yield viewParticipantData

  final case class RootAction(
      command: LfCommand,
      authorizers: Set[LfPartyId],
      failed: Boolean,
      packageIdPreference: Set[LfPackageId],
  )

  /** Indicates an attempt to create an invalid [[ViewParticipantData]]. */
  final case class InvalidViewParticipantData(message: String) extends RuntimeException(message)

  /** DO NOT USE IN PRODUCTION, as it does not necessarily check object invariants. */
  @VisibleForTesting
  object Optics {
    val coreInputsUnsafe: Lens[ViewParticipantData, Map[LfContractId, InputContract]] =
      GenLens[ViewParticipantData](_.coreInputs)
    val createdCoreUnsafe: Lens[ViewParticipantData, Seq[CreatedContract]] =
      GenLens[ViewParticipantData](_.createdCore)
    val actionDescriptionUnsafe: Lens[ViewParticipantData, ActionDescription] =
      GenLens[ViewParticipantData](_.actionDescription)
    val saltUnsafe: Lens[ViewParticipantData, Salt] =
      GenLens[ViewParticipantData](_.salt)
  }

}
