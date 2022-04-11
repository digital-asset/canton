// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either._
import cats.syntax.traverse._
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.data.ActionDescription.{
  CreateActionDescription,
  ExerciseActionDescription,
  FetchActionDescription,
  LookupByKeyActionDescription,
}
import com.digitalasset.canton.data.TransactionView.InvalidView
import com.digitalasset.canton.data.TreeSerialization.TransactionSerializationError
import com.digitalasset.canton.data.ViewCommonData.InvalidViewCommonData
import com.digitalasset.canton.data.ViewParticipantData.{InvalidViewParticipantData, RootAction}
import com.digitalasset.canton.data.ViewPosition.{ListIndex, MerklePathElement}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.ContractIdSyntax._
import com.digitalasset.canton.protocol.version.{
  VersionedViewCommonData,
  VersionedViewParticipantData,
}
import com.digitalasset.canton.protocol.{v0, _}
import com.digitalasset.canton.serialization.{
  MemoizedEvidence,
  ProtoConverter,
  SerializationCheckFailed,
}
import com.digitalasset.canton.protocol.RollbackContext
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.{
  HasProtoV0,
  HasVersionedToByteString,
  HasVersionedWrapper,
  NoCopy,
}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  LfCommand,
  LfCreateCommand,
  LfExerciseByKeyCommand,
  LfExerciseCommand,
  LfFetchByKeyCommand,
  LfFetchCommand,
  LfLookupByKeyCommand,
  LfPartyId,
  ProtoDeserializationError,
  checked,
}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

/** Encapsulates a subaction of the underlying transaction.
  *
  * @param subviews the top-most subviews of this view
  * @throws TransactionView$.InvalidView if the `viewCommonData` is unblinded and equals the `viewCommonData` of a direct subview
  */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
case class TransactionView private (
    viewCommonData: MerkleTree[ViewCommonData],
    viewParticipantData: MerkleTree[ViewParticipantData],
    subviews: Seq[MerkleTree[TransactionView]],
)(hashOps: HashOps)
    extends MerkleTreeInnerNode[TransactionView](hashOps)
    with HasVersionedToByteString
    with HasProtoV0[v0.ViewNode]
    with NoCopy {

  if (viewCommonData.unwrap.isRight) {
    subviews
      .find(_.unwrap.exists(_.viewCommonData == viewCommonData))
      .map(subview => {
        throw InvalidView(
          s"The subview with index ${subviews.indexOf(subview)} has an equal viewCommonData."
        )
      })
  }

  override def subtrees: Seq[MerkleTree[_]] =
    Seq[MerkleTree[_]](viewCommonData, viewParticipantData) ++ subviews

  override private[data] def withBlindedSubtrees(
      blindingCommandPerNode: PartialFunction[RootHash, MerkleTree.BlindingCommand]
  ): MerkleTree[TransactionView] =
    TransactionView(hashOps)(
      viewCommonData.doBlind(blindingCommandPerNode), // O(1)
      viewParticipantData.doBlind(blindingCommandPerNode), // O(1)
      subviews.map(_.doBlind(blindingCommandPerNode)), // O(#subviews)
    )

  val viewHash: ViewHash = ViewHash.fromRootHash(rootHash)

  /** Traverses all subviews `v1, v2, v3, ...` in pre-order and yields
    * `f(...f(f(v1, z), v2)..., vn)`
    */
  def foldLeft[A](z: A)(f: (A, TransactionView) => A): A =
    subviews
      .to(LazyList)
      .flatMap(_.unwrap.toSeq) // filter out blinded subviews
      .foldLeft(f(z, this))((acc, subView) => subView.foldLeft(acc)(f))

  /** Yields all (direct and indirect) subviews of this view in pre-order.
    * The first element is this view.
    */
  def flatten: Seq[TransactionView] =
    foldLeft(Seq.newBuilder[TransactionView])((acc, v) => acc += v).result()

  def subviewsWithIndex: Seq[(MerkleTree[TransactionView], MerklePathElement)] =
    subviews.zipWithIndex.map { case (view, index) => view -> ListIndex(index) }

  override def pretty: Pretty[TransactionView] = prettyOfClass(
    param("root hash", _.rootHash),
    param("view common data", _.viewCommonData),
    param("view participant data", _.viewParticipantData),
    param("subviews", _.subviews),
  )

  @VisibleForTesting
  private[data] def copy(
      viewCommonData: MerkleTree[ViewCommonData] = this.viewCommonData,
      viewParticipantData: MerkleTree[ViewParticipantData] = this.viewParticipantData,
      subviews: Seq[MerkleTree[TransactionView]] = this.subviews,
  ) =
    new TransactionView(viewCommonData, viewParticipantData, subviews)(hashOps)

  /** If the view with the given hash appears as a (possibly blinded) descendant of this view ,
    * replace it by the given view. Note that this view also counts as a descendant
    * of itself for this purpose.
    * TODO(M40): not stack safe unless we have limits on the depths of views.
    */
  def replace(h: ViewHash, v: TransactionView): TransactionView = {
    if (viewHash == h) v
    else
      this.copy(subviews =
        subviews.map(sv =>
          sv.unwrap.fold(h2 => if (h2.unwrap == h.unwrap) v else sv, tv => tv.replace(h, v))
        )
      )
  }

  protected def toProtoV0: v0.ViewNode = v0.ViewNode(
    viewCommonData = Some(MerkleTree.toBlindableNode(viewCommonData)),
    viewParticipantData = Some(MerkleTree.toBlindableNode(viewParticipantData)),
    subviews = subviews.map(subview => MerkleTree.toBlindableNode(subview)),
  )

  override def toByteString(version: ProtocolVersion): ByteString = toProtoV0.toByteString
}

object TransactionView {

  private def apply(
      viewCommonData: MerkleTree[ViewCommonData],
      viewParticipantData: MerkleTree[ViewParticipantData],
      subviews: Seq[MerkleTree[TransactionView]],
  )(hashOps: HashOps): TransactionView =
    throw new UnsupportedOperationException("Use the create/tryCreate methods instead")

  /** Creates a view.
    *
    * @throws InvalidView if the `viewCommonData` is unblinded and equals the `viewCommonData` of a direct subview
    */
  def apply(hashOps: HashOps)(
      viewCommonData: MerkleTree[ViewCommonData],
      viewParticipantData: MerkleTree[ViewParticipantData],
      subviews: Seq[MerkleTree[TransactionView]],
  ): TransactionView =
    new TransactionView(viewCommonData, viewParticipantData, subviews)(hashOps)

  /** Creates a view.
    *
    * Yields `Left(...)` if the `viewCommonData` is unblinded and equals the `viewCommonData` of a direct subview
    */
  def create(hashOps: HashOps)(
      viewCommonData: MerkleTree[ViewCommonData],
      viewParticipantData: MerkleTree[ViewParticipantData],
      subviews: Seq[MerkleTree[TransactionView]],
  ): Either[String, TransactionView] =
    Either
      .catchOnly[InvalidView](
        TransactionView(hashOps)(viewCommonData, viewParticipantData, subviews)
      )
      .leftMap(_.message)

  def fromByteString(hashOps: HashOps)(bytes: ByteString): ParsingResult[TransactionView] =
    for {
      protoView <- TreeSerialization.deserializeProtoNode(bytes, v0.ViewNode)
      view <- fromProtoV0(hashOps, protoView)
    } yield view

  private def fromProtoV0(
      hashOps: HashOps,
      protoView: v0.ViewNode,
  ): ParsingResult[TransactionView] = {
    for {
      commonData <- MerkleTree.fromProtoOption(
        protoView.viewCommonData,
        ViewCommonData.fromByteString(hashOps),
      )
      participantData <- MerkleTree.fromProtoOption(
        protoView.viewParticipantData,
        ViewParticipantData.fromByteString(hashOps),
      )
      subViews <- deserializeViews(hashOps)(protoView.subviews)
      view <- create(hashOps)(commonData, participantData, subViews).leftMap(e =>
        ProtoDeserializationError.OtherError(s"Unable to create transaction views: $e")
      )
    } yield view
  }

  private[data] def deserializeViews(
      hashOps: HashOps
  )(protoViews: Seq[v0.BlindableNode]): ParsingResult[Seq[MerkleTree[TransactionView]]] =
    protoViews.traverse(protoView =>
      MerkleTree.fromProtoOption(Some(protoView), fromByteString(hashOps))
    )

  /** Indicates an attempt to create an invalid view. */
  case class InvalidView(message: String) extends RuntimeException(message)

}

/** Tags transaction views where all the view metadata are visible (such as in the views sent to participants).
  *
  * Note that the subviews and their metadata are not guaranteed to be visible.
  */
case class ParticipantTransactionView private (view: TransactionView) extends NoCopy {
  def unwrap: TransactionView = view
  def viewCommonData: ViewCommonData = view.viewCommonData.tryUnwrap
  def viewParticipantData: ViewParticipantData = view.viewParticipantData.tryUnwrap
}

object ParticipantTransactionView {
  private def apply(view: TransactionView) = throw new UnsupportedOperationException(
    "Use the create method instead"
  )

  def create(view: TransactionView): Either[String, ParticipantTransactionView] = {
    val validated = view.viewCommonData.unwrap
      .leftMap(rh => s"Common data blinded (hash $rh)")
      .toValidatedNec
      .product(
        view.viewParticipantData.unwrap
          .leftMap(rh => s"Participant data blinded (hash $rh")
          .toValidatedNec
      )
    validated
      .map(_ => new ParticipantTransactionView(view))
      .toEither
      .leftMap(_.toString)
  }
}

/** Information concerning every '''member''' involved in processing the underlying view.
  *
  * @param threshold If the sum of the weights of the parties approving the view attains the threshold,
  *                  the view is considered approved.
  * @throws ViewCommonData$.InvalidViewCommonData if `threshold` is negative
  */
// This class is a reference example of serialization best practices, demonstrating:
// - handling of object invariants (i.e., the construction of an instance may fail with an exception)
// - memoized serialization, which is required if we need to compute a signature or cryptographic hash of a class
// - use of a Versioned... wrapper when serializing to an anonymous binary format
// Please consult the team if you intend to change the design of serialization.
//
// The constructor and `fromProto...` methods are private to ensure that clients cannot create instances with an incorrect `deserializedFrom` field.
//
// Optional parameters are strongly discouraged, as each parameter needs to be consciously set in a production context.
case class ViewCommonData private (informees: Set[Informee], threshold: Int, salt: Salt)(
    hashOps: HashOps,
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[ViewCommonData](hashOps)
    // The class needs to implement MemoizedEvidence, because we want that serialize always yields the same ByteString.
    // This is to ensure that different participants compute the same hash after receiving a ViewCommonData over the network.
    // (Recall that serialization is in general not guaranteed to be deterministic.)
    with MemoizedEvidence
    // The class implements `HasVersionedWrapper` because we serialize it to an anonymous binary format and need to encode
    // the version of the serialized Protobuf message
    with HasVersionedWrapper[VersionedViewCommonData]
    with HasProtoV0[v0.ViewCommonData]
    with NoCopy {

  // If an object invariant is violated, throw an exception specific to the class.
  // Thus, the exception can be caught during deserialization and translated to a human readable error message.
  if (threshold < 0)
    throw InvalidViewCommonData(s"The threshold must not be negative, but is $threshold.")

  // The toProto... methods are deliberately protected, as they could otherwise be abused to bypass memoization.
  //
  // If another serializable class contains a ViewCommonData, it has to include it as a ByteString
  // (and not as "message ViewCommonData") in its ProtoBuf representation.

  // A `toProtoVersioned` method for a message which only has a single version of the corresponding Protobuf message,
  // typically ignores the version-argument
  // If it has multiple versions, it needs to pattern-match on the versions to decide which version it should embed within the Versioned... wrapper
  override protected def toProtoVersioned(version: ProtocolVersion): VersionedViewCommonData =
    VersionedViewCommonData(VersionedViewCommonData.Version.V0(toProtoV0))

  // We use named parameters, because then the code remains correct even when the ProtoBuf code generator
  // changes the order of parameters.
  override protected def toProtoV0: v0.ViewCommonData =
    v0.ViewCommonData(
      informees = informees.map(_.toProtoV0).toSeq,
      threshold = threshold,
      salt = Some(salt.toProtoV0),
    )

  // TODO(i5768): remove `toByteString` from MemoizedEvidence so `super[HasVersionedWrapper]` is no longer required to avoid infinite recursion
  // When serializing the class to an anonymous binary format, we serialize it to a Versioned... version of the
  // corresponding Protobuf message
  override protected[this] def toByteStringUnmemoized(version: ProtocolVersion): ByteString =
    super[HasVersionedWrapper].toByteString(version)

  override val hashPurpose: HashPurpose = HashPurpose.ViewCommonData

  override def pretty: Pretty[ViewCommonData] = prettyOfClass(
    param("informees", _.informees),
    param("threshold", _.threshold),
    param("salt", _.salt),
  )

  @VisibleForTesting
  private[data] def copy(
      informees: Set[Informee] = this.informees,
      threshold: Int = this.threshold,
      salt: Salt = this.salt,
  ): ViewCommonData =
    new ViewCommonData(informees, threshold, salt)(hashOps, None)
}

object ViewCommonData {

  // Make the auto-generated apply method inaccessible to prevent clients from creating instances with an incorrect
  // `deserializedFrom` field.
  private[this] def apply(informees: Set[Informee], threshold: Int, salt: Salt)(
      hashOps: HashOps,
      deserializedFrom: Option[ByteString],
  ): ViewCommonData =
    throw new UnsupportedOperationException("Use the create/tryCreate methods instead")

  /** Creates a fresh [[ViewCommonData]].
    *
    * @throws ViewCommonData$.InvalidViewCommonData if `threshold` is negative
    */
  // This method is tailored to the case that the caller already knows that the parameters meet the object invariants.
  // Consequently, the method throws an exception on invalid parameters.
  //
  // The "tryCreate" method has the following advantages over the auto-generated "apply" method:
  // - The parameter lists have been flipped to facilitate curried usages.
  // - The deserializedFrom field cannot be set; so it cannot be set incorrectly.
  //
  // The method is called "tryCreate" instead of "apply" for two reasons:
  // - to emphasize that this method may throw an exception
  // - to not confuse the Idea compiler by overloading "apply".
  //   (This is not a problem with this particular class, but it has been a problem with other classes.)
  //
  // The "tryCreate" method is optional.
  // Feel free to omit "tryCreate", if the auto-generated "apply" method is good enough.
  def tryCreate(
      hashOps: HashOps
  )(informees: Set[Informee], threshold: Int, salt: Salt): ViewCommonData =
    // The deserializedFrom field is set to "None" as this is for creating "fresh" instances.
    new ViewCommonData(informees, threshold, salt)(hashOps, None)

  /** Creates a fresh [[ViewCommonData]].
    *
    * Yields `Left(...)` if `threshold` is negative
    */
  // Variant of "tryCreate" that returns Left(...) instead of throwing an exception.
  // This is for callers who *do not know up front* whether the parameters meet the object invariants.
  //
  // Optional method, feel free to omit it.
  def create(
      hashOps: HashOps
  )(informees: Set[Informee], threshold: Int, salt: Salt): Either[String, ViewCommonData] =
    returnLeftWhenInitializationFails(ViewCommonData.tryCreate(hashOps)(informees, threshold, salt))

  private[data] def returnLeftWhenInitializationFails[A](initialization: => A): Either[String, A] =
    Either.catchOnly[InvalidViewCommonData](initialization).leftMap(_.message)

  // The "fromProto..." methods are private for similar reasons as for "toProto...":
  // Note that a "bytes" parameter is needed to initialize "deserializedFrom".
  // The method is private, because it assumes (but does not check) that the "bytes" parameter is a
  // valid serialization of "viewCommonDataP".
  private def fromProtoVersioned(
      hashOps: HashOps
  )(bytes: ByteString, viewCommonDataP: VersionedViewCommonData): ParsingResult[ViewCommonData] =
    viewCommonDataP.version match {
      case VersionedViewCommonData.Version.Empty =>
        Left(FieldNotSet("VersionedViewCommonData.version"))
      case VersionedViewCommonData.Version.V0(data) => fromProtoV0(hashOps)(bytes, data)
    }

  private def fromProtoV0(
      hashOps: HashOps
  )(bytes: ByteString, viewCommonDataP: v0.ViewCommonData): ParsingResult[ViewCommonData] =
    for {
      informees <- viewCommonDataP.informees.traverse(Informee.fromProtoV0)

      salt <- ProtoConverter
        .parseRequired(Salt.fromProtoV0, "salt", viewCommonDataP.salt)
        .leftMap(_.inField("salt"))

      // The constructor of ViewCommandData throws an exception if an object invariant would be violated,
      // which must not escape this method. Therefore we translate the exception to Left(...).
      // We only translate `InvalidViewCommonData` and no other exceptions, because other exceptions
      // indicate a bug in the code and can therefore not be recovered from.
      viewCommonData <- returnLeftWhenInitializationFails(
        new ViewCommonData(informees.toSet, viewCommonDataP.threshold, salt)(hashOps, Some(bytes))
      ).leftMap(ProtoDeserializationError.OtherError(_))
    } yield viewCommonData

  // Unlike "create" and "tryCreate", this method initializes the "deserializedFrom" field with the given byte string.
  // This is to ensure that subsequent calls of "toByteString" yield the same byte string.
  def fromByteString(hashOps: HashOps)(bytes: ByteString): ParsingResult[ViewCommonData] =
    for {
      viewCommonDataP <- TreeSerialization.deserializeProtoNode(bytes, VersionedViewCommonData)
      viewCommonData <- ViewCommonData.fromProtoVersioned(hashOps)(bytes, viewCommonDataP)
    } yield viewCommonData

  /** Indicates an attempt to create an invalid [[ViewCommonData]] */
  case class InvalidViewCommonData(message: String) extends RuntimeException(message)
}

/** Information concerning every '''participant''' involved in processing the underlying view.
  *
  * @param coreInputs  [[LfContractId]] used by the core of the view and not created within the view or its subviews.
  *                    Every contract Id is mapped to its contract instances and their meta-information.
  *                    Contracts are marked as being [[InputContract.consumed]] iff
  *                    they are consumed in the core of the view.
  * @param createdCore associates contract ids created by the core of the view to the corresponding contract
  *                instance. The elements are ordered in execution order.
  * @param archivedFromSubviews The contracts that are created in subviews and archived in the core.
  * @param resolvedKeys Specifies how to resolve ResultKeyNeeded requests from DAMLe (resulting from e.g., fetchByKey,
  *                     lookupByKey) when interpreting the view. The resolved contract IDs must be in the [[coreInputs]].
  *                     [[com.digitalasset.canton.data.FreeKey]] is used only for lookup-by-key nodes.
  * @param actionDescription The description of the root action of the view
  * @param rollbackContext Specifies the location within the rollback hierarchy if a Daml exception was caught
  * @throws ViewParticipantData$.InvalidViewParticipantData
  *         if [[createdCore]] contains two elements with the same contract id,
  *         if [[coreInputs]]`(id).contractId != id`
  *         if [[archivedFromSubviews]] overlaps with [[createdCore]]'s ids or [[coreInputs]]
  *         if [[coreInputs]] does not contain the resolved contract ids of [[resolvedKeys]]
  *         if the [[actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.CreateActionDescription]]
  *         and the created id is not the first contract ID in [[createdCore]]
  *         if the [[actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.ExerciseActionDescription]]
  *         or [[com.digitalasset.canton.data.ActionDescription.FetchActionDescription]] and the input contract is not in [[coreInputs]]
  *         if the [[actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.LookupByKeyActionDescription]]
  *         and the key is not in [[resolvedKeys]].
  * @throws com.digitalasset.canton.serialization.SerializationCheckFailed if this instance cannot be serialized
  */
case class ViewParticipantData private (
    coreInputs: Map[LfContractId, InputContract],
    createdCore: Seq[CreatedContract],
    archivedFromSubviews: Set[LfContractId],
    resolvedKeys: Map[LfGlobalKey, KeyResolution],
    actionDescription: ActionDescription,
    rollbackContext: RollbackContext,
    salt: Salt,
)(hashOps: HashOps, override val deserializedFrom: Option[ByteString])
    extends MerkleTreeLeaf[ViewParticipantData](hashOps)
    with HasVersionedWrapper[VersionedViewParticipantData]
    with MemoizedEvidence
    with NoCopy {
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
      val indices = createdIds.zipWithIndex.filter(_._1 == id).map(_._2)
      s"createdCore contains the contract id $id multiple times at indices ${indices.mkString(", ")}"
    }

    coreInputs.foreach { case (id, usedContract) =>
      if (id != usedContract.contractId)
        throw InvalidViewParticipantData(
          s"Inconsistent ids for used contract: $id and ${usedContract.contractId}"
        )

      if (archivedFromSubviews.contains(id))
        throw InvalidViewParticipantData(
          s"Contracts created in a subview overlap with core inputs: $id"
        )
    }

    val transientOverlap = archivedFromSubviews intersect createdIds.toSet
    if (transientOverlap.nonEmpty)
      throw InvalidViewParticipantData(
        s"Contract created in a subview are also created in the core: $transientOverlap"
      )

    def inconsistentAssignedKey(keyWithResolution: (LfGlobalKey, KeyResolution)): Boolean = {
      val (key, resolution) = keyWithResolution
      resolution.resolution.fold(false) { cid =>
        val inconsistent = for {
          inputContract <- coreInputs.get(cid)
          declaredKey <- inputContract.contract.metadata.maybeKey
        } yield declaredKey != key
        inconsistent.getOrElse(true)
      }
    }
    val keyInconsistencies = resolvedKeys.filter(inconsistentAssignedKey)
    if (keyInconsistencies.nonEmpty) {
      println(show"View participant data: ${resolvedKeys}\n\n${coreInputs}")
      throw InvalidViewParticipantData(show"Inconsistencies for resolved keys: $keyInconsistencies")
    }
  }

  val rootAction: RootAction =
    actionDescription match {
      case CreateActionDescription(contractId, _seed, _version) =>
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
        val contractInst = createdContract.contract.rawContractInstance.contractInstance

        RootAction(
          LfCreateCommand(
            templateId = contractInst.unversioned.template,
            argument = contractInst.unversioned.arg,
          ),
          metadata.signatories,
          failed = false,
        )

      case ExerciseActionDescription(
            inputContractId,
            choice,
            chosenValue,
            actors,
            byKey,
            _seed,
            _version,
            failed,
          ) =>
        val inputContract = coreInputs.getOrElse(
          inputContractId,
          throw InvalidViewParticipantData(
            show"Input contract $inputContractId of the Exercise root action is not declared as core input."
          ),
        )
        val templateId = inputContract.contract.contractInstance.unversioned.template
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
            argument = chosenValue,
          )
        } else {
          LfExerciseCommand(
            templateId = templateId,
            contractId = inputContractId,
            choiceId = choice,
            argument = chosenValue,
          )
        }
        RootAction(cmd, actors, failed)

      case FetchActionDescription(inputContractId, actors, byKey, _version) =>
        val inputContract = coreInputs.getOrElse(
          inputContractId,
          throw InvalidViewParticipantData(
            show"Input contract $inputContractId of the Fetch root action is not declared as core input."
          ),
        )
        val templateId = inputContract.contract.contractInstance.unversioned.template
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
          LfFetchCommand(templateId = templateId, coid = inputContractId)
        }
        RootAction(cmd, actors, failed = false)

      case LookupByKeyActionDescription(key, _version) =>
        val keyResolution = resolvedKeys.getOrElse(
          key,
          throw InvalidViewParticipantData(
            show"Key $key of LookupByKey root action is not resolved."
          ),
        )
        val maintainers = keyResolution match {
          case AssignedKey(contractId) => checked(coreInputs(contractId)).maintainers
          case FreeKey(maintainers) => maintainers
        }

        RootAction(
          LfLookupByKeyCommand(templateId = key.templateId, contractKey = key.key),
          maintainers,
          failed = false,
        )
    }

  override protected def toProtoVersioned(version: ProtocolVersion): VersionedViewParticipantData =
    VersionedViewParticipantData(VersionedViewParticipantData.Version.V0(toProtoV0))

  protected def toProtoV0: v0.ViewParticipantData =
    v0.ViewParticipantData(
      coreInputs = coreInputs.values.map(_.toProtoV0).toSeq,
      createdCore = createdCore.map(_.toProtoV0),
      archivedFromSubviews = archivedFromSubviews.toSeq.map(_.toProtoPrimitive),
      resolvedKeys = resolvedKeys.toList.map { case (k, res) => ResolvedKey(k, res).toProtoV0 },
      actionDescription = Some(actionDescription.toProtoV0),
      rollbackContext = if (rollbackContext.isEmpty) None else Some(rollbackContext.toProtoV0),
      salt = Some(salt.toProtoV0),
    )

  override protected[this] def toByteStringUnmemoized(version: ProtocolVersion): ByteString =
    super[HasVersionedWrapper].toByteString(version)

  override def hashPurpose: HashPurpose = HashPurpose.ViewParticipantData

  override def pretty: Pretty[ViewParticipantData] = prettyOfClass(
    paramIfNonEmpty("core inputs", _.coreInputs),
    paramIfNonEmpty("created core", _.createdCore),
    paramIfNonEmpty("archived from subview", _.archivedFromSubviews),
    paramIfNonEmpty("resolved keys", _.resolvedKeys),
    param("action description", _.actionDescription),
    param("rollback context", _.rollbackContext),
    param("salt", _.salt),
  )
}

object ViewParticipantData {

  private[this] def apply(
      coreInputs: Map[LfContractId, InputContract],
      createdCore: Seq[CreatedContract],
      archivedFromSubviews: Set[LfContractId],
      resolvedKeys: Map[LfGlobalKey, KeyResolution],
      actionDescription: ActionDescription,
      salt: Salt,
  )(hashOps: HashOps, deserializedFrom: Option[ByteString]) =
    throw new UnsupportedOperationException("Use the public apply method instead")

  /** Creates a view participant data.
    *
    * @throws InvalidViewParticipantData
    * if [[ViewParticipantData.createdCore]] contains two elements with the same contract id,
    * if [[ViewParticipantData.coreInputs]]`(id).contractId != id`
    * if [[ViewParticipantData.archivedFromSubviews]] overlaps with [[ViewParticipantData.createdCore]]'s ids or [[ViewParticipantData.coreInputs]]
    * if [[ViewParticipantData.coreInputs]] does not contain the resolved contract ids in [[ViewParticipantData.resolvedKeys]]
    * if [[ViewParticipantData.createdCore]] creates a contract with a key that is not in [[ViewParticipantData.resolvedKeys]]
    * if the [[ViewParticipantData.actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.CreateActionDescription]]
    *   and the created id is not the first contract ID in [[ViewParticipantData.createdCore]]
    * if the [[ViewParticipantData.actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.ExerciseActionDescription]]
    *   or [[com.digitalasset.canton.data.ActionDescription.FetchActionDescription]] and the input contract is not in [[ViewParticipantData.coreInputs]]
    * if the [[ViewParticipantData.actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.LookupByKeyActionDescription]]
    *   and the key is not in [[ViewParticipantData.resolvedKeys]].
    * @throws com.digitalasset.canton.serialization.SerializationCheckFailed if this instance cannot be serialized
    */
  @throws[SerializationCheckFailed[TransactionSerializationError]]
  def apply(hashOps: HashOps)(
      coreInputs: Map[LfContractId, InputContract],
      createdCore: Seq[CreatedContract],
      archivedFromSubveiws: Set[LfContractId],
      resolvedKeys: Map[LfGlobalKey, KeyResolution],
      actionDescription: ActionDescription,
      rollbackContext: RollbackContext,
      salt: Salt,
  ): ViewParticipantData =
    new ViewParticipantData(
      coreInputs,
      createdCore,
      archivedFromSubveiws,
      resolvedKeys,
      actionDescription,
      rollbackContext,
      salt,
    )(hashOps, None)

  /** Creates a view participant data.
    *
    * Yields `Left(...)`
    * if [[ViewParticipantData.createdCore]] contains two elements with the same contract id,
    * if [[ViewParticipantData.coreInputs]]`(id).contractId != id`
    * if [[ViewParticipantData.archivedFromSubviews]] overlaps with [[ViewParticipantData.createdCore]]'s ids or [[ViewParticipantData.coreInputs]]
    * if [[ViewParticipantData.coreInputs]] does not contain the resolved contract ids in [[ViewParticipantData.resolvedKeys]]
    * if [[ViewParticipantData.createdCore]] creates a contract with a key that is not in [[ViewParticipantData.resolvedKeys]]
    * if the [[ViewParticipantData.actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.CreateActionDescription]]
    *   and the created id is not the first contract ID in [[ViewParticipantData.createdCore]]
    * if the [[ViewParticipantData.actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.ExerciseActionDescription]]
    *   or [[com.digitalasset.canton.data.ActionDescription.FetchActionDescription]] and the input contract is not in [[ViewParticipantData.coreInputs]]
    * if the [[ViewParticipantData.actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.LookupByKeyActionDescription]]
    *   and the key is not in [[ViewParticipantData.resolvedKeys]].
    * if this instance cannot be serialized.
    */
  def create(hashOps: HashOps)(
      coreInputs: Map[LfContractId, InputContract],
      createdCore: Seq[CreatedContract],
      archivedFromSubviews: Set[LfContractId],
      resolvedKeys: Map[LfGlobalKey, KeyResolution],
      actionDescription: ActionDescription,
      rollbackContext: RollbackContext,
      salt: Salt,
  ): Either[String, ViewParticipantData] =
    returnLeftWhenInitializationFails(
      ViewParticipantData(hashOps)(
        coreInputs,
        createdCore,
        archivedFromSubviews,
        resolvedKeys,
        actionDescription,
        rollbackContext,
        salt,
      )
    )

  private[this] def returnLeftWhenInitializationFails[A](initialization: => A): Either[String, A] =
    try {
      Right(initialization)
    } catch {
      case InvalidViewParticipantData(message) => Left(message)
      case SerializationCheckFailed(TransactionSerializationError(message)) => Left(message)
    }

  private def fromProtoV0(hashOps: HashOps, dataP: v0.ViewParticipantData)(
      bytes: ByteString
  ): ParsingResult[MerkleTree[ViewParticipantData]] =
    for {
      coreInputsSeq <- dataP.coreInputs.traverse(InputContract.fromProtoV0)
      v0.ViewParticipantData(
        saltP,
        _,
        createdCoreP,
        archivedFromSubviewsP,
        resolvedKeysP,
        actionDescriptionP,
        rbContextP,
      ) = dataP
      coreInputs = coreInputsSeq.map(x => x.contractId -> x).toMap
      createdCore <- createdCoreP.traverse(CreatedContract.fromProtoV0)
      archivedFromSubviews <- archivedFromSubviewsP
        .traverse(LfContractId.fromProtoPrimitive)
      resolvedKeys <- resolvedKeysP.traverse(
        ResolvedKey.fromProtoV0(_).map(rk => rk.key -> rk.resolution)
      )
      resolvedKeysMap = resolvedKeys.toMap
      actionDescription <- ProtoConverter
        .required("action_description", actionDescriptionP)
        .flatMap(ActionDescription.fromProtoV0)

      salt <- ProtoConverter
        .parseRequired(Salt.fromProtoV0, "salt", saltP)
        .leftMap(_.inField("salt"))

      rollbackContext <- RollbackContext
        .fromProtoV0(rbContextP)
        .leftMap(_.inField("rollbackContext"))

      viewParticipantData <- returnLeftWhenInitializationFails(
        new ViewParticipantData(
          coreInputs = coreInputs,
          createdCore = createdCore,
          archivedFromSubviews = archivedFromSubviews.toSet,
          resolvedKeys = resolvedKeysMap,
          actionDescription = actionDescription,
          rollbackContext = rollbackContext,
          salt = salt,
        )(hashOps, Some(bytes))
      ).leftMap(ProtoDeserializationError.OtherError(_))
    } yield viewParticipantData

  private def fromProtoVersioned(hashOps: HashOps, dataP: VersionedViewParticipantData)(
      bytes: ByteString
  ): ParsingResult[MerkleTree[ViewParticipantData]] =
    dataP.version match {
      case VersionedViewParticipantData.Version.Empty =>
        Left(FieldNotSet("VersionedViewParticipantData.version"))
      case VersionedViewParticipantData.Version.V0(data) => fromProtoV0(hashOps, data)(bytes)
    }

  def fromByteString(
      hashOps: HashOps
  )(bytes: ByteString): ParsingResult[MerkleTree[ViewParticipantData]] =
    for {
      protoViewParticipantData <- TreeSerialization.deserializeProtoNode(
        bytes,
        VersionedViewParticipantData,
      )
      viewParticipantData <- fromProtoVersioned(hashOps, protoViewParticipantData)(bytes)
    } yield viewParticipantData

  case class RootAction(command: LfCommand, authorizers: Set[LfPartyId], failed: Boolean)

  /** Indicates an attempt to create an invalid [[ViewParticipantData]]. */
  case class InvalidViewParticipantData(message: String) extends RuntimeException(message)
}
