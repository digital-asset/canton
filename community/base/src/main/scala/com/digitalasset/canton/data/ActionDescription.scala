// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.{
  FieldNotSet,
  OtherError,
  ValueDeserializationError,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.LfHashSyntax.*
import com.digitalasset.canton.protocol.RefIdentifierSyntax.*
import com.digitalasset.canton.protocol.{
  GlobalKeySerialization,
  LfActionNode,
  LfContractId,
  LfGlobalKey,
  LfHash,
  LfNodeCreate,
  LfNodeExercises,
  LfNodeFetch,
  LfNodeQueryByKey,
  LfTemplateId,
  RefIdentifierSyntax,
  v30,
  v31,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfChoiceName, LfInterfaceId, LfPackageId, LfPartyId, LfVersioned}
import com.digitalasset.daml.lf.value.{Value, ValueCoder, ValueOuterClass}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import monocle.macros.{GenLens, GenPrism}
import monocle.{Lens, Prism}

/** Summarizes the information that is needed in addition to the other fields of
  * [[ViewParticipantData]] for determining the root action of a view.
  */
sealed trait ActionDescription extends Product with Serializable with PrettyPrinting {

  /** Whether the root action was a byKey action (exerciseByKey, fetchByKey, lookupByKey) */
  def byKey: Boolean

  /** The node seed for the root action of a view. Empty for fetch and lookupByKey nodes */
  def seedOption: Option[LfHash]

  protected def toProtoDescriptionV30: v30.ActionDescription.Description

  protected def toProtoDescriptionV31: v31.ActionDescription.Description

  def toProtoV30: v30.ActionDescription =
    v30.ActionDescription(description = toProtoDescriptionV30)

  def toProtoV31: v31.ActionDescription =
    v31.ActionDescription(description = toProtoDescriptionV31)

}

object ActionDescription {

  final case class InvalidActionDescription(message: String)
      extends RuntimeException(message)
      with PrettyPrinting {
    override protected def pretty: Pretty[InvalidActionDescription] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  def tryFromLfActionNode(
      actionNode: LfActionNode,
      seedO: Option[LfHash],
      packagePreference: Set[LfPackageId],
  ): ActionDescription =
    fromLfActionNode(actionNode, seedO, packagePreference).valueOr(err => throw err)

  /** Extracts the action description from an LF node and the optional seed.
    * @param seedO
    *   Must be set iff `node` is a [[com.digitalasset.canton.protocol.LfNodeCreate]] or
    *   [[com.digitalasset.canton.protocol.LfNodeExercises]].
    */
  def fromLfActionNode(
      actionNode: LfActionNode,
      seedO: Option[LfHash],
      packagePreference: Set[LfPackageId],
  ): Either[InvalidActionDescription, ActionDescription] =
    actionNode match {
      case LfNodeCreate(
            contractId,
            _packageName,
            _templateId,
            _arg,
            _signatories,
            _stakeholders,
            _key,
            version,
          ) =>
        for {
          seed <- seedO.toRight(InvalidActionDescription("No seed for a Create node given"))
        } yield CreateActionDescription(contractId, seed)

      case LfNodeExercises(
            inputContract,
            _packageName,
            templateId,
            interfaceId,
            choice,
            _consuming,
            actors,
            chosenValue,
            _stakeholders,
            _signatories,
            _choiceObservers,
            _choiceAuthorizers,
            _children,
            exerciseResult,
            _key,
            byKey,
            version,
          ) =>
        for {
          seed <- seedO.toRight(InvalidActionDescription("No seed for an Exercise node given"))
          actionDescription <- ExerciseActionDescription.create(
            inputContract,
            templateId,
            choice,
            interfaceId,
            packagePreference,
            LfVersioned(version, chosenValue),
            actors,
            byKey,
            seed,
            failed = exerciseResult.isEmpty, // absence of exercise result indicates failure
          )
        } yield actionDescription

      case LfNodeFetch(
            inputContract,
            _packageName,
            templateId,
            actingParties,
            _signatories,
            _stakeholders,
            _key,
            byKey,
            interfaceId,
            version,
          ) =>
        for {
          _ <- Either.cond(
            seedO.isEmpty,
            (),
            InvalidActionDescription("No seed should be given for a Fetch node"),
          )
          actors <- Either.cond(
            actingParties.nonEmpty,
            actingParties,
            InvalidActionDescription("Fetch node without acting parties"),
          )
        } yield FetchActionDescription(inputContract, actors, byKey, templateId, interfaceId)

      case _: LfNodeQueryByKey =>
        Left(InvalidActionDescription("QueryByKey nodes are not supported as root nodes of views"))

    }

  private def fromCreateProtoV30(
      c: v30.ActionDescription.CreateActionDescription
  ): ParsingResult[CreateActionDescription] = {
    val v30.ActionDescription.CreateActionDescription(contractIdP, seedP) = c
    for {
      contractId <- ProtoConverter.parseLfContractId(contractIdP)
      seed <- LfHash.fromProtoPrimitive("node_seed", seedP)
    } yield CreateActionDescription(contractId, seed)
  }

  private def choiceFromProto(choiceP: String): ParsingResult[LfChoiceName] =
    LfChoiceName
      .fromString(choiceP)
      .leftMap(err => ValueDeserializationError("choice", err))

  private def fromExerciseProtoV30(
      e: v30.ActionDescription.ExerciseActionDescription
  ): ParsingResult[ExerciseActionDescription] = {
    val v30.ActionDescription.ExerciseActionDescription(
      inputContractIdP,
      choiceP,
      chosenValueB,
      actorsP,
      byKey,
      seedP,
      failed,
      interfaceIdP,
      templateIdP,
      packagePreferenceP,
    ) = e
    for {
      inputContractId <- ProtoConverter.parseLfContractId(inputContractIdP)
      templateId <- RefIdentifierSyntax.fromProtoPrimitive(templateIdP)
      packagePreference <- packagePreferenceP.traverse(ProtoConverter.parsePackageId).map(_.toSet)
      choice <- choiceFromProto(choiceP)
      interfaceId <- interfaceIdP.traverse(RefIdentifierSyntax.fromProtoPrimitive)
      chosenValueP <- ProtoConverter.protoParser(ValueOuterClass.VersionedValue.parseFrom)(
        chosenValueB
      )
      chosenValue <- ValueCoder
        .decodeVersionedValue(chosenValueP)
        .leftMap(err => ValueDeserializationError("chosen_value", err.errorMessage))
      actors <- actorsP.traverse(ProtoConverter.parseLfPartyId(_, field = "actors")).map(_.toSet)
      seed <- LfHash.fromProtoPrimitive("node_seed", seedP)
      actionDescription <- ExerciseActionDescription
        .create(
          inputContractId,
          templateId,
          choice,
          interfaceId,
          packagePreference,
          chosenValue,
          actors,
          byKey,
          seed,
          failed,
        )
        .leftMap(err => OtherError(err.message))
    } yield actionDescription
  }

  private def fromLookupByKeyProtoV30(
      k: v30.ActionDescription.LookupByKeyActionDescription
  ): ParsingResult[LookupByKeyActionDescription] = {
    val v30.ActionDescription.LookupByKeyActionDescription(keyP) = k
    for {
      key <- ProtoConverter
        .required("key", keyP)
        .flatMap(GlobalKeySerialization.fromProtoV30)
      actionDescription <- LookupByKeyActionDescription
        .create(key)
        .leftMap(err => OtherError(err.message))
    } yield actionDescription
  }

  private def fromFetchProtoV30(
      f: v30.ActionDescription.FetchActionDescription
  ): ParsingResult[FetchActionDescription] = {
    val v30.ActionDescription.FetchActionDescription(
      inputContractIdP,
      actorsP,
      byKey,
      templateIdP,
      interfaceIdP,
    ) = f
    for {
      inputContractId <- ProtoConverter.parseLfContractId(inputContractIdP)
      actors <- actorsP.traverse(ProtoConverter.parseLfPartyId(_, field = "actors")).map(_.toSet)
      templateId <- RefIdentifierSyntax.fromProtoPrimitive(templateIdP)
      interfaceId <- interfaceIdP.traverse(RefIdentifierSyntax.fromProtoPrimitive)
    } yield FetchActionDescription(inputContractId, actors, byKey, templateId, interfaceId)
  }

  private[data] def fromProtoV30(
      actionDescriptionP: v30.ActionDescription
  ): ParsingResult[ActionDescription] = {
    import v30.ActionDescription.Description.*
    val v30.ActionDescription(description) = actionDescriptionP

    description match {
      case Create(create) => fromCreateProtoV30(create)
      case Exercise(exercise) => fromExerciseProtoV30(exercise)
      case Fetch(fetch) => fromFetchProtoV30(fetch)
      case LookupByKey(lookup) => fromLookupByKeyProtoV30(lookup)
      case Empty => Left(FieldNotSet("description"))
    }
  }

  private[data] def fromProtoV31(
      actionDescriptionP: v31.ActionDescription
  ): ParsingResult[ActionDescription] = {
    import v31.ActionDescription.Description.*
    val v31.ActionDescription(description) = actionDescriptionP

    description match {
      case Create(create) => fromCreateProtoV30(create)
      case Exercise(exercise) => fromExerciseProtoV30(exercise)
      case Fetch(fetch) => fromFetchProtoV30(fetch)
      case Empty => Left(FieldNotSet("description"))
    }

  }

  def serializeChosenValue(
      chosenValue: LfVersioned[Value]
  ): Either[String, ByteString] =
    ValueCoder
      .encodeVersionedValue(chosenValue)
      .map(_.toByteString)
      .leftMap(_.errorMessage)

  final case class CreateActionDescription(
      contractId: LfContractId,
      seed: LfHash,
  ) extends ActionDescription {
    override def byKey: Boolean = false

    override def seedOption: Option[LfHash] = Some(seed)

    private def toCreateActionDescriptionV30 =
      v30.ActionDescription.CreateActionDescription(
        contractId = contractId.toProtoPrimitive,
        nodeSeed = seed.toProtoPrimitive,
      )

    override protected def toProtoDescriptionV30: v30.ActionDescription.Description.Create =
      v30.ActionDescription.Description.Create(toCreateActionDescriptionV30)

    override protected def toProtoDescriptionV31: v31.ActionDescription.Description.Create =
      v31.ActionDescription.Description.Create(toCreateActionDescriptionV30)

    override protected def pretty: Pretty[CreateActionDescription] = prettyOfClass(
      param("contract Id", _.contractId),
      param("seed", _.seed),
    )
  }

  /** @throws InvalidActionDescription if the `chosen_value` cannot be serialized */
  final case class ExerciseActionDescription private (
      inputContractId: LfContractId,
      templateId: LfTemplateId,
      choice: LfChoiceName,
      interfaceId: Option[LfInterfaceId],
      packagePreference: Set[LfPackageId],
      chosenValue: LfVersioned[Value],
      actors: Set[LfPartyId],
      override val byKey: Boolean,
      seed: LfHash,
      failed: Boolean,
  ) extends ActionDescription {

    private val serializedChosenValue: ByteString = serializeChosenValue(chosenValue)
      .valueOr(err => throw InvalidActionDescription(s"Failed to serialize chosen value: $err"))

    override def seedOption: Option[LfHash] = Some(seed)

    override protected def toProtoDescriptionV30: v30.ActionDescription.Description.Exercise =
      v30.ActionDescription.Description.Exercise(toExerciseActionDescriptionV30)

    override protected def toProtoDescriptionV31: v31.ActionDescription.Description.Exercise =
      v31.ActionDescription.Description.Exercise(toExerciseActionDescriptionV30)

    private def toExerciseActionDescriptionV30: v30.ActionDescription.ExerciseActionDescription =
      v30.ActionDescription.ExerciseActionDescription(
        inputContractId = inputContractId.toProtoPrimitive,
        templateId = new RefIdentifierSyntax(templateId).toProtoPrimitive,
        packagePreference = packagePreference.toSeq,
        choice = choice,
        interfaceId = interfaceId.map(i => new RefIdentifierSyntax(i).toProtoPrimitive),
        chosenValue = serializedChosenValue,
        actors = actors.toSeq,
        byKey = byKey,
        nodeSeed = seed.toProtoPrimitive,
        failed = failed,
      )

    override protected def pretty: Pretty[ExerciseActionDescription] = prettyOfClass(
      param("input contract id", _.inputContractId),
      param("template id", _.templateId),
      paramIfDefined("interface id", _.interfaceId),
      param("choice", _.choice.unquoted),
      param("chosen value", _.chosenValue),
      param("actors", _.actors),
      paramIfTrue("by key", _.byKey),
      param("seed", _.seed),
      paramIfTrue("failed", _.failed),
    )

    @VisibleForTesting
    private[data] def copy(
        inputContractId: LfContractId = this.inputContractId,
        templateId: LfTemplateId = this.templateId,
        choice: LfChoiceName = this.choice,
        interfaceId: Option[LfInterfaceId] = this.interfaceId,
        packagePreference: Set[LfPackageId] = this.packagePreference,
        chosenValue: LfVersioned[Value] = this.chosenValue,
        actors: Set[LfPartyId] = this.actors,
        byKey: Boolean = this.byKey,
        seed: LfHash = this.seed,
        failed: Boolean = this.failed,
    ): ExerciseActionDescription =
      ExerciseActionDescription(
        inputContractId,
        templateId,
        choice,
        interfaceId,
        packagePreference,
        chosenValue,
        actors,
        byKey,
        seed,
        failed,
      )

  }

  object ExerciseActionDescription {
    def tryCreate(
        inputContractId: LfContractId,
        templateId: LfTemplateId,
        choice: LfChoiceName,
        interfaceId: Option[LfInterfaceId],
        packagePreference: Set[LfPackageId],
        chosenValue: LfVersioned[Value],
        actors: Set[LfPartyId],
        byKey: Boolean,
        seed: LfHash,
        failed: Boolean,
    ): ExerciseActionDescription = create(
      inputContractId,
      templateId,
      choice,
      interfaceId,
      packagePreference,
      chosenValue,
      actors,
      byKey,
      seed,
      failed,
    ).fold(err => throw err, identity)

    def create(
        inputContractId: LfContractId,
        templateId: LfTemplateId,
        choice: LfChoiceName,
        interfaceId: Option[LfInterfaceId],
        packagePreference: Set[LfPackageId],
        chosenValue: LfVersioned[Value],
        actors: Set[LfPartyId],
        byKey: Boolean,
        seed: LfHash,
        failed: Boolean,
    ): Either[InvalidActionDescription, ExerciseActionDescription] =
      Either.catchOnly[InvalidActionDescription](
        ExerciseActionDescription(
          inputContractId,
          templateId,
          choice,
          interfaceId,
          packagePreference,
          chosenValue,
          actors,
          byKey,
          seed,
          failed,
        )
      )

    /** DO NOT USE IN PRODUCTION, as it does not necessarily check object invariants. */
    @VisibleForTesting
    object Optics {
      val packagePreferenceUnsafe: Lens[ExerciseActionDescription, Set[LfPackageId]] =
        GenLens[ExerciseActionDescription].apply(_.packagePreference)
      val templateIdUnsafe: Lens[ExerciseActionDescription, LfTemplateId] =
        GenLens[ExerciseActionDescription].apply(_.templateId)
      val choiceUnsafe: Lens[ExerciseActionDescription, LfChoiceName] =
        GenLens[ExerciseActionDescription].apply(_.choice)
    }
  }

  final case class FetchActionDescription(
      inputContractId: LfContractId,
      actors: Set[LfPartyId],
      override val byKey: Boolean,
      templateId: LfTemplateId,
      interfaceId: Option[LfTemplateId],
  ) extends ActionDescription
      with NoCopy {

    override def seedOption: Option[LfHash] = None

    // Fetch nodes that have been dispatched via a interface have an implicit package preference
    def packagePreference: Set[LfPackageId] =
      if (interfaceId.nonEmpty) Set(templateId.packageId) else Set.empty

    private def toFetchActionDescriptionV30: v30.ActionDescription.FetchActionDescription =
      v30.ActionDescription.FetchActionDescription(
        inputContractId = inputContractId.toProtoPrimitive,
        actors = actors.toSeq,
        byKey = byKey,
        templateId = new RefIdentifierSyntax(templateId).toProtoPrimitive,
        interfaceId = interfaceId.map(i => new RefIdentifierSyntax(i).toProtoPrimitive),
      )

    override protected def toProtoDescriptionV30: v30.ActionDescription.Description.Fetch =
      v30.ActionDescription.Description.Fetch(toFetchActionDescriptionV30)

    override protected def toProtoDescriptionV31: v31.ActionDescription.Description.Fetch =
      v31.ActionDescription.Description.Fetch(toFetchActionDescriptionV30)

    override protected def pretty: Pretty[FetchActionDescription] = prettyOfClass(
      param("input contract id", _.inputContractId),
      param("actors", _.actors),
      paramIfTrue("by key", _.byKey),
      paramIfDefined("interface id", _.interfaceId),
    )
  }

  final case class LookupByKeyActionDescription(key: LfVersioned[LfGlobalKey])
      extends ActionDescription {

    private val serializedKey =
      GlobalKeySerialization
        .toProtoV30(key)
        .valueOr(err => throw InvalidActionDescription(s"Failed to serialize key: $err"))

    override def byKey: Boolean = true

    override def seedOption: Option[LfHash] = None

    protected def toProtoDescriptionV30: v30.ActionDescription.Description.LookupByKey =
      v30.ActionDescription.Description.LookupByKey(
        v30.ActionDescription.LookupByKeyActionDescription(
          key = Some(serializedKey)
        )
      )

    override protected def toProtoDescriptionV31: v31.ActionDescription.Description =
      throw InvalidActionDescription(
        s"LookupByKey is not supported as root view action in ${ProtocolVersion.v35} or above"
      )

    override protected def pretty: Pretty[LookupByKeyActionDescription] = prettyOfClass(
      param("key", _.key)
    )

  }

  object LookupByKeyActionDescription {
    def tryCreate(key: LfVersioned[LfGlobalKey]): LookupByKeyActionDescription =
      new LookupByKeyActionDescription(key)

    def create(
        key: LfVersioned[LfGlobalKey]
    ): Either[InvalidActionDescription, LookupByKeyActionDescription] =
      Either.catchOnly[InvalidActionDescription](tryCreate(key))

  }

  @VisibleForTesting
  object Optics {
    val create: Prism[ActionDescription, CreateActionDescription] =
      GenPrism[ActionDescription, CreateActionDescription]
    val exercise: Prism[ActionDescription, ExerciseActionDescription] =
      GenPrism[ActionDescription, ExerciseActionDescription]
    val fetch: Prism[ActionDescription, FetchActionDescription] =
      GenPrism[ActionDescription, FetchActionDescription]
  }

}
