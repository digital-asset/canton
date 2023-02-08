// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.lf.transaction.TransactionVersion
import com.daml.lf.value.{Value, ValueCoder}
import com.digitalasset.canton.ProtoDeserializationError.{
  FieldNotSet,
  OtherError,
  ValueDeserializationError,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.InterfaceIdSyntax.*
import com.digitalasset.canton.protocol.LfHashSyntax.*
import com.digitalasset.canton.protocol.{
  GlobalKeySerialization,
  InterfaceIdSyntax,
  LfActionNode,
  LfContractId,
  LfGlobalKey,
  LfHash,
  LfNodeCreate,
  LfNodeExercises,
  LfNodeFetch,
  LfNodeLookupByKey,
  LfTransactionVersion,
  v0,
  v1,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{LfTransactionUtil, NoCopy}
import com.digitalasset.canton.version.{
  HasProtocolVersionedCompanion,
  HasProtocolVersionedWrapper,
  HasProtocolVersionedWrapperCompanion,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.digitalasset.canton.{LfChoiceName, LfInterfaceId, LfPartyId, LfVersioned}
import com.google.protobuf.ByteString

/** Summarizes the information that is needed in addition to the other fields of [[ViewParticipantData]] for
  * determining the root action of a view.
  */
sealed trait ActionDescription
    extends Product
    with Serializable
    with PrettyPrinting
    with HasProtocolVersionedWrapper[ActionDescription] {

  /** Whether the root action was a byKey action (exerciseByKey, fetchByKey, lookupByKey) */
  def byKey: Boolean

  /** The node seed for the root action of a view. Empty for fetch and lookupByKey nodes */
  def seedOption: Option[LfHash]

  /** The lf transaction version of the node */
  def version: LfTransactionVersion

  override protected def companionObj: HasProtocolVersionedWrapperCompanion[ActionDescription] =
    ActionDescription

  protected def toProtoDescriptionV0: v0.ActionDescription.Description
  protected def toProtoDescriptionV1: v1.ActionDescription.Description

  def toProtoV0: v0.ActionDescription =
    v0.ActionDescription(description = toProtoDescriptionV0)

  def toProtoV1: v1.ActionDescription =
    v1.ActionDescription(description = toProtoDescriptionV1)
}

object ActionDescription extends HasProtocolVersionedCompanion[ActionDescription] {
  override lazy val name: String = "ActionDescription"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v3,
      supportedProtoVersion(v0.ActionDescription)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(
      ProtocolVersion.v4,
      supportedProtoVersion(v1.ActionDescription)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  case class InvalidActionDescription(message: String)
      extends RuntimeException(message)
      with PrettyPrinting {
    override def pretty: Pretty[InvalidActionDescription] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  def tryFromLfActionNode(
      actionNode: LfActionNode,
      seedO: Option[LfHash],
      protocolVersion: ProtocolVersion,
  ): ActionDescription =
    fromLfActionNode(actionNode, seedO, protocolVersion).valueOr(err => throw err)

  /** Extracts the action description from an LF node and the optional seed.
    * @param seedO Must be set iff `node` is a [[com.digitalasset.canton.protocol.LfNodeCreate]] or [[com.digitalasset.canton.protocol.LfNodeExercises]].
    */
  def fromLfActionNode(
      actionNode: LfActionNode,
      seedO: Option[LfHash],
      protocolVersion: ProtocolVersion,
  ): Either[InvalidActionDescription, ActionDescription] =
    actionNode match {
      case LfNodeCreate(
            contractId,
            _templateId,
            _arg,
            _agreementText,
            _signatories,
            _stakeholders,
            _key,
            version,
          ) =>
        for {
          seed <- seedO.toRight(InvalidActionDescription("No seed for a Create node given"))
        } yield CreateActionDescription(contractId, seed, version)(
          protocolVersionRepresentativeFor(protocolVersion)
        )

      case LfNodeExercises(
            inputContract,
            _templateId,
            interfaceId,
            choice,
            _consuming,
            actors,
            chosenValue,
            _stakeholders,
            _signatories,
            _choiceObservers,
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
            choice,
            interfaceId,
            chosenValue,
            actors,
            byKey,
            seed,
            version,
            failed = exerciseResult.isEmpty, // absence of exercise result indicates failure
            protocolVersionRepresentativeFor(protocolVersion),
          )
        } yield actionDescription

      case LfNodeFetch(
            inputContract,
            _templateId,
            actingParties,
            _signatories,
            _stakeholders,
            _key,
            byKey,
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
        } yield FetchActionDescription(inputContract, actors, byKey, version)(
          protocolVersionRepresentativeFor(protocolVersion)
        )

      case LfNodeLookupByKey(templateId, key, _result, version) =>
        for {
          _ <- Either.cond(
            seedO.isEmpty,
            (),
            InvalidActionDescription("No seed should be given for a LookupByKey node"),
          )
          keyWithMaintainers <- LfTransactionUtil
            .globalKeyWithMaintainers(templateId, key)
            .leftMap(coid =>
              InvalidActionDescription(
                show"Contract ID $coid found in contract key for template $templateId"
              )
            )
          actionDescription <- LookupByKeyActionDescription.create(
            keyWithMaintainers.globalKey,
            version,
            protocolVersionRepresentativeFor(protocolVersion),
          )
        } yield actionDescription
    }

  private def fromCreateProtoV0(
      c: v0.ActionDescription.CreateActionDescription
  ): ParsingResult[CreateActionDescription] = {
    val v0.ActionDescription.CreateActionDescription(contractIdP, seedP, versionP) = c
    for {
      contractId <- ProtoConverter.parseLfContractId(contractIdP)
      seed <- LfHash.fromProtoPrimitive("node_seed", seedP)
      version <- lfVersionFromProtoVersioned(versionP)
    } yield CreateActionDescription(contractId, seed, version)(
      protocolVersionRepresentativeFor(ProtoVersion(0))
    )
  }

  private def choiceFromProto(choiceP: String): ParsingResult[LfChoiceName] =
    LfChoiceName
      .fromString(choiceP)
      .leftMap(err => ValueDeserializationError("choice", err))

  private def fromExerciseProtoV0(
      e: v0.ActionDescription.ExerciseActionDescription
  ): ParsingResult[ExerciseActionDescription] = {
    val v0.ActionDescription
      .ExerciseActionDescription(
        inputContractIdP,
        choiceP,
        chosenValueB,
        actorsP,
        byKey,
        seedP,
        versionP,
        failed,
      ) = e
    for {
      inputContractId <- ProtoConverter.parseLfContractId(inputContractIdP)
      choice <- choiceFromProto(choiceP)
      interfaceId = None
      version <- lfVersionFromProtoVersioned(versionP)
      chosenValue <- ValueCoder
        .decodeValue(ValueCoder.CidDecoder, version, chosenValueB)
        .leftMap(err => ValueDeserializationError("chosen_value", err.errorMessage))
      actors <- actorsP.traverse(ProtoConverter.parseLfPartyId).map(_.toSet)
      seed <- LfHash.fromProtoPrimitive("node_seed", seedP)
      actionDescription <- ExerciseActionDescription
        .create(
          inputContractId,
          choice,
          interfaceId,
          chosenValue,
          actors,
          byKey,
          seed,
          version,
          failed,
          protocolVersionRepresentativeFor(ProtoVersion(0)),
        )
        .leftMap(err => OtherError(err.message))
    } yield actionDescription
  }

  private def fromExerciseProtoV1(
      e: v1.ActionDescription.ExerciseActionDescription
  ): ParsingResult[ExerciseActionDescription] = {
    val v1.ActionDescription
      .ExerciseActionDescription(
        inputContractIdP,
        choiceP,
        chosenValueB,
        actorsP,
        byKey,
        seedP,
        versionP,
        failed,
        interfaceIdP,
      ) = e
    for {
      inputContractId <- ProtoConverter.parseLfContractId(inputContractIdP)
      choice <- choiceFromProto(choiceP)
      interfaceId <- interfaceIdP.traverse(InterfaceIdSyntax.fromProtoPrimitive)
      version <- lfVersionFromProtoVersioned(versionP)
      chosenValue <- ValueCoder
        .decodeValue(ValueCoder.CidDecoder, version, chosenValueB)
        .leftMap(err => ValueDeserializationError("chosen_value", err.errorMessage))
      actors <- actorsP.traverse(ProtoConverter.parseLfPartyId).map(_.toSet)
      seed <- LfHash.fromProtoPrimitive("node_seed", seedP)
      actionDescription <- ExerciseActionDescription
        .create(
          inputContractId,
          choice,
          interfaceId,
          chosenValue,
          actors,
          byKey,
          seed,
          version,
          failed,
          protocolVersionRepresentativeFor(ProtoVersion(1)),
        )
        .leftMap(err => OtherError(err.message))
    } yield actionDescription
  }

  private def fromLookupByKeyProtoV0(
      k: v0.ActionDescription.LookupByKeyActionDescription
  ): ParsingResult[LookupByKeyActionDescription] = {
    val v0.ActionDescription.LookupByKeyActionDescription(keyP) = k
    for {
      key <- ProtoConverter
        .required("key", keyP)
        .flatMap(GlobalKeySerialization.fromProtoV0)
      actionDescription <- LookupByKeyActionDescription
        .create(key.unversioned, key.version, protocolVersionRepresentativeFor(ProtoVersion(0)))
        .leftMap(err => OtherError(err.message))
    } yield actionDescription
  }

  private def fromFetchProtoV0(
      f: v0.ActionDescription.FetchActionDescription
  ): ParsingResult[FetchActionDescription] = {
    val v0.ActionDescription.FetchActionDescription(inputContractIdP, actorsP, byKey, versionP) = f
    for {
      inputContractId <- ProtoConverter.parseLfContractId(inputContractIdP)
      actors <- actorsP.traverse(ProtoConverter.parseLfPartyId).map(_.toSet)
      version <- lfVersionFromProtoVersioned(versionP)
    } yield FetchActionDescription(inputContractId, actors, byKey, version)(
      protocolVersionRepresentativeFor(ProtoVersion(0))
    )
  }

  private[data] def fromProtoV0(
      actionDescriptionP: v0.ActionDescription
  ): ParsingResult[ActionDescription] = {
    import v0.ActionDescription.Description.*
    val v0.ActionDescription(description) = actionDescriptionP
    description match {
      case Create(create) => fromCreateProtoV0(create)
      case Exercise(exercise) => fromExerciseProtoV0(exercise)
      case Fetch(fetch) => fromFetchProtoV0(fetch)
      case LookupByKey(lookup) => fromLookupByKeyProtoV0(lookup)
      case Empty => Left(FieldNotSet("description"))
    }
  }

  private[data] def fromProtoV1(
      actionDescriptionP: v1.ActionDescription
  ): ParsingResult[ActionDescription] = {
    import v1.ActionDescription.Description.*
    val v1.ActionDescription(description) = actionDescriptionP
    description match {
      case Create(create) => fromCreateProtoV0(create)
      case Exercise(exercise) => fromExerciseProtoV1(exercise)
      case Fetch(fetch) => fromFetchProtoV0(fetch)
      case LookupByKey(lookup) => fromLookupByKeyProtoV0(lookup)
      case Empty => Left(FieldNotSet("description"))
    }
  }

  private def lfVersionFromProtoVersioned(
      versionP: String
  ): ParsingResult[LfTransactionVersion] = TransactionVersion.All
    .find(_.protoValue == versionP)
    .toRight(s"Unsupported transaction version ${versionP}")
    .leftMap(ValueDeserializationError("version", _))

  def serializeChosenValue(
      chosenValue: Value,
      transactionVersion: LfTransactionVersion,
  ): Either[String, ByteString] =
    ValueCoder
      .encodeValue(ValueCoder.CidEncoder, transactionVersion, chosenValue)
      .leftMap(_.errorMessage)

  case class CreateActionDescription(
      contractId: LfContractId,
      seed: LfHash,
      override val version: LfTransactionVersion,
  )(val representativeProtocolVersion: RepresentativeProtocolVersion[ActionDescription])
      extends ActionDescription {
    override def byKey: Boolean = false

    override def seedOption: Option[LfHash] = Some(seed)

    protected override def toProtoDescriptionV0: v0.ActionDescription.Description.Create =
      v0.ActionDescription.Description.Create(
        v0.ActionDescription.CreateActionDescription(
          contractId = contractId.toProtoPrimitive,
          nodeSeed = seed.toProtoPrimitive,
          version = version.protoValue,
        )
      )

    override protected def toProtoDescriptionV1: v1.ActionDescription.Description.Create =
      v1.ActionDescription.Description.Create(toProtoDescriptionV0.value)

    override def pretty: Pretty[CreateActionDescription] = prettyOfClass(
      param("contract Id", _.contractId),
      param("seed", _.seed),
      param("version", _.version),
    )
  }

  /** @throws InvalidActionDescription if the `chosen_value` cannot be serialized */
  final case class ExerciseActionDescription private (
      inputContractId: LfContractId,
      choice: LfChoiceName,
      interfaceId: Option[LfInterfaceId],
      chosenValue: Value,
      actors: Set[LfPartyId],
      override val byKey: Boolean,
      seed: LfHash,
      override val version: LfTransactionVersion,
      failed: Boolean,
  )(val representativeProtocolVersion: RepresentativeProtocolVersion[ActionDescription])
      extends ActionDescription {

    private val serializedChosenValue: ByteString = serializeChosenValue(chosenValue, version)
      .valueOr(err => throw InvalidActionDescription(s"Failed to serialize chosen value: ${err}"))

    override def seedOption: Option[LfHash] = Some(seed)

    protected override def toProtoDescriptionV0: v0.ActionDescription.Description.Exercise =
      v0.ActionDescription.Description.Exercise(
        v0.ActionDescription.ExerciseActionDescription(
          inputContractId = inputContractId.toProtoPrimitive,
          choice = choice,
          chosenValue = serializedChosenValue,
          actors = actors.toSeq,
          byKey = byKey,
          nodeSeed = seed.toProtoPrimitive,
          version = version.protoValue,
          failed = failed,
        )
      )

    override protected def toProtoDescriptionV1: v1.ActionDescription.Description.Exercise =
      v1.ActionDescription.Description.Exercise(
        v1.ActionDescription.ExerciseActionDescription(
          inputContractId = inputContractId.toProtoPrimitive,
          choice = choice,
          interfaceId = interfaceId.map(_.toProtoPrimitive),
          chosenValue = serializedChosenValue,
          actors = actors.toSeq,
          byKey = byKey,
          nodeSeed = seed.toProtoPrimitive,
          version = version.protoValue,
          failed = failed,
        )
      )

    override def pretty: Pretty[ExerciseActionDescription] = prettyOfClass(
      param("input contract id", _.inputContractId),
      param("choice", _.choice.unquoted),
      param("chosen value", _.chosenValue),
      param("actors", _.actors),
      paramIfTrue("by key", _.byKey),
      param("seed", _.seed),
      param("version", _.version),
      paramIfTrue("failed", _.failed),
    )
  }

  object ExerciseActionDescription {
    def tryCreate(
        inputContractId: LfContractId,
        choice: LfChoiceName,
        interfaceId: Option[LfInterfaceId],
        chosenValue: Value,
        actors: Set[LfPartyId],
        byKey: Boolean,
        seed: LfHash,
        version: LfTransactionVersion,
        failed: Boolean,
        protocolVersion: RepresentativeProtocolVersion[ActionDescription],
    ): ExerciseActionDescription = create(
      inputContractId,
      choice,
      interfaceId,
      chosenValue,
      actors,
      byKey,
      seed,
      version,
      failed,
      protocolVersion,
    ).fold(err => throw err, identity)

    def create(
        inputContractId: LfContractId,
        choice: LfChoiceName,
        interfaceId: Option[LfInterfaceId],
        chosenValue: Value,
        actors: Set[LfPartyId],
        byKey: Boolean,
        seed: LfHash,
        version: LfTransactionVersion,
        failed: Boolean,
        protocolVersion: RepresentativeProtocolVersion[ActionDescription],
    ): Either[InvalidActionDescription, ExerciseActionDescription] = {
      val hasInterface = interfaceId.nonEmpty
      val interfaceSupportedSince = ProtocolVersion.v4
      val canHaveInterface = protocolVersion.representative >= interfaceSupportedSince

      for {
        _ <- Either.cond(
          !hasInterface || canHaveInterface,
          (),
          InvalidActionDescription(
            s"Protocol version is equivalent to ${protocolVersion.representative} but interface id is supported since protocol version $interfaceSupportedSince"
          ),
        )

        action <- Either.catchOnly[InvalidActionDescription](
          ExerciseActionDescription(
            inputContractId,
            choice,
            interfaceId,
            chosenValue,
            actors,
            byKey,
            seed,
            version,
            failed,
          )(protocolVersion)
        )
      } yield action
    }
  }

  case class FetchActionDescription(
      inputContractId: LfContractId,
      actors: Set[LfPartyId],
      override val byKey: Boolean,
      override val version: LfTransactionVersion,
  )(val representativeProtocolVersion: RepresentativeProtocolVersion[ActionDescription])
      extends ActionDescription
      with NoCopy {

    override def seedOption: Option[LfHash] = None

    protected override def toProtoDescriptionV0: v0.ActionDescription.Description.Fetch =
      v0.ActionDescription.Description.Fetch(
        v0.ActionDescription.FetchActionDescription(
          inputContractId = inputContractId.toProtoPrimitive,
          actors = actors.toSeq,
          byKey = byKey,
          version = version.protoValue,
        )
      )

    override protected def toProtoDescriptionV1: v1.ActionDescription.Description.Fetch =
      v1.ActionDescription.Description.Fetch(toProtoDescriptionV0.value)

    override def pretty: Pretty[FetchActionDescription] = prettyOfClass(
      param("input contract id", _.inputContractId),
      param("actors", _.actors),
      paramIfTrue("by key", _.byKey),
      param("version", _.version),
    )
  }

  case class LookupByKeyActionDescription private (
      key: LfGlobalKey,
      override val version: LfTransactionVersion,
  )(val representativeProtocolVersion: RepresentativeProtocolVersion[ActionDescription])
      extends ActionDescription {

    private val serializedKey =
      GlobalKeySerialization
        .toProto(LfVersioned(version, key))
        .valueOr(err => throw InvalidActionDescription(s"Failed to serialize key: $err"))

    override def byKey: Boolean = true

    override def seedOption: Option[LfHash] = None

    protected override def toProtoDescriptionV0: v0.ActionDescription.Description.LookupByKey =
      v0.ActionDescription.Description.LookupByKey(
        v0.ActionDescription.LookupByKeyActionDescription(
          key = Some(serializedKey)
        )
      )

    override protected def toProtoDescriptionV1: v1.ActionDescription.Description.LookupByKey =
      v1.ActionDescription.Description.LookupByKey(toProtoDescriptionV0.value)

    override def pretty: Pretty[LookupByKeyActionDescription] = prettyOfClass(
      param("key", _.key),
      param("version", _.version),
    )
  }

  object LookupByKeyActionDescription {
    def tryCreate(
        key: LfGlobalKey,
        version: LfTransactionVersion,
        protocolVersion: RepresentativeProtocolVersion[ActionDescription],
    ): LookupByKeyActionDescription =
      new LookupByKeyActionDescription(key, version)(protocolVersion)

    def create(
        key: LfGlobalKey,
        version: LfTransactionVersion,
        protocolVersion: RepresentativeProtocolVersion[ActionDescription],
    ): Either[InvalidActionDescription, LookupByKeyActionDescription] =
      Either.catchOnly[InvalidActionDescription](tryCreate(key, version, protocolVersion))

  }
}
