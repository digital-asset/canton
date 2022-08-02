// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either._
import cats.syntax.traverse._
import com.daml.lf.CantonOnly
import com.daml.lf.value.{Value, ValueCoder}
import com.digitalasset.canton.ProtoDeserializationError.{
  FieldNotSet,
  OtherError,
  ValueDeserializationError,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.ContractIdSyntax._
import com.digitalasset.canton.protocol.LfHashSyntax._
import com.digitalasset.canton.protocol.{
  GlobalKeySerialization,
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
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.ShowUtil._
import com.digitalasset.canton.util.{LfTransactionUtil, NoCopy}
import com.digitalasset.canton.{LfChoiceName, LfPartyId, LfVersioned}
import com.google.protobuf.ByteString

/** Summarizes the information that is needed in addition to the other fields of [[ViewParticipantData]] for
  * determining the root action of a view.
  */
sealed trait ActionDescription extends Product with Serializable with PrettyPrinting {

  /** Whether the root action was a byKey action (exerciseByKey, fetchByKey, lookupByKey) */
  def byKey: Boolean

  /** The node seed for the root action of a view. Empty for fetch and lookupByKey nodes */
  def seedOption: Option[LfHash]

  /** The lf transaction version of the node */
  def version: LfTransactionVersion

  protected def toProtoDescription: v0.ActionDescription.Description

  def toProtoV0: v0.ActionDescription =
    v0.ActionDescription(description = toProtoDescription)
}

object ActionDescription {

  case class InvalidActionDescription(message: String)
      extends RuntimeException(message)
      with PrettyPrinting {
    override def pretty: Pretty[InvalidActionDescription] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  def tryFromLfActionNode(actionNode: LfActionNode, seedO: Option[LfHash]): ActionDescription =
    fromLfActionNode(actionNode, seedO).valueOr(err => throw err)

  /** Extracts the action description from an LF node and the optional seed.
    * @param seedO Must be set iff `node` is a [[com.digitalasset.canton.protocol.LfNodeCreate]] or [[com.digitalasset.canton.protocol.LfNodeExercises]].
    */
  def fromLfActionNode(
      actionNode: LfActionNode,
      seedO: Option[LfHash],
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
        } yield CreateActionDescription(contractId, seed, version)

      case LfNodeExercises(
            inputContract,
            _templateId,
            _interfaceId,
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
            chosenValue,
            actors,
            byKey,
            seed,
            version,
            failed = exerciseResult.isEmpty, // absence of exercise result indicates failure
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
        } yield FetchActionDescription(inputContract, actors, byKey, version)

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
          )
        } yield actionDescription
    }

  def fromProtoV0(
      actionDescriptionP: v0.ActionDescription
  ): ParsingResult[ActionDescription] = {
    import v0.ActionDescription.Description._
    val v0.ActionDescription(description) = actionDescriptionP
    description match {
      case Create(v0.ActionDescription.CreateActionDescription(contractIdP, seedP, versionP)) =>
        for {
          contractId <- LfContractId.fromProtoPrimitive(contractIdP)
          seed <- LfHash.fromProtoPrimitive("node_seed", seedP)
          version <- lfVersionfromProtoVersioned(versionP)
        } yield CreateActionDescription(contractId, seed, version)

      case Exercise(
            v0.ActionDescription
              .ExerciseActionDescription(
                inputContractIdP,
                choiceP,
                chosenValueB,
                actorsP,
                byKey,
                seedP,
                versionP,
                failed,
              )
          ) =>
        for {
          inputContractId <- LfContractId.fromProtoPrimitive(inputContractIdP)
          choice <- LfChoiceName
            .fromString(choiceP)
            .leftMap(err => ValueDeserializationError("choice", err))
          version <- lfVersionfromProtoVersioned(versionP)
          chosenValue <- ValueCoder
            .decodeValue(ValueCoder.CidDecoder, version, chosenValueB)
            .leftMap(err => ValueDeserializationError("chosen_value", err.errorMessage))
          actors <- actorsP.traverse(ProtoConverter.parseLfPartyId).map(_.toSet)
          seed <- LfHash.fromProtoPrimitive("node_seed", seedP)
          actionDescription <- ExerciseActionDescription
            .create(inputContractId, choice, chosenValue, actors, byKey, seed, version, failed)
            .leftMap(err => OtherError(err.message))
        } yield actionDescription

      case Fetch(
            v0.ActionDescription.FetchActionDescription(inputContractIdP, actorsP, byKey, versionP)
          ) =>
        for {
          inputContractId <- LfContractId.fromProtoPrimitive(inputContractIdP)
          actors <- actorsP.traverse(ProtoConverter.parseLfPartyId).map(_.toSet)
          version <- lfVersionfromProtoVersioned(versionP)
        } yield FetchActionDescription(inputContractId, actors, byKey, version)

      case LookupByKey(v0.ActionDescription.LookupByKeyActionDescription(keyP)) =>
        for {
          key <- ProtoConverter
            .required("key", keyP)
            .flatMap(GlobalKeySerialization.fromProtoV0(_))
          actionDescription <- LookupByKeyActionDescription
            .create(key.unversioned, key.version)
            .leftMap(err => OtherError(err.message))
        } yield actionDescription

      case Empty => Left(FieldNotSet("description"))
    }
  }

  private def lfVersionfromProtoVersioned(
      versionP: String
  ): Either[ValueDeserializationError, LfTransactionVersion] =
    CantonOnly.lookupTransactionVersion(versionP).leftMap(ValueDeserializationError("version", _))

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
  ) extends ActionDescription {
    override def byKey: Boolean = false

    override def seedOption: Option[LfHash] = Some(seed)

    protected override def toProtoDescription: v0.ActionDescription.Description =
      v0.ActionDescription.Description.Create(
        v0.ActionDescription.CreateActionDescription(
          contractId = contractId.toProtoPrimitive,
          nodeSeed = seed.toProtoPrimitive,
          version = version.protoValue,
        )
      )

    override def pretty: Pretty[CreateActionDescription] = prettyOfClass(
      param("contract Id", _.contractId),
      param("seed", _.seed),
      param("version", _.version),
    )
  }

  /** @throws InvalidActionDescription if the `chosen_value` cannot be serialized */
  case class ExerciseActionDescription private (
      inputContractId: LfContractId,
      choice: LfChoiceName,
      chosenValue: Value,
      actors: Set[LfPartyId],
      override val byKey: Boolean,
      seed: LfHash,
      override val version: LfTransactionVersion,
      failed: Boolean,
  ) extends ActionDescription
      with NoCopy {

    private val serializedChosenValue: ByteString = serializeChosenValue(chosenValue, version)
      .valueOr(err => throw InvalidActionDescription(s"Failed to serialize chosen value: ${err}"))

    override def seedOption: Option[LfHash] = Some(seed)

    protected override def toProtoDescription: v0.ActionDescription.Description =
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
    private[this] def apply(
        inputContractId: LfContractId,
        choice: LfChoiceName,
        chosenValue: Value,
        actors: Set[LfPartyId],
        byKey: Boolean,
        seed: LfHash,
        version: LfTransactionVersion,
        failed: Boolean,
    ): ExerciseActionDescription =
      throw new UnsupportedOperationException("Use the other factory methods instead")

    def tryCreate(
        inputContractId: LfContractId,
        choice: LfChoiceName,
        chosenValue: Value,
        actors: Set[LfPartyId],
        byKey: Boolean,
        seed: LfHash,
        version: LfTransactionVersion,
        failed: Boolean,
    ): ExerciseActionDescription =
      new ExerciseActionDescription(
        inputContractId,
        choice,
        chosenValue,
        actors,
        byKey,
        seed,
        version,
        failed,
      )

    def create(
        inputContractId: LfContractId,
        choice: LfChoiceName,
        chosenValue: Value,
        actors: Set[LfPartyId],
        byKey: Boolean,
        seed: LfHash,
        version: LfTransactionVersion,
        failed: Boolean,
    ): Either[InvalidActionDescription, ExerciseActionDescription] =
      Either.catchOnly[InvalidActionDescription](
        tryCreate(inputContractId, choice, chosenValue, actors, byKey, seed, version, failed)
      )
  }

  case class FetchActionDescription(
      inputContractId: LfContractId,
      actors: Set[LfPartyId],
      override val byKey: Boolean,
      override val version: LfTransactionVersion,
  ) extends ActionDescription
      with NoCopy {

    override def seedOption: Option[LfHash] = None

    protected override def toProtoDescription: v0.ActionDescription.Description =
      v0.ActionDescription.Description.Fetch(
        v0.ActionDescription.FetchActionDescription(
          inputContractId = inputContractId.toProtoPrimitive,
          actors = actors.toSeq,
          byKey = byKey,
          version = version.protoValue,
        )
      )

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
  ) extends ActionDescription
      with NoCopy {

    private val serializedKey =
      GlobalKeySerialization
        .toProto(LfVersioned(version, key))
        .valueOr(err => throw InvalidActionDescription(s"Failed to serialize key: $err"))

    override def byKey: Boolean = true

    override def seedOption: Option[LfHash] = None

    protected override def toProtoDescription: v0.ActionDescription.Description =
      v0.ActionDescription.Description.LookupByKey(
        v0.ActionDescription.LookupByKeyActionDescription(
          key = Some(serializedKey)
        )
      )

    override def pretty: Pretty[LookupByKeyActionDescription] = prettyOfClass(
      param("key", _.key),
      param("version", _.version),
    )
  }

  object LookupByKeyActionDescription {
    private[this] def apply(
        key: LfGlobalKey,
        version: LfTransactionVersion,
    ): LookupByKeyActionDescription =
      throw new UnsupportedOperationException("Use the other factory methods")

    def tryCreate(key: LfGlobalKey, version: LfTransactionVersion): LookupByKeyActionDescription =
      new LookupByKeyActionDescription(key, version)

    def create(
        key: LfGlobalKey,
        version: LfTransactionVersion,
    ): Either[InvalidActionDescription, LookupByKeyActionDescription] =
      Either.catchOnly[InvalidActionDescription](tryCreate(key, version))

  }
}
