// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.version.*
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

/** Information concerning every '''member''' involved in processing the underlying view.
  *
  * @param threshold If the sum of the weights of the parties approving the view attains the threshold,
  *                  the view is considered approved.
  */
// This class is a reference example of serialization best practices, demonstrating:
// - memoized serialization, which is required if we need to compute a signature or cryptographic hash of a class
// - use of an UntypedVersionedMessage wrapper when serializing to an anonymous binary format
// Please consult the team if you intend to change the design of serialization.
//
// The constructor and `fromProto...` methods are private to ensure that clients cannot create instances with an incorrect `deserializedFrom` field.
//
// Optional parameters are strongly discouraged, as each parameter needs to be consciously set in a production context.
case class ViewCommonData private (informees: Set[Informee], threshold: NonNegativeInt, salt: Salt)(
    hashOps: HashOps,
    val representativeProtocolVersion: RepresentativeProtocolVersion[ViewCommonData],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[ViewCommonData](hashOps)
    // The class needs to implement ProtocolVersionedMemoizedEvidence, because we want that serialize always yields the same ByteString.
    // This is to ensure that different participants compute the same hash after receiving a ViewCommonData over the network.
    // (Recall that serialization is in general not guaranteed to be deterministic.)
    with ProtocolVersionedMemoizedEvidence
    // The class implements `HasProtocolVersionedWrapper` because we serialize it to an anonymous binary format and need to encode
    // the version of the serialized Protobuf message
    with HasProtocolVersionedWrapper[ViewCommonData] {

  // The toProto... methods are deliberately protected, as they could otherwise be abused to bypass memoization.
  //
  // If another serializable class contains a ViewCommonData, it has to include it as a ByteString
  // (and not as "message ViewCommonData") in its ProtoBuf representation.

  override def companionObj: ViewCommonData.type = ViewCommonData

  // We use named parameters, because then the code remains correct even when the ProtoBuf code generator
  // changes the order of parameters.
  protected def toProtoV0: v0.ViewCommonData =
    v0.ViewCommonData(
      informees = informees.map(_.toProtoV0).toSeq,
      threshold = threshold.unwrap,
      salt = Some(salt.toProtoV0),
    )

  // When serializing the class to an anonymous binary format, we serialize it to an UntypedVersionedMessage version of the
  // corresponding Protobuf message
  override protected[this] def toByteStringUnmemoized: ByteString = toByteString

  override val hashPurpose: HashPurpose = HashPurpose.ViewCommonData

  override def pretty: Pretty[ViewCommonData] = prettyOfClass(
    param("informees", _.informees),
    param("threshold", _.threshold),
    param("salt", _.salt),
  )

  @VisibleForTesting
  def copy(
      informees: Set[Informee] = this.informees,
      threshold: NonNegativeInt = this.threshold,
      salt: Salt = this.salt,
  ): ViewCommonData =
    ViewCommonData(informees, threshold, salt)(hashOps, representativeProtocolVersion, None)
}

object ViewCommonData
    extends HasMemoizedProtocolVersionedWithContextCompanion[ViewCommonData, HashOps] {
  override val name: String = "ViewCommonData"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v3,
      supportedProtoVersionMemoized(v0.ViewCommonData)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  /** Creates a fresh [[ViewCommonData]]. */
  // The "create" method has the following advantages over the auto-generated "apply" method:
  // - The parameter lists have been flipped to facilitate curried usages.
  // - The deserializedFrom field cannot be set; so it cannot be set incorrectly.
  //
  // The method is called "create" instead of "apply"
  // to not confuse the Idea compiler by overloading "apply".
  // (This is not a problem with this particular class, but it has been a problem with other classes.)
  def create(hashOps: HashOps)(
      informees: Set[Informee],
      threshold: NonNegativeInt,
      salt: Salt,
      protocolVersion: ProtocolVersion,
  ): ViewCommonData =
    // The deserializedFrom field is set to "None" as this is for creating "fresh" instances.
    new ViewCommonData(informees, threshold, salt)(
      hashOps,
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    )

  private def fromProtoV0(
      hashOps: HashOps,
      viewCommonDataP: v0.ViewCommonData,
  )(bytes: ByteString): ParsingResult[ViewCommonData] =
    for {
      informees <- viewCommonDataP.informees.traverse(Informee.fromProtoV0)

      salt <- ProtoConverter
        .parseRequired(Salt.fromProtoV0, "salt", viewCommonDataP.salt)
        .leftMap(_.inField("salt"))

      threshold <- NonNegativeInt.create(viewCommonDataP.threshold).leftMap(_.inField("threshold"))
    } yield new ViewCommonData(informees.toSet, threshold, salt)(
      hashOps,
      protocolVersionRepresentativeFor(ProtoVersion(0)),
      Some(bytes),
    )
}
