// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.MultiHashSignature.LeafSalt
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.{
  HasVersionedMessageCompanionE,
  HasVersionedWrapperE,
  ProtoVersion,
  ProtocolVersion,
  ProtocolVersionValidation,
}
import com.google.protobuf.ByteString

/** A signature over the root of a [[MerkleHashTree]] of salted leaves, together with the salt and
  * the inclusion path of one of its leaves. The signature, the salt and the path are serialized and
  * compared together, so that none of them can get lost. Without an inclusion path, it is the
  * signature of a tree over a single item, which covers that item's salted leaf. A signature that
  * is not made through such a tree is not a `MultiHashSignature`, but a plain [[Signature]].
  *
  * Every leaf of the tree is salted, also in a tree over a single item: the leaf of a signed item
  * is the hash of a salt and the item's hash (see [[MultiHashSignature.saltedLeaf]]), so that the
  * sibling hashes in a path reveal nothing about the other items of the tree.
  *
  * @param signature
  *   the signature over the root of the tree
  * @param inclusionPath
  *   the inclusion path of the item's salted leaf. It is empty in a tree over a single item, whose
  *   salted leaf is the root itself.
  * @param leafSalt
  *   the salt of the item's leaf
  *
  * Multi-hash signatures are only available from the protocol version
  * [[com.digitalasset.canton.version.ProtocolVersion.multiHashSignatures]].
  */
final case class MultiHashSignature private (
    signature: Signature,
    inclusionPath: InclusionPath,
    leafSalt: LeafSalt,
) extends HasVersionedWrapperE[MultiHashSignature]
    with PrettyPrintingFromCompanion {

  override protected def companionObj: MultiHashSignature.type = MultiHashSignature

  /** The root of the tree for the given hash of a signed item, computed from the item's salted leaf
    * and the inclusion path. The signature covers the item only if verifying [[signature]] over
    * this root succeeds.
    */
  def rootHash(hash: Hash): Hash =
    MerkleHashTree.rootFromPath(MultiHashSignature.saltedLeaf(leafSalt, hash), inclusionPath)

  def toProtoV30: v30.MultiHashSignature =
    v30.MultiHashSignature(
      signature = Some(signature.toProtoV30),
      inclusionPath = Some(inclusionPath.toProtoV30),
      leafSalt = leafSalt.unwrap,
    )

  override def prettyCompanion: PrettyPrintingCompanion[MultiHashSignature] = MultiHashSignature
}

object MultiHashSignature
    extends HasVersionedMessageCompanionE[MultiHashSignature]
    with PrettyPrintingCompanion[MultiHashSignature] {

  override def name: String = "multi-hash signature"

  // Multi-hash signatures do not exist before `ProtocolVersion.multiHashSignatures`: serializing for
  // an earlier protocol version fails with an error, and so does deserializing. The unsupported
  // codec covers the minimum protocol version, as required by the framework.
  // TODO(#34479): HasVersionedWrapper deserializers do not get a pvv.
  @SuppressWarnings(Array("com.digitalasset.canton.NoProtocolVersionValidation"))
  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(-1) -> ProtoCodec(
      ProtocolVersion.minimum,
      unsupportedProtoCodecDeserializer(ProtocolVersion.minimum),
      _ =>
        Left(
          s"A $name is not supported before protocol version ${ProtocolVersion.multiHashSignatures}"
        ),
    ),
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.multiHashSignatures,
      supportedProtoVersion(v30.MultiHashSignature)(
        fromProtoV30(ProtocolVersionValidation.NoValidation, _)
      ),
      multiHashSignature => Right(multiHashSignature.toProtoV30),
    ),
  )

  override protected val pretty: Pretty[MultiHashSignature] = prettyOfClass(
    param("signature", _.signature),
    param("inclusionPath", _.inclusionPath),
    param("leafSalt", _.leafSalt),
  )

  /** The salt of a leaf: random bytes of a fixed length, which are hashed together with the hash of
    * the signed item. No information about how the salt was generated is shipped, as receivers only
    * hash it.
    */
  final case class LeafSalt private (unwrap: ByteString) extends PrettyPrintingFromCompanion {
    override def prettyCompanion: PrettyPrintingCompanion[LeafSalt] = LeafSalt
  }

  object LeafSalt extends PrettyPrintingCompanion[LeafSalt] {

    /** The length of a leaf salt in bytes */
    val Length: Int = 32

    override protected val pretty: Pretty[LeafSalt] = prettyOfParam(_.unwrap)

    /** Creates the leaf salt, or fails if it does not have [[Length]] bytes */
    def create(bytes: ByteString): Either[String, LeafSalt] =
      Either.cond(
        bytes.size == Length,
        new LeafSalt(bytes),
        s"A leaf salt has $Length bytes, got ${bytes.size}",
      )

    /** Creates the leaf salt. Throws if [[create]] fails. */
    def tryCreate(bytes: ByteString): LeafSalt =
      create(bytes).valueOr(err => throw new IllegalArgumentException(err))
  }

  /** The leaf of a signed item in the tree: a hash of the leaf's salt and the raw SHA-256 digest of
    * the item's hash, as in the inner nodes of the tree. The dedicated hash purpose keeps the
    * salted leaves apart from the inner nodes of the tree and from any other hash.
    */
  def saltedLeaf(salt: LeafSalt, hash: Hash): Hash =
    Hash
      .build(HashPurpose.MerkleHashTreeSaltedLeaf, HashAlgorithm.Sha256)
      // The salt and the item's hash have a fixed length, so no length prefix is needed. The item's
      // hash is a SHA-256 hash, so the multi-hash prefix is not needed either.
      .addWithoutLengthPrefix(salt.unwrap)
      .addWithoutLengthPrefix(hash.unwrap)
      .finish()

  /** A signature over the root of a tree, with the inclusion path and the salt of the item's leaf
    */
  def create(
      signature: Signature,
      inclusionPath: InclusionPath,
      leafSalt: LeafSalt,
  ): MultiHashSignature =
    new MultiHashSignature(signature, inclusionPath, leafSalt)

  /** Parses a multi-hash signature. The expected protocol version is passed on to the parsing of
    * the signature. The bound on the number of siblings of the inclusion path applies for every
    * protocol version, as inclusion paths come from untrusted sources.
    */
  def fromProtoV30(
      expectedProtocolVersion: ProtocolVersionValidation,
      multiHashSignatureP: v30.MultiHashSignature,
  ): ParsingResult[MultiHashSignature] =
    for {
      signature <- ProtoConverter.parseRequired(
        Signature.fromProtoV30(expectedProtocolVersion, _),
        "signature",
        multiHashSignatureP.signature,
      )
      inclusionPath <- ProtoConverter.parseRequired(
        InclusionPath.fromProtoV30,
        "inclusion_path",
        multiHashSignatureP.inclusionPath,
      )
      leafSalt <- LeafSalt
        .create(multiHashSignatureP.leafSalt)
        .leftMap(ProtoDeserializationError.ValueConversionError("leaf_salt", _))
    } yield new MultiHashSignature(signature, inclusionPath, leafSalt)
}
