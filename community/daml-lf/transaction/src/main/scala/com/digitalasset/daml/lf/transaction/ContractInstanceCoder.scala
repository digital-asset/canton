// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.daml.scalautil.Statement.discard
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.{DecodeError, EncodeError, Value}
import com.google.protobuf.{ByteString, ProtocolStringList}

import scala.Ordering.Implicits.infixOrderingOps
import scala.annotation.nowarn
import scala.collection.immutable.TreeSet
import scala.jdk.CollectionConverters.*

object ContractInstanceCoder extends ContractInstanceCoder(allowNullCharacters = false)

class ContractInstanceCoder(allowNullCharacters: Boolean) {

  private[this] val ValueCoder =
    new value.ValueCoder(allowNullCharacters = allowNullCharacters).internal

  /** Decode a contract instance from wire format
    *
    * @param protoCoinst
    *   protocol buffer encoded contract instance
    * @return
    *   contract instance value
    */
  def decodeContractInstance(
      protoCoinst: TransactionOuterClass.ThinContractInstance
  ): Either[DecodeError, Versioned[Value.ThinContractInstance]] =
    ensuresNoUnknownFieldsThenDecode(protoCoinst)(internal.decodeContractInstance)

  /** Encodes a contract instance with the help of the contractId encoding function
    *
    * @param coinst
    *   the contract instance to be encoded
    * @return
    *   protobuf wire format contract instance
    */
  def encodeContractInstance(
      coinst: Versioned[Value.ThinContractInstance]
  ): Either[EncodeError, TransactionOuterClass.ThinContractInstance] =
    internal.encodeContractInstance(coinst)

  def decodeFatContractInstance(bytes: ByteString): Either[DecodeError, FatContractInstance] =
    internal.decodeFatContractInstance(bytes)

  def encodeFatContractInstance(
      contractInstance: FatContractInstance
  ): Either[EncodeError, ByteString] =
    internal.encodeFatContractInstance(contractInstance)

  private[transaction] def encodeVersioned(
      version: SerializationVersion,
      payload: ByteString,
  ): ByteString =
    internal.encodeVersioned(version, payload)

  private[transaction] def decodeVersioned(
      bytes: ByteString
  ): Either[DecodeError, Versioned[ByteString]] =
    internal.decodeVersioned(bytes)

  private[lf] def encodeFatContractInstanceInternal(
      contractInstance: FatContractInstance
  ): Either[EncodeError, TransactionOuterClass.FatContractInstance] =
    internal.encodeFatContractInstanceInternal(contractInstance)

  private[lf] def decodeFatContractInstance(
      txVersion: SerializationVersion,
      msg: TransactionOuterClass.FatContractInstance,
  ): Either[DecodeError, FatContractInstance] =
    internal.decodeFatContractInstance(txVersion, msg)

  private[transaction] object internal {

    private[ContractInstanceCoder] def encodeContractInstance(
        coinst: Versioned[Value.ThinContractInstance]
    ): Either[EncodeError, TransactionOuterClass.ThinContractInstance] =
      for {
        value <- ValueCoder.encodeVersionedValue(coinst.version, coinst.unversioned.arg)
      } yield {
        val builder = TransactionOuterClass.ThinContractInstance.newBuilder()
        discard(builder.setPackageName(coinst.unversioned.packageName))
        discard(builder.setTemplateId(ValueCoder.encodeIdentifier(coinst.unversioned.template)))
        discard(builder.setArgVersioned(value))
        builder.build()
      }

    def decodePackageName(s: String): Either[DecodeError, Ref.PackageName] =
      Either
        .cond(
          s.nonEmpty,
          s,
          DecodeError(s"packageName must not be empty"),
        )
        .flatMap(
          Ref.PackageName
            .fromString(_)
            .left
            .map(err => DecodeError(s"Invalid package name '$s': $err"))
        )

    private[ContractInstanceCoder] def decodeContractInstance(
        protoCoinst: TransactionOuterClass.ThinContractInstance
    ): Either[DecodeError, Versioned[Value.ThinContractInstance]] =
      for {
        id <- ValueCoder.decodeIdentifier(protoCoinst.getTemplateId)
        value <- ValueCoder.decodeVersionedValue(protoCoinst.getArgVersioned)
        pkgName <- decodePackageName(protoCoinst.getPackageName)
      } yield value.map(arg => Value.ThinContractInstance(pkgName, id, arg))

    private[transaction] def encodeKeyWithMaintainers(
        version: SerializationVersion,
        key: GlobalKeyWithMaintainers,
    ): Either[EncodeError, TransactionOuterClass.KeyWithMaintainers] =
      if (version >= SerializationVersion.minVersion) {
        val builder = TransactionOuterClass.KeyWithMaintainers.newBuilder()
        TreeSet.from(key.maintainers).foreach(builder.addMaintainers(_))
        ValueCoder.encodeValue(version, key.globalKey.key).map { bs =>
          builder.setKey(bs)
          // In canton >=3.5, encodeKeyWithMaintainers should always be called with vSerializationVersion.V2,
          // so the `else` branch here is dead production code. We however need it to preserve canton<3.5's behavior
          // in tests which test the interaction between the encoding of fat contract instances in canton<3.5 and
          // the decoding of fact contract instances in canton>=3.5.
          if (version >= SerializationVersion.V2)
            discard(builder.setHash(key.globalKey.hash.bytes.toByteString))
          builder.build
        }
      } else
        Left(EncodeError(s"Contract key are not supported by $version"))

    private[this] def decodeKeyWithMaintainers(
        version: SerializationVersion,
        templateId: Ref.TypeConId,
        packageName: Ref.PackageName,
        msg: TransactionOuterClass.KeyWithMaintainers,
    ): Either[DecodeError, GlobalKeyWithMaintainers] =
      for {
        maintainers <- toPartySet(msg.getMaintainersList)
        // Contracts written with SerializationVersion.V1 should never contain a key, because canton >=3.5 uses
        // V2 for contracts with keys, and canton <3.5 doesn't support keys.
        // However, as this "lenient parsing" has been released as part of Canton 3.5, it cannot be changed anymore within PV35.
        // Currently, if a participant sends a contract key at SerializationVersion V1,
        // the model conformance check fails and the view (as well as all ancestors) get rolled back.
        // If we failed the same situation already at this point, it would result in a parse error.
        // Due to EncryptedMultiViews that would result in discarding all members of the same MultiView.
        // So a model conformance failure and a parse error have different consequences and can therefore not be done interchangeably within PV35.
        keyValue <- ValueCoder.decodeValue(version, msg.getKey)
        hash <-
          if (version >= SerializationVersion.V2)
            crypto.Hash
              .fromBytes(data.Bytes.fromByteString(msg.getHash))
              .left
              .map(DecodeError(_))
          else
            Either
              .cond(
                msg.getHash.isEmpty,
                (),
                DecodeError(s"unexpected hash field in KeyWithMaintainers for version $version"),
              )
              .flatMap(_ =>
                // Ok to call unsafe hash function, as this will fail anyway during the model conformance check, as stated above.
                crypto.Hash
                  .hashContractKeyUnsafe(templateId, packageName, keyValue)
                  .left
                  .map(hashErr => DecodeError(hashErr.msg))
              )
        gkey <- GlobalKey
          .build(templateId, packageName, keyValue, hash)
          .left
          .map(hashErr => DecodeError(hashErr.msg))
      } yield GlobalKeyWithMaintainers(gkey, maintainers)

    private[transaction] def strictDecodeKeyWithMaintainers(
        version: SerializationVersion,
        templateId: Ref.TypeConId,
        packageName: Ref.PackageName,
        msg: TransactionOuterClass.KeyWithMaintainers,
    ): Either[DecodeError, GlobalKeyWithMaintainers] =
      for {
        kwm <- decodeKeyWithMaintainers(version, templateId, packageName, msg)
        _ <- Either.cond(kwm.maintainers.nonEmpty, (), DecodeError("key without maintainers"))
      } yield kwm

    def toPartySet(strList: ProtocolStringList): Either[DecodeError, Set[Ref.Party]] = {
      val parties = strList
        .asByteStringList()
        .asScala
        .map(bs => Ref.Party.fromString(bs.toStringUtf8))

      sequence(parties) match {
        case Left(err) => Left(DecodeError(s"Cannot decode party: $err"))
        case Right(ps) => Right(ps.toSet)
      }
    }

    // Similar to toPartySet but
    // - requires strList to be strictly ordered, fails otherwise
    // - produces a TreeSet instead of a Set
    // Note this function has a linear complexity, See data.TreeSet.fromStrictlyOrderedEntries
    def toPartyTreeSet(strList: ProtocolStringList): Either[DecodeError, TreeSet[Ref.Party]] =
      if (strList.isEmpty)
        Right(TreeSet.empty)
      else {
        val parties = strList
          .asByteStringList()
          .asScala
          .map(bs => Ref.Party.fromString(bs.toStringUtf8))

        sequence(parties) match {
          case Left(err) =>
            Left(DecodeError(s"Cannot decode party: $err"))
          case Right(ps) =>
            scala.util
              .Try(data.TreeSet.fromStrictlyOrderedEntries(ps))
              .toEither
              .left
              .map(e => DecodeError(e.getMessage))
        }
      }

    private[transaction] def encodeVersioned(
        version: SerializationVersion,
        payload: ByteString,
    ): ByteString = {
      val builder = TransactionOuterClass.Versioned.newBuilder()
      discard(builder.setVersion(SerializationVersion.toProtoValue(version)))
      discard(builder.setPayload(payload))
      builder.build().toByteString
    }

    private[ContractInstanceCoder] def parseVersioned(bytes: ByteString) =
      try {
        val msg = TransactionOuterClass.Versioned.parseFrom(bytes)
        ensuresNoUnknownFields(msg).map(_ => msg)
      } catch {
        case scala.util.control.NonFatal(e) =>
          Left(DecodeError(s"exception $e while decoding the versioned object"))
      }

    private[ContractInstanceCoder] def decodeVersioned(
        bytes: ByteString
    ): Either[DecodeError, Versioned[ByteString]] =
      for {
        proto <- parseVersioned(bytes)
        version <- SerializationVersion.fromString(proto.getVersion).left.map(DecodeError)
        payload = proto.getPayload
      } yield Versioned(version, payload)

    private[ContractInstanceCoder] def encodeFatContractInstanceInternal(
        contractInstance: FatContractInstance
    ): Either[EncodeError, TransactionOuterClass.FatContractInstance] = {
      import contractInstance.*
      for {
        encodedArg <- ValueCoder.encodeValue(version, createArg)
        encodedKeyOpt <- contractKeyWithMaintainers match {
          case None =>
            Right(None)
          case Some(key) =>
            encodeKeyWithMaintainers(version, key).map(Some(_))
        }
      } yield {
        val builder = TransactionOuterClass.FatContractInstance.newBuilder()
        discard(builder.setContractId(contractId.toBytes.toByteString))
        discard(builder.setPackageName(packageName))
        discard(builder.setTemplateId(ValueCoder.encodeIdentifier(templateId)))
        discard(builder.setCreateArg(encodedArg))
        encodedKeyOpt.foreach(builder.setContractKeyWithMaintainers)
        nonMaintainerSignatories.foreach(builder.addNonMaintainerSignatories)
        nonSignatoryStakeholders.foreach(builder.addNonSignatoryStakeholders)
        discard(builder.setCreatedAt(CreationTime.encode(createdAt)))
        discard(builder.setAuthenticationData(authenticationData.toByteString))
        builder.build()
      }
    }

    def encodeFatContractInstance(
        contractInstance: FatContractInstance
    ): Either[EncodeError, ByteString] =
      for {
        unversioned <- encodeFatContractInstanceInternal(contractInstance)
      } yield encodeVersioned(contractInstance.version, unversioned.toByteString)

    private[lf] def assertEncodeFatContractInstance(
        contractInstance: FatContractInstance
    ): data.Bytes =
      encodeFatContractInstance(contractInstance).fold(
        e => throw new IllegalArgumentException(e.errorMessage),
        data.Bytes.fromByteString,
      )

    private[ContractInstanceCoder] def decodeFatContractInstance(
        txVersion: SerializationVersion,
        msg: TransactionOuterClass.FatContractInstance,
    ): Either[DecodeError, FatContractInstance] =
      for {
        contractId <- ValueCoder.decodeCoid(msg.getContractId)
        pkgName <- decodePackageName(msg.getPackageName)
        templateId <- ValueCoder.decodeIdentifier(msg.getTemplateId)
        createArg <- ValueCoder.decodeValue(version = txVersion, bytes = msg.getCreateArg)
        keyWithMaintainers <-
          if (msg.hasContractKeyWithMaintainers)
            strictDecodeKeyWithMaintainers(
              txVersion,
              templateId,
              pkgName,
              msg.getContractKeyWithMaintainers,
            )
              .map(Some(_))
          else
            Right(None)
        maintainers = keyWithMaintainers.fold(TreeSet.empty[Ref.Party])(k =>
          TreeSet.from(k.maintainers)
        )
        nonMaintainerSignatories <- toPartyTreeSet(msg.getNonMaintainerSignatoriesList)
        _ <- Either.cond(
          maintainers.nonEmpty || nonMaintainerSignatories.nonEmpty,
          (),
          DecodeError("maintainers or non_maintainer_signatories should be non empty"),
        )
        nonSignatoryStakeholders <- toPartyTreeSet(msg.getNonSignatoryStakeholdersList)
        signatories <- maintainers.find(nonMaintainerSignatories) match {
          case Some(p) =>
            Left(DecodeError(s"party $p is declared as maintainer and nonMaintainerSignatory"))
          case None => Right(maintainers | nonMaintainerSignatories)
        }
        stakeholders <- nonSignatoryStakeholders.find(signatories) match {
          case Some(p) =>
            Left(DecodeError(s"party $p is declared as signatory and nonSignatoryStakeholder"))
          case None => Right(signatories | nonSignatoryStakeholders)
        }
        createdAt <- CreationTime.decode(msg.getCreatedAt).left.map(DecodeError)
        authenticationData = msg.getAuthenticationData
      } yield FatContractInstanceImpl(
        version = txVersion,
        contractId = contractId,
        packageName = pkgName,
        templateId = templateId,
        createArg = createArg,
        signatories = signatories,
        stakeholders = stakeholders,
        createdAt = createdAt,
        contractKeyWithMaintainers = keyWithMaintainers,
        authenticationData = data.Bytes.fromByteString(authenticationData),
      )

    private def parseFatContractInstance(bytes: ByteString) =
      try {
        val msg = TransactionOuterClass.FatContractInstance.parseFrom(bytes)
        ensuresNoUnknownFields(msg).map(_ => msg)
      } catch {
        case scala.util.control.NonFatal(e) =>
          Left(DecodeError(s"exception $e while decoding the object"))
      }

    @nowarn(
      "cat=unused-pat-vars"
    ) // suppress wrong warnings that version and unversioned are unused
    def decodeFatContractInstance(bytes: ByteString): Either[DecodeError, FatContractInstance] =
      for {
        versionedBlob <- decodeVersioned(bytes)
        Versioned(version, unversioned) = versionedBlob
        proto <- parseFatContractInstance(unversioned)
        fatContractInstance <- decodeFatContractInstance(version, proto)
      } yield fatContractInstance

  }
}
