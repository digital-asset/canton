// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import better.files.File
import cats.syntax.either.*
import com.digitalasset.canton.ReassignmentCounter
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.protocol.messages.HasDomainId
import com.digitalasset.canton.protocol.{HasSerializableContract, SerializableContract}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.util.{ByteStringUtil, GrpcStreamingUtils, ResourceUtil}
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

import java.io.{ByteArrayInputStream, InputStream}

final case class ActiveContract(
    domainId: DomainId,
    contract: SerializableContract,
    reassignmentCounter: ReassignmentCounter,
)(override val representativeProtocolVersion: RepresentativeProtocolVersion[ActiveContract.type])
    extends HasProtocolVersionedWrapper[ActiveContract]
    with HasDomainId
    with HasSerializableContract {
  private def toProtoV30: v30.ActiveContract =
    v30.ActiveContract(
      domainId.toProtoPrimitive,
      Some(contract.toAdminProtoV30),
      reassignmentCounter.toProtoPrimitive,
    )

  override protected lazy val companionObj: ActiveContract.type = ActiveContract

  private[canton] def withSerializableContract(
      contract: SerializableContract
  ): ActiveContract =
    copy(contract = contract)(representativeProtocolVersion)

}

private[canton] object ActiveContract extends HasProtocolVersionedCompanion[ActiveContract] {

  override def name: String = "ActiveContract"

  override def supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v33)(v30.ActiveContract)(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  private def fromProtoV30(
      proto: v30.ActiveContract
  ): ParsingResult[ActiveContract] =
    for {
      domainId <- DomainId.fromProtoPrimitive(proto.domainId, "domain_id")
      contract <- ProtoConverter.parseRequired(
        SerializableContract.fromAdminProtoV30,
        "contract",
        proto.contract,
      )
      reassignmentCounter = proto.reassignmentCounter
      reprProtocolVersion <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield {
      ActiveContract(domainId, contract, ReassignmentCounter(reassignmentCounter))(
        reprProtocolVersion
      )
    }

  def create(
      domainId: DomainId,
      contract: SerializableContract,
      reassignmentCounter: ReassignmentCounter,
  )(protocolVersion: ProtocolVersion): ActiveContract =
    ActiveContract(domainId, contract, reassignmentCounter)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  private[canton] def fromFile(fileInput: File): Iterator[ActiveContract] =
    ResourceUtil.withResource(fileInput.newGzipInputStream(8192)) { fileInput =>
      loadFromSource(fileInput) match {
        case Left(error) => throw new Exception(error)
        case Right(value) => value.iterator
      }
    }

  private[admin] def loadFromByteString(
      bytes: ByteString
  ): Either[String, List[ActiveContract]] =
    for {
      decompressedBytes <-
        ByteStringUtil
          .decompressGzip(bytes, None)
          .leftMap(err => s"Failed to decompress bytes: $err")
      contracts <- ResourceUtil.withResource(
        new ByteArrayInputStream(decompressedBytes.toByteArray)
      ) { inputSource =>
        loadFromSource(inputSource)
      }
    } yield contracts

  private def loadFromSource(
      source: InputStream
  ): Either[String, List[ActiveContract]] =
    GrpcStreamingUtils
      .parseDelimitedFromTrusted[ActiveContract](
        source,
        ActiveContract,
      )
      .map(_.toList)
}
