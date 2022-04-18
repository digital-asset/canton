// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either._
import com.daml.lf.value.ValueCoder
import com.digitalasset.canton.ProtoDeserializationError.ValueConversionError
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.ContractIdSyntax._
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.{
  HasProtoV0,
  HasVersionedMessageCompanion,
  HasVersionedWrapper,
  ProtocolVersion,
  VersionedMessage,
}

/** Represents a serializable contract.
  *
  * @param contractId The ID of the contract.
  * @param rawContractInstance The raw instance of the contract.
  * @param metadata The metadata with stakeholders and signatories; can be computed from contract instance
  * @param ledgerCreateTime The ledger time of the transaction '''creating''' the contract
  */
// This class is a reference example of serialization best practices, demonstrating:
// - use of an UntypedVersionedMessage wrapper when serializing to an anonymous binary format. For a more extensive example of this,
// please also see the writeup under `Backwards-incompatible Protobuf changes` in `CONTRIBUTING.md`.

// Please consult the team if you intend to change the design of serialization.
case class SerializableContract(
    contractId: LfContractId,
    rawContractInstance: SerializableRawContractInstance,
    metadata: ContractMetadata,
    ledgerCreateTime: CantonTimestamp,
)
// The class implements `HasVersionedWrapper` because we serialize it to an anonymous binary format (ByteString/Array[Byte]) when
// writing to the TransferStore and thus need to encode the version of the serialized Protobuf message
    extends HasVersionedWrapper[VersionedMessage[SerializableContract]]
    // Even if implementing HasVersionedWrapper, we should still implement HasProtoV0
    with HasProtoV0[v0.SerializableContract]
    with PrettyPrinting {
  def contractInstance: LfContractInst = rawContractInstance.contractInstance

  override def toByteArray(version: ProtocolVersion): Array[Byte] = super.toByteArray(version)

  /*
  A `toProtoVersioned` method for a class which only has a single version of the corresponding Protobuf message
  typically ignores the version-argument.
  If there are multiple versions of the corresponding Protobuf message (e.g. v0.DomainTopologyTransaction and
  v1.DomainTopologyTransaction), it needs to pattern-match on the versions to decide which version it should embed
  within the UntypedVersionedMessage wrapper. Note: `VersionedMessage[SerializableContract]` is an alias
  for UntypedVersionedMessage.
   */

  override def toProtoVersioned(version: ProtocolVersion): VersionedMessage[SerializableContract] =
    VersionedMessage(toProtoV0.toByteString, 0)

  override def toProtoV0: v0.SerializableContract =
    v0.SerializableContract(
      contractId = contractId.toProtoPrimitive,
      rawContractInstance = rawContractInstance.getCryptographicEvidence,
      // Even though [[ContractMetadata]] also implements `HasVersionedWrapper`, we explicitly use Protobuf V0
      // -> we only use `UntypedVersionedMessage` when required and not for 'regularly' nested Protobuf messages
      metadata = Some(metadata.toProtoV0),
      ledgerCreateTime = Some(ledgerCreateTime.toProtoPrimitive),
    )

  override def pretty: Pretty[SerializableContract] = prettyOfClass(
    param("contractId", _.contractId),
    param(
      "instance",
      (serContract: SerializableContract) => serContract.rawContractInstance.contractInstance,
    )(adHocPrettyInstance), // TODO(#3269) This may leak confidential data
    param("metadata", _.metadata),
    param("create time", _.ledgerCreateTime),
  )

}

/** Serializable contract with witnesses for contract add/import used in admin repairs.
  *
  * @param contract serializable contract
  * @param witnesses optional witnesses that observe the creation of the contract
  */
case class SerializableContractWithWitnesses(
    contract: SerializableContract,
    witnesses: Set[PartyId],
)

object SerializableContract extends HasVersionedMessageCompanion[SerializableContract] {
  val supportedProtoVersions: Map[Int, Parser] = Map(
    0 -> supportedProtoVersion(v0.SerializableContract)(fromProtoV0)
  )

  override protected def name: String = "serializable contract"

  def apply(
      contractId: LfContractId,
      contractInstance: LfContractInst,
      metadata: ContractMetadata,
      ledgerTime: CantonTimestamp,
  ): Either[ValueCoder.EncodeError, SerializableContract] =
    SerializableRawContractInstance
      .create(contractInstance)
      .map(SerializableContract(contractId, _, metadata, ledgerTime))

  def fromProtoV0(
      serializableContractInstanceP: v0.SerializableContract
  ): ParsingResult[SerializableContract] = {
    val v0.SerializableContract(contractIdP, rawP, metadataP, ledgerCreateTime) =
      serializableContractInstanceP

    for {
      contractId <- LfContractId.fromProtoPrimitive(contractIdP)
      raw <- SerializableRawContractInstance
        .fromByteString(rawP)
        .leftMap(error => ValueConversionError("raw_contract_instance", error.toString))
      metadata <- ProtoConverter
        .required("metadata", metadataP)
        .flatMap(ContractMetadata.fromProtoV0)
      ledgerTime <- ProtoConverter
        .required("ledger_create_time", ledgerCreateTime)
        .flatMap(CantonTimestamp.fromProtoPrimitive)
    } yield SerializableContract(contractId, raw, metadata, ledgerTime)
  }
}
