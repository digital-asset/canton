// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash.v2

import com.digitalasset.canton.crypto.{HashPurpose, InteractiveSubmission}
import com.digitalasset.canton.protocol.hash.{HashTracer, v2}
import com.digitalasset.canton.protocol.{LfHash, hash}
import com.digitalasset.canton.util.HexString
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.transaction.CreationTime

private[hash] class TransactionMetadataHasher extends hash.TransactionMetadataHasher {

  // Version of the protobuf used to encode the metadata defined in the interactive_submission_data.proto
  // Used in HashingSchemeVersion.V2 but removed in subsequent versions, because it does not add any
  // security value: once the transaction is deserialized by the EPN and submitted to the synchronizer,
  // which encoding version of the protobuf was used is irrelevant and cannot be verified anyway
  private val MetadataEncodingV1 = 1

  protected def newBuilder(hashTracer: HashTracer): hash.NodeHashBuilder = new v2.NodeHashBuilder(
    HashPurpose.PreparedSubmission,
    hashTracer,
    enforceNodeSeedForCreateNodes = false,
  )
    .addPurpose()
    .addByte(
      MetadataEncodingV1.byteValue,
      byte => s"${HexString.toHexString(byte)} (Metadata Encoding Version)",
    )

  override protected[hash] def tryHashMetadataBuilder(
      metadata: InteractiveSubmission.TransactionMetadataForHashing,
      hashTracer: HashTracer,
  ): hash.NodeHashBuilder =
    newBuilder(hashTracer)
      .withContext("Act As Parties")(
        _.addIterator(metadata.actAs.iterator, metadata.actAs.size)(_ addString _)
      )
      .withContext("Command Id")(_.addString(metadata.commandId))
      .withContext("Transaction UUID")(_.addString(metadata.transactionUUID.toString))
      .withContext("Mediator Group")(_.addInt(metadata.mediatorGroup))
      .withContext("Synchronizer Id")(_.addString(metadata.synchronizer.toProtoPrimitive))
      .withContext("Min Time Boundary")(
        _.addOptional(
          metadata.timeBoundaries.minConstraint,
          b => (v: Time.Timestamp) => b.addLong(v.micros),
        )
      )
      .withContext("Max Time Boundary")(
        _.addOptional(
          metadata.timeBoundaries.maxConstraint,
          b => (v: Time.Timestamp) => b.addLong(v.micros),
        )
      )
      .withContext("Preparation Time")(_.addLong(metadata.preparationTime.micros))
      .withContext("Disclosed Contracts")(
        _.addIterator(metadata.disclosedContracts.valuesIterator, metadata.disclosedContracts.size)(
          (builder, fatInstance) =>
            builder
              .withContext("Created At")(_.addLong(CreationTime.encode(fatInstance.createdAt)))
              .withContext("Create Contract")(builder =>
                builder.addHash(
                  builder.hashNode(
                    node = fatInstance.toCreateNode,
                    nodeSeed = Option.empty[LfHash],
                    nodes = Map.empty,
                    nodeSeeds = Map.empty,
                    hashTracer = hashTracer.subNodeTracer,
                  ),
                  "Disclosed Contract",
                )
              )
        )
      )
}
