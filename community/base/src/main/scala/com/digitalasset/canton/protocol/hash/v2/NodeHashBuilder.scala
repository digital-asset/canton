// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash.v2

import com.digitalasset.canton.crypto.HashPurpose
import com.digitalasset.canton.protocol.hash
import com.digitalasset.canton.protocol.hash.HashTracer
import com.digitalasset.canton.util.HexString
import com.digitalasset.canton.version.HashingSchemeVersion

/** Implementation of HashingSchemeVersion.V2 on Daml transaction nodes
  * @param enforceNodeSeedForCreateNodes
  *   when true, hashing will fail if a node seed is missing for a create node Set to false when
  *   hashing nodes coming from explicit disclosure for instance for which we don't have the seed.
  */
private[hash] class NodeHashBuilder(
    purpose: HashPurpose,
    hashTracer: HashTracer,
    enforceNodeSeedForCreateNodes: Boolean,
) extends hash.NodeHashBuilderCommon(
      purpose,
      hashTracer,
      enforceNodeSeedForCreateNodes,
      HashingSchemeVersion.V2,
    ) {
  // Version of the protobuf used to encode nodes defined in the interactive_submission_data.proto
  // Used in HashingSchemeVersion.V2 but removed in subsequent versions, because it does not add any
  // security value: once the transaction is deserialized by the EPN and submitted to the synchronizer,
  // which encoding version of the protobuf was used is irrelevant and cannot be verified anyway
  private val NodeEncodingV1 = 1

  override private[hash] def newBuilder(hashTracer: HashTracer): hash.NodeHashBuilder =
    new NodeHashBuilder(purpose, hashTracer, enforceNodeSeedForCreateNodes)
      .addByte(
        NodeEncodingV1.byteValue,
        byte => s"${HexString.toHexString(byte)} (Node Encoding Version)",
      )
}
