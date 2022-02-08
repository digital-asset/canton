// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.authentication.grpc

import com.digitalasset.canton.sequencing.authentication.AuthenticationToken
import com.google.protobuf.ByteString
import io.grpc.Metadata
import io.grpc.Metadata.{ASCII_STRING_MARSHALLER, BinaryMarshaller}

object Constant {
  val AUTH_TOKEN_MARSHALLER = new BinaryMarshaller[AuthenticationToken] {
    override def toBytes(value: AuthenticationToken): Array[Byte] =
      value.getCryptographicEvidence.toByteArray
    override def parseBytes(serialized: Array[Byte]): AuthenticationToken =
      AuthenticationToken.tryFromProtoPrimitive(ByteString.copyFrom(serialized))
  }
  val AUTH_TOKEN_METADATA_KEY: Metadata.Key[AuthenticationToken] =
    Metadata.Key.of("authToken-bin", AUTH_TOKEN_MARSHALLER)
  val MEMBER_ID_METADATA_KEY: Metadata.Key[String] =
    Metadata.Key.of("memberId", ASCII_STRING_MARSHALLER)
  val DOMAIN_ID_METADATA_KEY: Metadata.Key[String] =
    Metadata.Key.of("domainId", ASCII_STRING_MARSHALLER)
  val AUTHENTICATION_ERROR_CODE: Metadata.Key[String] =
    Metadata.Key.of("authErrorCode", ASCII_STRING_MARSHALLER)
}
