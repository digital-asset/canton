// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import com.google.protobuf.ByteString;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class GetUpdateByHashRequest {

  @NonNull private final ByteString transactionHash;

  @NonNull private final UpdateFormat updateFormat;

  public GetUpdateByHashRequest(
      @NonNull ByteString transactionHash, @NonNull UpdateFormat updateFormat) {
    this.transactionHash = transactionHash;
    this.updateFormat = updateFormat;
  }

  @NonNull
  public ByteString getTransactionHash() {
    return transactionHash;
  }

  @NonNull
  public UpdateFormat getUpdateFormat() {
    return updateFormat;
  }

  public static GetUpdateByHashRequest fromProto(
      UpdateServiceOuterClass.GetUpdateByHashRequest request) {
    return new GetUpdateByHashRequest(
        request.getTransactionHash(), UpdateFormat.fromProto(request.getUpdateFormat()));
  }

  public UpdateServiceOuterClass.GetUpdateByHashRequest toProto() {
    return UpdateServiceOuterClass.GetUpdateByHashRequest.newBuilder()
        .setTransactionHash(transactionHash)
        .setUpdateFormat(updateFormat.toProto())
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetUpdateByHashRequest that = (GetUpdateByHashRequest) o;
    return Objects.equals(transactionHash, that.transactionHash)
        && Objects.equals(updateFormat, that.updateFormat);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transactionHash, updateFormat);
  }

  @Override
  public String toString() {
    return "GetUpdateByHashRequest{"
        + "transactionHash="
        + transactionHash
        + ", updateFormat="
        + updateFormat
        + '}';
  }
}
