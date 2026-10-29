// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data

import com.daml.ledger.api.v2.admin.UserManagementServiceOuterClass.Right
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UserRightSpec extends AnyFlatSpec with Matchers {

  private val protos: Seq[Right] = Seq(
    Right.newBuilder().setCanActAs(Right.CanActAs.newBuilder().setParty("alice")).build(),
    Right.newBuilder().setCanReadAs(Right.CanReadAs.newBuilder().setParty("alice")).build(),
    Right.newBuilder().setCanExecuteAs(Right.CanExecuteAs.newBuilder().setParty("alice")).build(),
    Right.newBuilder().setParticipantAdmin(Right.ParticipantAdmin.getDefaultInstance).build(),
    Right
      .newBuilder()
      .setIdentityProviderAdmin(Right.IdentityProviderAdmin.getDefaultInstance)
      .build(),
    Right.newBuilder().setCanReadAsAnyParty(Right.CanReadAsAnyParty.getDefaultInstance).build(),
    Right
      .newBuilder()
      .setCanExecuteAsAnyParty(Right.CanExecuteAsAnyParty.getDefaultInstance)
      .build(),
    Right.newBuilder().setCanActAsAnyParty(Right.CanActAsAnyParty.getDefaultInstance).build(),
    Right
      .newBuilder()
      .setCanDebitTrafficFrom(Right.CanDebitTrafficFrom.newBuilder().setAccountId("acme/billing"))
      .build(),
    Right
      .newBuilder()
      .setCanDebitTrafficFromAnyAccount(Right.CanDebitTrafficFromAnyAccount.getDefaultInstance)
      .build(),
  )

  "User.Right" should "round-trip every right kind through its proto" in {
    // A new proto right kind fails here until we add it to this list and to fromProto.
    protos
      .map(_.getKindCase)
      .toSet shouldBe Right.KindCase.values().toSet - Right.KindCase.KIND_NOT_SET
    protos.foreach(proto => User.Right.fromProto(proto).toProto shouldBe proto)
  }
}
