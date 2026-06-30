// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.user

import com.digitalasset.daml.lf.data.Ref

final case class User(
    id: Ref.UserId,
    primaryParty: Option[Ref.Party],
    isDeactivated: Boolean = false,
    metadata: ObjectMeta = ObjectMeta.empty,
    identityProviderId: IdentityProviderId = IdentityProviderId.Default,
    primaryPartyAuthentication: Boolean = false,
) {
  // Note: this should be replaced by pretty printing once the ledger-api server packages move
  //  into their proper place
  override def toString: String =
    s"User(id=$id, primaryParty=$primaryParty, isDeactivated=$isDeactivated, metadata=${metadata.toString
        .take(512)}, identityProviderId=${identityProviderId.toRequestString}, primaryPartyAuthentication=$primaryPartyAuthentication)"
}

final case class UserUpdate(
    id: Ref.UserId,
    identityProviderId: IdentityProviderId,
    primaryPartyUpdateO: Option[Option[Ref.Party]] = None,
    isDeactivatedUpdateO: Option[Boolean] = None,
    metadataUpdate: ObjectMetaUpdate,
    primaryPartyAuthenticationUpdateO: Option[Boolean] = None,
)
