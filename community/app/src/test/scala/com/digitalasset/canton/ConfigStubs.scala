// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import cats.syntax.option._
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.config.{
  CommunityAdminServerConfig,
  CommunityCryptoConfig,
  CommunityStorageConfig,
  InitConfig,
}
import com.digitalasset.canton.domain.config.CommunityDomainConfig
import com.digitalasset.canton.participant.config.CommunityParticipantConfig

/** Utilities for creating config objects for tests
  */
@SuppressWarnings(Array("org.wartremover.warts.Null"))
object ConfigStubs {
  def participant: CommunityParticipantConfig =
    CommunityParticipantConfig(
      InitConfig(),
      CommunityCryptoConfig(),
      null,
      adminApi,
      CommunityStorageConfig.Memory(),
    )

  def domain: CommunityDomainConfig =
    CommunityDomainConfig(
      InitConfig(),
      false,
      null,
      null,
      CommunityStorageConfig.Memory(),
      CommunityCryptoConfig(),
    )

  def adminApi: CommunityAdminServerConfig =
    CommunityAdminServerConfig(internalPort = Port.tryCreate(42).some)
}
