// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.localstore

import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.platform.localstore.api.PartyRecordStore
import org.scalatest.freespec.AsyncFreeSpec

import scala.concurrent.Future

class InMemoryPartyRecordStoreSpec extends AsyncFreeSpec with PartyRecordStoreTests {

  override def newStore(): PartyRecordStore = new InMemoryPartyRecordStore(
    executionContext = executionContext
  )

  override def createIdentityProviderConfig(
      identityProviderConfig: domain.IdentityProviderConfig
  ): Future[Unit] = Future.unit
}