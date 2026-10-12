// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto

import org.bouncycastle.jce.provider.BouncyCastleProvider

import java.security.{Provider, Security}

object Providers {
  val bouncyCastle: Provider = new BouncyCastleProvider
  Security.addProvider(bouncyCastle)

  val sunEC: Provider = Option(Security.getProvider("SunEC")).getOrElse {
    throw new IllegalStateException("SunEC provider not found")
  }

  val sunJCE: Provider = Option(Security.getProvider("SunJCE")).getOrElse {
    throw new IllegalStateException("SunJCE provider not found")
  }
}
