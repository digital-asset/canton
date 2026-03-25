// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.crypto.{EncryptionPublicKey, PublicKey, SigningPublicKey}

final case class KeyCollection(
    signingKeys: Seq[SigningPublicKey],
    encryptionKeys: Seq[EncryptionPublicKey],
) {
  def add(key: PublicKey): KeyCollection = (key: @unchecked) match {
    case sigKey: SigningPublicKey => copy(signingKeys = signingKeys :+ sigKey)
    case encKey: EncryptionPublicKey => copy(encryptionKeys = encryptionKeys :+ encKey)
  }

  def addAll(keys: Seq[PublicKey]): KeyCollection = keys.foldLeft(this)(_.add(_))

  def isEmpty: Boolean = signingKeys.isEmpty && encryptionKeys.isEmpty
}

object KeyCollection {

  val empty: KeyCollection = KeyCollection(Seq(), Seq())

}
