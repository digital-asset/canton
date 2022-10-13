// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import cats.syntax.either.*
import com.digitalasset.canton.config.Password
import com.digitalasset.canton.crypto.{X509Certificate, X509CertificatePem}
import com.digitalasset.canton.util.ResourceUtil

import java.security.{KeyStore as JKeyStore, KeyStoreException}

// NOTE: Trust store is represented by a [[java.security.KeyStore]] class too.
case class TrustStore(private val store: JKeyStore) {
  def unwrap: JKeyStore = store
}

object TrustStore {

  // Create and initialize a trust store from the given server certificates
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def create(certificate: X509CertificatePem): Either[String, TrustStore] =
    for {
      store <- Either
        .catchOnly[KeyStoreException](JKeyStore.getInstance("PKCS12"))
        .leftMap(err => s"Failed to create trust store instance: $err")

      _ <- Either
        .catchNonFatal(store.load(null, "".toCharArray))
        .leftMap(err => s"Failed to initialize trust store: $err")

      cert <- X509Certificate.fromPem(certificate).leftMap(_.toString)

      _ <- Either
        .catchNonFatal(store.setCertificateEntry(cert.id.unwrap, cert.unwrap))
        .leftMap(err => s"Failed to store certificate in trust store: $err")
    } yield TrustStore(store)
}

/** A password protected keystore */
case class ProtectedKeyStore(store: JKeyStore, password: Password) {
  def saveToFile(filename: String): Either[String, Unit] =
    ResourceUtil
      .withResourceEither(new java.io.FileOutputStream(filename)) { fos =>
        store.store(fos, password.toCharArray)
      }
      .leftMap(_.toString)
}
