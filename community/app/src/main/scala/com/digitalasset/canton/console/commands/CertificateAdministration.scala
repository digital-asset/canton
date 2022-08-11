// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.admin.api.client.commands.VaultAdminCommands
import com.digitalasset.canton.admin.api.client.data.console.CertificateResult
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  ConsoleEnvironment,
  FeatureFlag,
  FeatureFlagFilter,
  Help,
}
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.topology.UniqueIdentifier

trait CertificateAdministration extends FeatureFlagFilter {
  this: AdminCommandRunner =>

  protected val consoleEnvironment: ConsoleEnvironment

  @Help.Summary("Manage your certificates")
  @Help.Group("Certificates")
  object certs {

    @Help.Summary("Generate a self-signed certificate", FeatureFlag.Preview)
    def generate(
        uid: UniqueIdentifier,
        certificateKey: Fingerprint,
        additionalSubject: String = "",
        subjectAlternativeNames: Seq[String] = Seq.empty,
    ): CertificateResult = check(FeatureFlag.Preview) {
      consoleEnvironment.run {
        adminCommand(
          VaultAdminCommands.GenerateCertificate(
            uid,
            certificateKey,
            additionalSubject,
            subjectAlternativeNames,
          )
        )
      }
    }

    @Help.Summary("List locally stored certificates", FeatureFlag.Preview)
    def list(filterUid: String = ""): List[CertificateResult] = check(FeatureFlag.Preview) {
      consoleEnvironment.run {
        adminCommand(VaultAdminCommands.ListCertificates(filterUid))
      }
    }

    @Help.Summary("Import X509 certificate in PEM format", FeatureFlag.Preview)
    def load(x509Pem: String): String = check(FeatureFlag.Preview) {
      consoleEnvironment.run {
        adminCommand(VaultAdminCommands.ImportCertificate(x509Pem = x509Pem))
      }
    }

  }

}
