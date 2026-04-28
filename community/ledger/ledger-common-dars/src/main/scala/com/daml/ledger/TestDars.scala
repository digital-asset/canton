// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import com.digitalasset.canton.util.JarResourceUtils
import com.digitalasset.daml.lf.archive.DarDecoder
import com.digitalasset.daml.lf.language.LanguageVersion
import com.google.protobuf.ByteString

import java.io.File
import java.util.zip.ZipInputStream

final case class TestDar(nameVersion: String, lfVersion: LanguageVersion)
    extends Product
    with Serializable {
  val path: String = s"$nameVersion-$lfVersionSuffix.dar"
  lazy val file: File = JarResourceUtils.resourceFile(path)

  lazy val bytes: ByteString =
    ByteString.readFrom(getClass.getClassLoader.getResourceAsStream(path))

  lazy val archive = {
    val inputStream = new ZipInputStream(getClass.getClassLoader.getResourceAsStream(path))
    DarDecoder
      .readArchive(nameVersion, inputStream)
      .toOption
      .getOrElse(sys.error(s"Cannot decode DAR: $path"))
  }

  override def toString = path

  private def lfVersionSuffix = {
    import lfVersion.{major, minor}
    minor match {
      case LanguageVersion.Minor.Stable(version) => s"v${major.pretty}$version"
      case LanguageVersion.Minor.Staging(version, _) => s"v${major.pretty}$version-staging"
      case LanguageVersion.Minor.Dev => s"v${major.pretty}dev"
    }
  }
}

final class TestDars private (val lfVersion: LanguageVersion) {
  val ExperimentalTestDar = testDar("experimental-tests-1.0.0")
  val KeysTestDar = testDar("keys-tests-1.0.0")
  val ModelTestDar = testDar("model-tests-1.0.0")
  val SemanticTestDar = testDar("semantic-tests-1.0.0")
  val OngoingStreamPackageUploadTestDar = testDar("ongoing-stream-package-upload-tests-1.0.0")
  val PackageManagementTestDar = testDar("package-management-tests-1.0.0")
  val Carbonv1TestDar = testDar("carbonv1-tests-1.0.0")
  val Carbonv2TestDar = testDar("carbonv2-tests-1.0.0")

  val UpgradeTestDar1_0_0 = testDar("upgrade-tests-1.0.0")
  val UpgradeTestDar2_0_0 = testDar("upgrade-tests-2.0.0")
  val UpgradeTestDar3_0_0 = testDar("upgrade-tests-3.0.0")
  val UpgradeFetchTestDar1_0_0 = testDar("upgrade-fetch-tests-1.0.0")
  val UpgradeFetchTestDar2_0_0 = testDar("upgrade-fetch-tests-2.0.0")
  val UpgradeIfaceDar = testDar("upgrade-iface-tests-1.0.0")

  val VettingMainDar_1_0_0 = testDar("vetting-main-1.0.0")
  val VettingMainDar_2_0_0 = testDar("vetting-main-2.0.0")
  val VettingMainDar_Split_Lineage_2_0_0 = testDar("vetting-main-split-lineage-2.0.0")
  val VettingMainDar_3_0_0_Incompatible = testDar("vetting-main-3.0.0")
  val VettingDepDar = testDar("vetting-dep-1.0.0")
  val VettingAltDar = testDar("vetting-alt-1.0.0")

  private val v22Plus = List(
    ModelTestDar,
    SemanticTestDar,
    OngoingStreamPackageUploadTestDar,
    PackageManagementTestDar,
    Carbonv1TestDar,
    Carbonv2TestDar,
  )

  private val v23Plus = Seq(KeysTestDar)

  private val v2DevPlus = Seq(ExperimentalTestDar)

  val darsToUpload: List[TestDar] = lfVersion match {
    case LanguageVersion.v2_2 => v22Plus
    case LanguageVersion.v2_3_2 => v22Plus ++ v23Plus
    case LanguageVersion.v2_dev => v22Plus ++ v23Plus ++ v2DevPlus
    case _ => sys.error(s"Unsupported LF version $lfVersion")
  }

  private def testDar(nameVersion: String): TestDar = TestDar(nameVersion, lfVersion)
}

object TestDars {
  val v2_2: TestDars = new TestDars(LanguageVersion.v2_2)
  val v2_3: TestDars = new TestDars(LanguageVersion.v2_3_2)
  val v2_dev: TestDars = new TestDars(LanguageVersion.v2_dev)
}
