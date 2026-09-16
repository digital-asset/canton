// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation

import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.validation.buildinfo.BuildInfo
import org.scalatest.Inspectors.forEvery
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{Assertion, Inside}

import java.io.File

final class UpgradesCheckSpec extends AsyncWordSpec with Matchers with Inside {
  protected def loadPackageId(path: String): PackageId = {
    val dar = DarReader.assertReadArchiveFromFile(toTestResourceDarFile(path))
    assert(dar != null, s"Unable to load test package resource '$path'")
    dar.main.pkgId
  }

  private def toTestResourceDarFile(rawPath: String): File =
    new File(BuildInfo.parallelDamlBuildDarsOutput + "/" + rawPath)

  def testPackages(
      rawPaths: Seq[String],
      uploadAssertions: Seq[(String, String, Option[String])],
  ): Assertion = {
    val builder = new StringBuilder()
    val loggerFactory = StringLoggerFactory("")
    val upgradeCheck = UpgradeCheckMain(loggerFactory)
    upgradeCheck.check(rawPaths.map(toTestResourceDarFile(_)).toArray)
    for { msg <- loggerFactory.msgs } {
      (builder append msg) append '\n'
    }
    val out = builder.toString

    forEvery(uploadAssertions) { uploadAssertion =>
      checkTwo(uploadAssertion)(out)
    }
  }

  def checkTwo(assertion: (String, String, Option[String]))(
      upgradeCheckToolLogs: String
  ): Assertion = {
    val (firstIdx: String, secondIdx: String, failureMessage: Option[String]) = assertion
    val testPackageFirstId: PackageId = loadPackageId(firstIdx)
    val testPackageSecondId: PackageId = loadPackageId(secondIdx)
    val header =
      s"Error while checking two DARs:\nUpgrade checks indicate that ($testPackageFirstId|$testPackageSecondId) \\(.*\\) cannot be an upgrade of ($testPackageFirstId|$testPackageSecondId) \\(.*\\)"
    failureMessage match {
      case None => upgradeCheckToolLogs should not include regex(header)
      case Some(msg) => upgradeCheckToolLogs should include regex (s"$header. Reason: $msg")
    }
  }

  s"Upgradeability Checks using `daml upgrade-check` tool" should {
    "report no upgrade errors when the upgrade use a newer version of LF" in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenNewerPackagesUsesANewerLFVersion-v1.dar",
          "upgrades-SucceedsWhenNewerPackagesUsesANewerLFVersion-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenNewerPackagesUsesANewerLFVersion-v1.dar",
            "upgrades-SucceedsWhenNewerPackagesUsesANewerLFVersion-v2.dar",
            None,
          )
        ),
      )
    }

    "report upgrade errors when the upgrade use a older version of LF" in {
      testPackages(
        rawPaths = Seq(
          "upgrades-FailsWhenNewerPackagesUsesAnOlderLFVersion-v1.dar",
          "upgrades-FailsWhenNewerPackagesUsesAnOlderLFVersion-v2.dar",
        ),
        uploadAssertions = Seq(
          (
            "upgrades-FailsWhenNewerPackagesUsesAnOlderLFVersion-v1.dar",
            "upgrades-FailsWhenNewerPackagesUsesAnOlderLFVersion-v2.dar",
            Some("The upgraded package uses an older LF version"),
          )
        ),
      )
    }

    "Succeeds when v2 depends on v2dep which is a valid upgrade of v1dep" in {
      testPackages(
        Seq(
          "upgrades-UploadSucceedsWhenDepsAreValidUpgrades-v1.dar",
          "upgrades-UploadSucceedsWhenDepsAreValidUpgrades-v2.dar",
        ),
        Seq(
          (
            "upgrades-UploadSucceedsWhenDepsAreValidUpgradesDep-v1.dar",
            "upgrades-UploadSucceedsWhenDepsAreValidUpgradesDep-v2.dar",
            None,
          )
        ),
      )
    }

    "report upgrade errors when v2 depends on v2dep which is an invalid upgrade of v1dep" in {
      testPackages(
        Seq(
          "upgrades-UploadFailsWhenDepsAreInvalidUpgrades-v1.dar",
          "upgrades-UploadFailsWhenDepsAreInvalidUpgrades-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenExistingFieldInTemplateIsChanged-v1.dar",
            "upgrades-FailsWhenExistingFieldInTemplateIsChanged-v2.dar",
            Some("The upgraded template A has changed the types of some of its original fields."),
          )
        ),
      )
    }

    "Fails when a package embeds a previous version of itself it is not a valid upgrade of" in {
      testPackages(
        Seq(
          "upgrades-FailsWhenDepIsInvalidPreviousVersionOfSelf-v2.dar"
        ),
        Seq(
          (
            "upgrades-FailsWhenDepIsInvalidPreviousVersionOfSelf-v1.dar",
            "upgrades-FailsWhenDepIsInvalidPreviousVersionOfSelf-v2.dar",
            Some(
              "The upgraded data type T has added new fields, but those fields are not Optional."
            ),
          )
        ),
      )
    }

    "Fail when only one version of a package depends on both v1 and v2 of a dep which are themselves incompatible" in {
      testPackages(
        Seq("upgrades-FailsWhenOnePackageHasTwoIncompatibleDeps-v1.dar"),
        Seq(
          (
            "upgrades-FailsWhenOnePackageHasTwoIncompatibleDeps-dep-v1.dar",
            "upgrades-FailsWhenOnePackageHasTwoIncompatibleDeps-dep-v2.dar",
            Some(
              "The upgraded data type Dep has added new fields, but those fields are not Optional"
            ),
          )
        ),
      )
    }

    "Succeeds when a package embeds a previous version of itself it is a valid upgrade of" in {
      testPackages(
        Seq("upgrades-SucceedsWhenDepIsValidPreviousVersionOfSelf-v2.dar"),
        Seq(
          (
            "upgrades-SucceedsWhenDepIsValidPreviousVersionOfSelf-v1.dar",
            "upgrades-SucceedsWhenDepIsValidPreviousVersionOfSelf-v2.dar",
            None,
          )
        ),
      )
    }

    "Succeeds when upgrading a dependency" in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenUpgradingADependency-v1.dar",
          "upgrades-SucceedsWhenUpgradingADependency-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenUpgradingADependency-dep-v1.dar",
            "upgrades-SucceedsWhenUpgradingADependency-dep-v2.dar",
            None,
          ),
          (
            "upgrades-SucceedsWhenUpgradingADependency-v1.dar",
            "upgrades-SucceedsWhenUpgradingADependency-v2.dar",
            None,
          ),
        ),
      )
    }

    "Succeeds when upgrading a dependency of a dependency" in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenUpgradingADependencyOfAnUpgradedDependency-v1.dar",
          "upgrades-SucceedsWhenUpgradingADependencyOfAnUpgradedDependency-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenUpgradingADependencyOfAnUpgradedDependency-dep-dep-v1.dar",
            "upgrades-SucceedsWhenUpgradingADependencyOfAnUpgradedDependency-dep-dep-v2.dar",
            None,
          ),
          (
            "upgrades-SucceedsWhenUpgradingADependencyOfAnUpgradedDependency-dep-v1.dar",
            "upgrades-SucceedsWhenUpgradingADependencyOfAnUpgradedDependency-dep-v2.dar",
            None,
          ),
          (
            "upgrades-SucceedsWhenUpgradingADependencyOfAnUpgradedDependency-v1.dar",
            "upgrades-SucceedsWhenUpgradingADependencyOfAnUpgradedDependency-v2.dar",
            None,
          ),
        ),
      )
    }

    "Fails when upgrading an erroneous dependency of a dependency" in {
      testPackages(
        Seq(
          "upgrades-FailsWhenUpgradingAnUnupgradeableDependencyOfAnUpgradedDependency-v1.dar",
          "upgrades-FailsWhenUpgradingAnUnupgradeableDependencyOfAnUpgradedDependency-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenUpgradingAnUnupgradeableDependencyOfAnUpgradedDependency-dep-dep-v1.dar",
            "upgrades-FailsWhenUpgradingAnUnupgradeableDependencyOfAnUpgradedDependency-dep-dep-v2.dar",
            Some(
              "The upgraded data type D has added new fields, but those fields are not Optional."
            ),
          )
        ),
      )
    }

    s"Succeeds when v1 upgrades to v2 and then v3" in {
      testPackages(
        Seq(
          "upgrades-SuccessUpgradingV2ThenV3-v1.dar",
          "upgrades-SuccessUpgradingV2ThenV3-v2.dar",
          "upgrades-SuccessUpgradingV2ThenV3-v3.dar",
        ),
        Seq(
          (
            "upgrades-SuccessUpgradingV2ThenV3-v1.dar",
            "upgrades-SuccessUpgradingV2ThenV3-v2.dar",
            None,
          ),
          (
            "upgrades-SuccessUpgradingV2ThenV3-v2.dar",
            "upgrades-SuccessUpgradingV2ThenV3-v3.dar",
            None,
          ),
        ),
      )
    }

    s"Succeeds when v1 upgrades to v3 and then v2" in {
      testPackages(
        Seq(
          "upgrades-SuccessUpgradingV3ThenV2-v1.dar",
          "upgrades-SuccessUpgradingV3ThenV2-v3.dar",
          "upgrades-SuccessUpgradingV3ThenV2-v2.dar",
        ),
        Seq(
          (
            "upgrades-SuccessUpgradingV3ThenV2-v1.dar",
            "upgrades-SuccessUpgradingV3ThenV2-v2.dar",
            None,
          ),
          (
            "upgrades-SuccessUpgradingV3ThenV2-v1.dar",
            "upgrades-SuccessUpgradingV3ThenV2-v3.dar",
            None,
          ),
        ),
      )
    }

    s"Fails when v1 upgrades to v2, but v3 does not upgrade v2" in {
      testPackages(
        Seq(
          "upgrades-FailsWhenUpgradingV2ThenV3-v1.dar",
          "upgrades-FailsWhenUpgradingV2ThenV3-v2.dar",
          "upgrades-FailsWhenUpgradingV2ThenV3-v3.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenUpgradingV2ThenV3-v1.dar",
            "upgrades-FailsWhenUpgradingV2ThenV3-v2.dar",
            None,
          ),
          (
            "upgrades-FailsWhenUpgradingV2ThenV3-v2.dar",
            "upgrades-FailsWhenUpgradingV2ThenV3-v3.dar",
            Some("The upgraded template T is missing some of its original fields."),
          ),
        ),
      )
    }

    s"Fails when v1 upgrades to v3, but v3 does not upgrade v2" in {
      testPackages(
        Seq(
          "upgrades-FailsWhenUpgradingV3ThenV2-v1.dar",
          "upgrades-FailsWhenUpgradingV3ThenV2-v3.dar",
          "upgrades-FailsWhenUpgradingV3ThenV2-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenUpgradingV3ThenV2-v1.dar",
            "upgrades-FailsWhenUpgradingV3ThenV2-v2.dar",
            None,
          ),
          (
            "upgrades-FailsWhenUpgradingV3ThenV2-v3.dar",
            "upgrades-FailsWhenUpgradingV3ThenV2-v2.dar",
            Some("The upgraded template T is missing some of its original fields."),
          ),
        ),
      )
    }

    "Fails when an instance is dropped." in {
      testPackages(
        Seq(
          "upgrades-FailsWhenAnInstanceIsDropped-dep.dar",
          "upgrades-FailsWhenAnInstanceIsDropped-v1.dar",
          "upgrades-FailsWhenAnInstanceIsDropped-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenAnInstanceIsDropped-v1.dar",
            "upgrades-FailsWhenAnInstanceIsDropped-v2.dar",
            Some(
              "Implementation of interface .*:Dep:I by template T appears in package that is being upgraded, but does not appear in this package."
            ),
          )
        ),
      )
    }

    "Succeeds when an instance is added (separate dep)." in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenAnInstanceIsAddedSeparateDep-dep.dar",
          "upgrades-SucceedsWhenAnInstanceIsAddedSeparateDep-v1.dar",
          "upgrades-SucceedsWhenAnInstanceIsAddedSeparateDep-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenAnInstanceIsAddedSeparateDep-v1.dar",
            "upgrades-SucceedsWhenAnInstanceIsAddedSeparateDep-v2.dar",
            None,
          )
        ),
      )
    }

    s"report no upgrade errors for valid upgrade" in {
      testPackages(
        Seq(
          "upgrades-ValidUpgrade-v1.dar",
          "upgrades-ValidUpgrade-v2.dar",
        ),
        Seq(
          (
            "upgrades-ValidUpgrade-v1.dar",
            "upgrades-ValidUpgrade-v2.dar",
            None,
          )
        ),
      )
    }
    s"report no upgrade errors for valid upgrades of parameterized data types" in {
      testPackages(
        Seq(
          "upgrades-ValidParameterizedTypesUpgrade-v1.dar",
          "upgrades-ValidParameterizedTypesUpgrade-v2.dar",
        ),
        Seq(
          (
            "upgrades-ValidParameterizedTypesUpgrade-v1.dar",
            "upgrades-ValidParameterizedTypesUpgrade-v2.dar",
            None,
          )
        ),
      )
    }
    s"report no upgrade errors for alpha-equivalent complex key types" in {
      testPackages(
        Seq(
          "upgrades-ValidKeyTypeEquality-v1.dar",
          "upgrades-ValidKeyTypeEquality-v2.dar",
        ),
        Seq(
          (
            "upgrades-ValidKeyTypeEquality-v1.dar",
            "upgrades-ValidKeyTypeEquality-v2.dar",
            None,
          )
        ),
      )
    }
    s"report error when module is missing in upgrading package" in {
      testPackages(
        Seq(
          "upgrades-MissingModule-v1.dar",
          "upgrades-MissingModule-v2.dar",
        ),
        Seq(
          (
            "upgrades-MissingModule-v1.dar",
            "upgrades-MissingModule-v2.dar",
            Some(
              "Module Other appears in package that is being upgraded, but does not appear in the upgrading package."
            ),
          )
        ),
      )
    }
    s"report error when template is missing in upgrading package" in {
      testPackages(
        Seq(
          "upgrades-MissingTemplate-v1.dar",
          "upgrades-MissingTemplate-v2.dar",
        ),
        Seq(
          (
            "upgrades-MissingTemplate-v1.dar",
            "upgrades-MissingTemplate-v2.dar",
            Some(
              "Template U appears in package that is being upgraded, but does not appear in the upgrading package."
            ),
          )
        ),
      )
    }
    s"allow uploading a package with a missing template but for a different package-name" in {
      testPackages(
        Seq(
          "upgrades-MissingTemplate-v1.dar",
          "upgrades-MissingTemplateDifferentPackageName.dar",
        ),
        Seq(
          (
            "upgrades-MissingTemplate-v1.dar",
            "upgrades-MissingTemplateDifferentPackageName.dar",
            None,
          )
        ),
      )
    }
    s"report error when datatype is missing in upgrading package" in {
      testPackages(
        Seq(
          "upgrades-MissingDataCon-v1.dar",
          "upgrades-MissingDataCon-v2.dar",
        ),
        Seq(
          (
            "upgrades-MissingDataCon-v1.dar",
            "upgrades-MissingDataCon-v2.dar",
            Some(
              "Data type U appears in package that is being upgraded, but does not appear in the upgrading package."
            ),
          )
        ),
      )
    }
    s"report error when choice is missing in upgrading package" in {
      testPackages(
        Seq(
          "upgrades-MissingChoice-v1.dar",
          "upgrades-MissingChoice-v2.dar",
        ),
        Seq(
          (
            "upgrades-MissingChoice-v1.dar",
            "upgrades-MissingChoice-v2.dar",
            Some(
              "Choice C2 appears in package that is being upgraded, but does not appear in the upgrading package."
            ),
          )
        ),
      )
    }
    s"succeed when adding a choice to a template in upgrading package" in {
      testPackages(
        Seq(
          "upgrades-TemplateAddedChoice-v1.dar",
          "upgrades-TemplateAddedChoice-v2.dar",
        ),
        Seq(
          (
            "upgrades-TemplateAddedChoice-v1.dar",
            "upgrades-TemplateAddedChoice-v2.dar",
            None,
          )
        ),
      )
    }
    s"report error when key type changes" in {
      testPackages(
        Seq(
          "upgrades-TemplateChangedKeyType-v1.dar",
          "upgrades-TemplateChangedKeyType-v2.dar",
        ),
        Seq(
          (
            "upgrades-TemplateChangedKeyType-v1.dar",
            "upgrades-TemplateChangedKeyType-v2.dar",
            Some("The upgraded template T cannot change its key type."),
          )
        ),
      )
    }
    s"report error when record fields change" in {
      testPackages(
        Seq(
          "upgrades-RecordFieldsNewNonOptional-v1.dar",
          "upgrades-RecordFieldsNewNonOptional-v2.dar",
        ),
        Seq(
          (
            "upgrades-RecordFieldsNewNonOptional-v1.dar",
            "upgrades-RecordFieldsNewNonOptional-v2.dar",
            Some(
              "The upgraded data type Struct has added new fields, but those fields are not Optional."
            ),
          )
        ),
      )
    }

    // Ported from DamlcUpgrades.hs
    s"Fails when template changes key type" in {
      testPackages(
        Seq(
          "upgrades-FailsWhenTemplateChangesKeyType-v1.dar",
          "upgrades-FailsWhenTemplateChangesKeyType-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenTemplateChangesKeyType-v1.dar",
            "upgrades-FailsWhenTemplateChangesKeyType-v2.dar",
            Some("The upgraded template A cannot change its key type."),
          )
        ),
      )
    }
    s"Succeeds when template upgrades its key type" in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenTemplateUpgradesKeyType-v1.dar",
          "upgrades-SucceedsWhenTemplateUpgradesKeyType-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenTemplateUpgradesKeyType-v1.dar",
            "upgrades-SucceedsWhenTemplateUpgradesKeyType-v2.dar",
            None,
          )
        ),
      )
    }
    s"Fails when template removes key type" in {
      testPackages(
        Seq(
          "upgrades-FailsWhenTemplateRemovesKeyType-v1.dar",
          "upgrades-FailsWhenTemplateRemovesKeyType-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenTemplateRemovesKeyType-v1.dar",
            "upgrades-FailsWhenTemplateRemovesKeyType-v2.dar",
            Some("The upgraded template A cannot remove its key."),
          )
        ),
      )
    }
    s"Fails when template adds key type" in {
      testPackages(
        Seq(
          "upgrades-FailsWhenTemplateAddsKeyType-v1.dar",
          "upgrades-FailsWhenTemplateAddsKeyType-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenTemplateAddsKeyType-v1.dar",
            "upgrades-FailsWhenTemplateAddsKeyType-v2.dar",
            Some("The upgraded template A cannot add a key."),
          )
        ),
      )
    }
    s"Fails when new field is added to template without Optional type" in {
      testPackages(
        Seq(
          "upgrades-FailsWhenNewFieldIsAddedToTemplateWithoutOptionalType-v1.dar",
          "upgrades-FailsWhenNewFieldIsAddedToTemplateWithoutOptionalType-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenNewFieldIsAddedToTemplateWithoutOptionalType-v1.dar",
            "upgrades-FailsWhenNewFieldIsAddedToTemplateWithoutOptionalType-v2.dar",
            Some("The upgraded template A has added new fields, but those fields are not Optional."),
          )
        ),
      )
    }
    s"Fails when old field is deleted from template" in {
      testPackages(
        Seq(
          "upgrades-FailsWhenOldFieldIsDeletedFromTemplate-v1.dar",
          "upgrades-FailsWhenOldFieldIsDeletedFromTemplate-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenOldFieldIsDeletedFromTemplate-v1.dar",
            "upgrades-FailsWhenOldFieldIsDeletedFromTemplate-v2.dar",
            Some("The upgraded template A is missing some of its original fields."),
          )
        ),
      )
    }
    s"Fails when existing field in template is changed" in {
      testPackages(
        Seq(
          "upgrades-FailsWhenExistingFieldInTemplateIsChanged-v1.dar",
          "upgrades-FailsWhenExistingFieldInTemplateIsChanged-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenExistingFieldInTemplateIsChanged-v1.dar",
            "upgrades-FailsWhenExistingFieldInTemplateIsChanged-v2.dar",
            Some("The upgraded template A has changed the types of some of its original fields."),
          )
        ),
      )
    }
    s"Succeeds when new field with optional type is added to template" in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate-v1.dar",
          "upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate-v1.dar",
            "upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate-v2.dar",
            None,
          )
        ),
      )
    }
    s"Fails when new field is added to template choice without Optional type" in {
      testPackages(
        Seq(
          "upgrades-FailsWhenNewFieldIsAddedToTemplateChoiceWithoutOptionalType-v1.dar",
          "upgrades-FailsWhenNewFieldIsAddedToTemplateChoiceWithoutOptionalType-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenNewFieldIsAddedToTemplateChoiceWithoutOptionalType-v1.dar",
            "upgrades-FailsWhenNewFieldIsAddedToTemplateChoiceWithoutOptionalType-v2.dar",
            Some(
              "The upgraded input type of choice C on template A has added new fields, but those fields are not Optional."
            ),
          )
        ),
      )
    }
    s"Fails when old field is deleted from template choice" in {
      testPackages(
        Seq(
          "upgrades-FailsWhenOldFieldIsDeletedFromTemplateChoice-v1.dar",
          "upgrades-FailsWhenOldFieldIsDeletedFromTemplateChoice-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenOldFieldIsDeletedFromTemplateChoice-v1.dar",
            "upgrades-FailsWhenOldFieldIsDeletedFromTemplateChoice-v2.dar",
            Some(
              "The upgraded input type of choice C on template A is missing some of its original fields."
            ),
          )
        ),
      )
    }
    s"Fails when existing field in template choice is changed" in {
      testPackages(
        Seq(
          "upgrades-FailsWhenExistingFieldInTemplateChoiceIsChanged-v1.dar",
          "upgrades-FailsWhenExistingFieldInTemplateChoiceIsChanged-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenExistingFieldInTemplateChoiceIsChanged-v1.dar",
            "upgrades-FailsWhenExistingFieldInTemplateChoiceIsChanged-v2.dar",
            Some(
              "The upgraded input type of choice C on template A has changed the types of some of its original fields."
            ),
          )
        ),
      )
    }
    s"Fails when template choice changes its return type" in {
      testPackages(
        Seq(
          "upgrades-FailsWhenTemplateChoiceChangesItsReturnType-v1.dar",
          "upgrades-FailsWhenTemplateChoiceChangesItsReturnType-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenTemplateChoiceChangesItsReturnType-v1.dar",
            "upgrades-FailsWhenTemplateChoiceChangesItsReturnType-v2.dar",
            Some("The upgraded choice C cannot change its return type."),
          )
        ),
      )
    }
    s"Succeeds when template choice returns a template which has changed" in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenTemplateChoiceReturnsATemplateWhichHasChanged-v1.dar",
          "upgrades-SucceedsWhenTemplateChoiceReturnsATemplateWhichHasChanged-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenTemplateChoiceReturnsATemplateWhichHasChanged-v1.dar",
            "upgrades-SucceedsWhenTemplateChoiceReturnsATemplateWhichHasChanged-v2.dar",
            None,
          )
        ),
      )
    }
    s"Succeeds when template choice input argument template has changed" in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenTemplateChoiceInputArgumentTemplateHasChanged-v1.dar",
          "upgrades-SucceedsWhenTemplateChoiceInputArgumentTemplateHasChanged-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenTemplateChoiceInputArgumentTemplateHasChanged-v1.dar",
            "upgrades-SucceedsWhenTemplateChoiceInputArgumentTemplateHasChanged-v2.dar",
            None,
          )
        ),
      )
    }
    s"Succeeds when template choice input argument enum has changed" in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenTemplateChoiceInputArgumentEnumHasChanged-v1.dar",
          "upgrades-SucceedsWhenTemplateChoiceInputArgumentEnumHasChanged-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenTemplateChoiceInputArgumentEnumHasChanged-v1.dar",
            "upgrades-SucceedsWhenTemplateChoiceInputArgumentEnumHasChanged-v2.dar",
            None,
          )
        ),
      )
    }
    s"Succeeds when template choice input argument struct has changed" in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenTemplateChoiceInputArgumentStructHasChanged-v1.dar",
          "upgrades-SucceedsWhenTemplateChoiceInputArgumentStructHasChanged-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenTemplateChoiceInputArgumentStructHasChanged-v1.dar",
            "upgrades-SucceedsWhenTemplateChoiceInputArgumentStructHasChanged-v2.dar",
            None,
          )
        ),
      )
    }
    s"Succeeds when template choice input argument variant has changed" in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenTemplateChoiceInputArgumentVariantHasChanged-v1.dar",
          "upgrades-SucceedsWhenTemplateChoiceInputArgumentVariantHasChanged-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenTemplateChoiceInputArgumentVariantHasChanged-v1.dar",
            "upgrades-SucceedsWhenTemplateChoiceInputArgumentVariantHasChanged-v2.dar",
            None,
          )
        ),
      )
    }
    s"Succeeds when new field with optional type is added to template choice" in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplateChoice-v1.dar",
          "upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplateChoice-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplateChoice-v1.dar",
            "upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplateChoice-v2.dar",
            None,
          )
        ),
      )
    }

    "Fails when a top-level record adds a non-optional field" in {
      testPackages(
        Seq(
          "upgrades-FailsWhenATopLevelRecordAddsANonOptionalField-v1.dar",
          "upgrades-FailsWhenATopLevelRecordAddsANonOptionalField-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenATopLevelRecordAddsANonOptionalField-v1.dar",
            "upgrades-FailsWhenATopLevelRecordAddsANonOptionalField-v2.dar",
            Some(
              "The upgraded data type A has added new fields, but those fields are not Optional."
            ),
          )
        ),
      )
    }

    "Succeeds when a top-level record adds an optional field at the end" in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenATopLevelRecordAddsAnOptionalFieldAtTheEnd-v1.dar",
          "upgrades-SucceedsWhenATopLevelRecordAddsAnOptionalFieldAtTheEnd-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenATopLevelRecordAddsAnOptionalFieldAtTheEnd-v1.dar",
            "upgrades-SucceedsWhenATopLevelRecordAddsAnOptionalFieldAtTheEnd-v2.dar",
            None,
          )
        ),
      )
    }

    "Fails when a top-level record adds an optional field before the end" in {
      testPackages(
        Seq(
          "upgrades-FailsWhenATopLevelRecordAddsAnOptionalFieldBeforeTheEnd-v1.dar",
          "upgrades-FailsWhenATopLevelRecordAddsAnOptionalFieldBeforeTheEnd-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenATopLevelRecordAddsAnOptionalFieldBeforeTheEnd-v1.dar",
            "upgrades-FailsWhenATopLevelRecordAddsAnOptionalFieldBeforeTheEnd-v2.dar",
            Some(
              "The upgraded data type A has changed the order of its fields - any new fields must be added at the end of the record."
            ),
          )
        ),
      )
    }

    "Succeeds when a top-level variant adds a variant" in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenATopLevelVariantAddsAConstructor-v1.dar",
          "upgrades-SucceedsWhenATopLevelVariantAddsAConstructor-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenATopLevelVariantAddsAConstructor-v1.dar",
            "upgrades-SucceedsWhenATopLevelVariantAddsAConstructor-v2.dar",
            None,
          )
        ),
      )
    }

    "Fails when a top-level variant removes a variant" in {
      testPackages(
        Seq(
          "upgrades-FailsWhenATopLevelVariantRemovesAConstructor-v1.dar",
          "upgrades-FailsWhenATopLevelVariantRemovesAConstructor-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenATopLevelVariantRemovesAConstructor-v1.dar",
            "upgrades-FailsWhenATopLevelVariantRemovesAConstructor-v2.dar",
            Some(
              "Data type A.Z appears in package that is being upgraded, but does not appear in the upgrading package."
            ),
          )
        ),
      )
    }

    "Fail when a top-level variant changes changes the order of its variants" in {
      testPackages(
        Seq(
          "upgrades-FailWhenATopLevelVariantChangesChangesTheOrderOfItsConstructors-v1.dar",
          "upgrades-FailWhenATopLevelVariantChangesChangesTheOrderOfItsConstructors-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailWhenATopLevelVariantChangesChangesTheOrderOfItsConstructors-v1.dar",
            "upgrades-FailWhenATopLevelVariantChangesChangesTheOrderOfItsConstructors-v2.dar",
            Some(
              "The upgraded data type A has changed the order of its variants - any new variant must be added at the end of the variant."
            ),
          )
        ),
      )
    }

    "Fails when a top-level variant adds a field to a variant's type" in {
      testPackages(
        Seq(
          "upgrades-FailsWhenATopLevelVariantAddsAFieldToAConstructorsType-v1.dar",
          "upgrades-FailsWhenATopLevelVariantAddsAFieldToAConstructorsType-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenATopLevelVariantAddsAFieldToAConstructorsType-v1.dar",
            "upgrades-FailsWhenATopLevelVariantAddsAFieldToAConstructorsType-v2.dar",
            Some("The upgraded variant constructor A.Y from variant A has added a field."),
          )
        ),
      )
    }

    "Succeeds when a top-level variant adds an optional field to a variant's type" in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenATopLevelVariantAddsAnOptionalFieldToAConstructorsType-v1.dar",
          "upgrades-SucceedsWhenATopLevelVariantAddsAnOptionalFieldToAConstructorsType-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenATopLevelVariantAddsAnOptionalFieldToAConstructorsType-v1.dar",
            "upgrades-SucceedsWhenATopLevelVariantAddsAnOptionalFieldToAConstructorsType-v2.dar",
            None,
          )
        ),
      )
    }

    "Fails when a top-level enum drops a constructor" in {
      testPackages(
        Seq(
          "upgrades-FailsWhenAnEnumDropsAConstructor-v1.dar",
          "upgrades-FailsWhenAnEnumDropsAConstructor-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenAnEnumDropsAConstructor-v1.dar",
            "upgrades-FailsWhenAnEnumDropsAConstructor-v2.dar",
            Some("The upgraded data type MyEnum has removed an existing variant."),
          )
        ),
      )
    }

    "Succeeds when a top-level enum changes" in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenATopLevelEnumChanges-v1.dar",
          "upgrades-SucceedsWhenATopLevelEnumChanges-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenATopLevelEnumChanges-v1.dar",
            "upgrades-SucceedsWhenATopLevelEnumChanges-v2.dar",
            None,
          )
        ),
      )
    }

    "Fail when a top-level enum changes changes the order of its variants" in {
      testPackages(
        Seq(
          "upgrades-FailWhenATopLevelEnumChangesChangesTheOrderOfItsConstructors-v1.dar",
          "upgrades-FailWhenATopLevelEnumChangesChangesTheOrderOfItsConstructors-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailWhenATopLevelEnumChangesChangesTheOrderOfItsConstructors-v1.dar",
            "upgrades-FailWhenATopLevelEnumChangesChangesTheOrderOfItsConstructors-v2.dar",
            Some(
              "The upgraded data type A has changed the order of its variants - any new variant must be added at the end of the enum."
            ),
          )
        ),
      )
    }

    "Succeeds when a top-level type synonym changes" in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenATopLevelTypeSynonymChanges-v1.dar",
          "upgrades-SucceedsWhenATopLevelTypeSynonymChanges-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenATopLevelTypeSynonymChanges-v1.dar",
            "upgrades-SucceedsWhenATopLevelTypeSynonymChanges-v2.dar",
            None,
          )
        ),
      )
    }

    "Succeeds when two deeply nested type synonyms resolve to the same datatypes" in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenTwoDeeplyNestedTypeSynonymsResolveToTheSameDatatypes-v1.dar",
          "upgrades-SucceedsWhenTwoDeeplyNestedTypeSynonymsResolveToTheSameDatatypes-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenTwoDeeplyNestedTypeSynonymsResolveToTheSameDatatypes-v1.dar",
            "upgrades-SucceedsWhenTwoDeeplyNestedTypeSynonymsResolveToTheSameDatatypes-v2.dar",
            None,
          )
        ),
      )
    }

    "Fails when two deeply nested type synonyms resolve to different datatypes" in {
      testPackages(
        Seq(
          "upgrades-FailsWhenTwoDeeplyNestedTypeSynonymsResolveToDifferentDatatypes-v1.dar",
          "upgrades-FailsWhenTwoDeeplyNestedTypeSynonymsResolveToDifferentDatatypes-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenTwoDeeplyNestedTypeSynonymsResolveToDifferentDatatypes-v1.dar",
            "upgrades-FailsWhenTwoDeeplyNestedTypeSynonymsResolveToDifferentDatatypes-v2.dar",
            Some("The upgraded template A has changed the types of some of its original fields."),
          )
        ),
      )
    }

    "Fails when datatype changes variety" in {
      testPackages(
        Seq(
          "upgrades-FailsWhenDatatypeChangesVariety-v1.dar",
          "upgrades-FailsWhenDatatypeChangesVariety-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenDatatypeChangesVariety-v1.dar",
            "upgrades-FailsWhenDatatypeChangesVariety-v2.dar",
            Some("The upgraded data type RecordToEnum has changed from a record to a enum."),
          )
        ),
      )
    }

    "Succeeds when adding non-optional fields to unserializable types" in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenAddingNonOptionalFieldsToUnserializableTypes-v1.dar",
          "upgrades-SucceedsWhenAddingNonOptionalFieldsToUnserializableTypes-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenAddingNonOptionalFieldsToUnserializableTypes-v1.dar",
            "upgrades-SucceedsWhenAddingNonOptionalFieldsToUnserializableTypes-v2.dar",
            None,
          )
        ),
      )
    }

    "Succeeds when changing variant of unserializable type" in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenChangingConstructorOfUnserializableType-v1.dar",
          "upgrades-SucceedsWhenChangingConstructorOfUnserializableType-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenChangingConstructorOfUnserializableType-v1.dar",
            "upgrades-SucceedsWhenChangingConstructorOfUnserializableType-v2.dar",
            None,
          )
        ),
      )
    }

    "Succeeds when deleting unserializable type" in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenDeletingUnserializableType-v1.dar",
          "upgrades-SucceedsWhenDeletingUnserializableType-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenDeletingUnserializableType-v1.dar",
            "upgrades-SucceedsWhenDeletingUnserializableType-v2.dar",
            None,
          )
        ),
      )
    }

    "Fails when making type unserializable" in {
      testPackages(
        Seq(
          "upgrades-FailsWhenMakingTypeUnserializable-v1.dar",
          "upgrades-FailsWhenMakingTypeUnserializable-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenMakingTypeUnserializable-v1.dar",
            "upgrades-FailsWhenMakingTypeUnserializable-v2.dar",
            Some(
              "The upgraded data type MyData was serializable and is now unserializable. Datatypes cannot change their serializability via upgrades."
            ),
          )
        ),
      )
    }

    // Copied interface tests
    "Succeeds when an interface is only defined in the initial package." in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenAnInterfaceIsOnlyDefinedInTheInitialPackage-v1.dar",
          "upgrades-SucceedsWhenAnInterfaceIsOnlyDefinedInTheInitialPackage-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenAnInterfaceIsOnlyDefinedInTheInitialPackage-v1.dar",
            "upgrades-SucceedsWhenAnInterfaceIsOnlyDefinedInTheInitialPackage-v2.dar",
            None,
          )
        ),
      )
    }

    "Fails when an interface is defined in an upgrading package when it was already in the prior package." in {
      testPackages(
        Seq(
          "upgrades-FailsWhenAnInterfaceIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage-v1.dar",
          "upgrades-FailsWhenAnInterfaceIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenAnInterfaceIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage-v1.dar",
            "upgrades-FailsWhenAnInterfaceIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage-v2.dar",
            // TODO (dylant-da): Re-enable this test once the -Wupgrade-interfaces
            // flag on the compiler goes away and interface upgrades are always an
            // error
            // Some(
            //  "Tried to upgrade interface I, but interfaces cannot be upgraded. They should be removed in any upgrading package."
            // ),
            None,
          )
        ),
      )
    }

    "Succeeds when an instance is added (upgraded package)." in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenAnInstanceIsAddedUpgradedPackage-v1.dar",
          "upgrades-SucceedsWhenAnInstanceIsAddedUpgradedPackage-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenAnInstanceIsAddedUpgradedPackage-v1.dar",
            "upgrades-SucceedsWhenAnInstanceIsAddedUpgradedPackage-v2.dar",
            None,
          )
        ),
      )
    }

    "Fails when an instance is replaced with a different instance of an identically named interface." in {
      testPackages(
        Seq(
          "upgrades-FailsWhenAnInstanceIsReplacedWithADifferentInstanceOfAnIdenticallyNamedInterface-v1.dar",
          "upgrades-FailsWhenAnInstanceIsReplacedWithADifferentInstanceOfAnIdenticallyNamedInterface-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenAnInstanceIsReplacedWithADifferentInstanceOfAnIdenticallyNamedInterface-v1.dar",
            "upgrades-FailsWhenAnInstanceIsReplacedWithADifferentInstanceOfAnIdenticallyNamedInterface-v2.dar",
            Some(
              "Implementation of interface .*:Dep:I by template T appears in package that is being upgraded, but does not appear in this package."
            ),
          )
        ),
      )
    }

    "Succeeds when an instance is added to a new template (upgraded package)." in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenAnInstanceIsAddedToNewTemplateUpgradedPackage-v1.dar",
          "upgrades-SucceedsWhenAnInstanceIsAddedToNewTemplateUpgradedPackage-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenAnInstanceIsAddedToNewTemplateUpgradedPackage-v1.dar",
            "upgrades-SucceedsWhenAnInstanceIsAddedToNewTemplateUpgradedPackage-v2.dar",
            None,
          )
        ),
      )
    }

    "Succeeds when an instance is added to a new template (separate dep)." in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenAnInstanceIsAddedToNewTemplateSeparateDep-v1.dar",
          "upgrades-SucceedsWhenAnInstanceIsAddedToNewTemplateSeparateDep-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenAnInstanceIsAddedToNewTemplateSeparateDep-v1.dar",
            "upgrades-SucceedsWhenAnInstanceIsAddedToNewTemplateSeparateDep-v2.dar",
            None,
          )
        ),
      )
    }

    "Succeeds even when non-serializable types are incompatible" in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenNonSerializableTypesAreIncompatible-v1.dar",
          "upgrades-SucceedsWhenNonSerializableTypesAreIncompatible-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenNonSerializableTypesAreIncompatible-v1.dar",
            "upgrades-SucceedsWhenNonSerializableTypesAreIncompatible-v2.dar",
            None,
          )
        ),
      )
    }

    "Fails when comparing types from packages with different names" in {
      testPackages(
        Seq(
          "upgrades-FailsWhenUpgradedFieldFromDifferentPackageName-v1.dar",
          "upgrades-FailsWhenUpgradedFieldFromDifferentPackageName-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenUpgradedFieldFromDifferentPackageName-v1.dar",
            "upgrades-FailsWhenUpgradedFieldFromDifferentPackageName-v2.dar",
            Some("The upgraded data type A has changed the types of some of its original fields."),
          )
        ),
      )
    }

    "Fails when comparing type constructors from other packages that resolve to incompatible types" in {
      testPackages(
        Seq(
          "upgrades-FailsWhenUpgradedFieldPackagesAreNotUpgradable-v1.dar",
          "upgrades-FailsWhenUpgradedFieldPackagesAreNotUpgradable-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenUpgradedFieldPackagesAreNotUpgradable-v1.dar",
            "upgrades-FailsWhenUpgradedFieldPackagesAreNotUpgradable-v2.dar",
            Some("The upgraded data type T has changed the types of some of its original fields."),
          )
        ),
      )
    }

    "FailWhenParamCountChanges" in {
      testPackages(
        Seq(
          "upgrades-FailWhenParamCountChanges-v1.dar",
          "upgrades-FailWhenParamCountChanges-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailWhenParamCountChanges-v1.dar",
            "upgrades-FailWhenParamCountChanges-v2.dar",
            Some(
              "The upgraded data type MyStruct has changed the number of type variables it has."
            ),
          )
        ),
      )
    }

    "SucceedWhenParamNameChanges" in {
      testPackages(
        Seq(
          "upgrades-SucceedWhenParamNameChanges-v1.dar",
          "upgrades-SucceedWhenParamNameChanges-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedWhenParamNameChanges-v1.dar",
            "upgrades-SucceedWhenParamNameChanges-v2.dar",
            None,
          )
        ),
      )
    }

    "SucceedWhenPhantomParamBecomesUsed" in {
      testPackages(
        Seq(
          "upgrades-SucceedWhenPhantomParamBecomesUsed-v1.dar",
          "upgrades-SucceedWhenPhantomParamBecomesUsed-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedWhenPhantomParamBecomesUsed-v1.dar",
            "upgrades-SucceedWhenPhantomParamBecomesUsed-v2.dar",
            None,
          )
        ),
      )
    }

    // TODO (dylant-da): Re-enable this test from 995efe7 after reversion in 20631

    "Succeeds when an exception is only defined in the initial package." in {
      testPackages(
        Seq(
          "upgrades-SucceedsWhenAnExceptionIsOnlyDefinedInTheInitialPackage-v1.dar",
          "upgrades-SucceedsWhenAnExceptionIsOnlyDefinedInTheInitialPackage-v2.dar",
        ),
        Seq(
          (
            "upgrades-SucceedsWhenAnExceptionIsOnlyDefinedInTheInitialPackage-v1.dar",
            "upgrades-SucceedsWhenAnExceptionIsOnlyDefinedInTheInitialPackage-v2.dar",
            None,
          )
        ),
      )
    }

    "Fails when an exception is defined in an upgrading package when it was already in the prior package." in {
      testPackages(
        Seq(
          "upgrades-FailsWhenAnExceptionIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage-v1.dar",
          "upgrades-FailsWhenAnExceptionIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage-v2.dar",
        ),
        Seq(
          (
            "upgrades-FailsWhenAnExceptionIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage-v1.dar",
            "upgrades-FailsWhenAnExceptionIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage-v2.dar",
            // TODO (dylant-da): Re-enable this test once the -Wupgrade-exceptions
            // flag on the compiler goes away and exception upgrades are always an
            // error
            // Some(
            //  "Tried to upgrade exception E, but exceptions cannot be upgraded. They should be removed in any upgrading package."
            // ),
            None,
          )
        ),
      )
    }
  }
}
