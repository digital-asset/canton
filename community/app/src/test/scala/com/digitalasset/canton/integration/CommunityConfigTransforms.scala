// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.config.{
  CantonCommunityConfig,
  CommunityDbConfig,
  CommunityStorageConfig,
  H2DbConfig,
  StorageConfig,
}
import com.digitalasset.canton.domain.config.CommunityDomainConfig
import com.digitalasset.canton.participant.config.CommunityParticipantConfig
import com.typesafe.config.{Config, ConfigValueFactory}
import monocle.macros.syntax.lens._

import scala.reflect.ClassTag
import scala.util.Random

object CommunityConfigTransforms {

  /** make the ec monitoring just emit debug messages and not warnings */
  def deadlockDetectionAsDebug(config: CantonCommunityConfig): CantonCommunityConfig =
    config.focus(_.monitoring.deadlockDetection.reportAsWarnings).replace(false)

  /** Parameterized version to allow specifying community or enterprise versions */
  def withUniqueDbName[SC <: StorageConfig, H2SC <: H2DbConfig with SC](
      nodeName: String,
      storageConfig: SC,
      mkH2: Config => H2SC,
  )(implicit h2Tag: ClassTag[H2SC]): SC =
    storageConfig match {
      case h2: H2SC =>
        // Make sure that each environment and its database names are unique by generating a random prefix
        val dbName = generateUniqueH2DatabaseName(nodeName)
        mkH2(
          h2.config.withValue(
            "url",
            ConfigValueFactory.fromAnyRef(
              s"jdbc:h2:mem:$dbName;MODE=PostgreSQL;LOCK_TIMEOUT=10000;DB_CLOSE_DELAY=-1"
            ),
          )
        )
      case x => x
    }

  def ammoniteWithoutConflicts: CantonCommunityConfig => CantonCommunityConfig =
    config =>
      config
        .focus(_.parameters.console.cacheDir)
        .replace(None) // don't use cache for testing

  def withUniqueDbName(
      nodeName: String,
      storageConfig: CommunityStorageConfig,
  ): CommunityStorageConfig =
    withUniqueDbName(nodeName, storageConfig, CommunityDbConfig.H2(_))

  def generateUniqueH2DatabaseName(nodeName: String): String = {
    val dbPrefix = Random.alphanumeric.take(8).map(_.toLower).mkString
    s"${dbPrefix}_$nodeName"
  }

  def updateAllDomainConfigs(
      update: (String, CommunityDomainConfig) => CommunityDomainConfig
  ): CantonCommunityConfig => CantonCommunityConfig =
    cantonConfig =>
      cantonConfig
        .focus(_.domains)
        .modify(_.map { case (dName, dConfig) => (dName, update(dName, dConfig)) })

  def updateAllParticipantConfigs(
      update: (String, CommunityParticipantConfig) => CommunityParticipantConfig
  ): CantonCommunityConfig => CantonCommunityConfig =
    cantonConfig =>
      cantonConfig
        .focus(_.participants)
        .modify(_.map { case (pName, pConfig) => (pName, update(pName, pConfig)) })

  def uniqueH2DatabaseNames: CantonCommunityConfig => CantonCommunityConfig = {
    updateAllDomainConfigs { case (nodeName, cfg) =>
      cfg.focus(_.storage).modify(CommunityConfigTransforms.withUniqueDbName(nodeName, _))
    } compose updateAllParticipantConfigs { case (nodeName, cfg) =>
      cfg.focus(_.storage).modify(CommunityConfigTransforms.withUniqueDbName(nodeName, _))
    }
  }

}
