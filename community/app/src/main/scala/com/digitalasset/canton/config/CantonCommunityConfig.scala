// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import cats.data.ValidatedNel

import java.io.File
import cats.syntax.functor._
import com.digitalasset.canton.config.ConfigErrors.CantonConfigError
import com.digitalasset.canton.domain.config.{CommunityDomainConfig, RemoteDomainConfig}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.participant.config.{
  CommunityParticipantConfig,
  RemoteParticipantConfig,
}
import com.digitalasset.canton.tracing.TraceContext
import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}
import pureconfig.{ConfigReader, ConfigWriter}
import monocle.macros.syntax.lens._

import scala.annotation.nowarn

case class CantonCommunityConfig(
    domains: Map[String, CommunityDomainConfig] = Map.empty,
    participants: Map[String, CommunityParticipantConfig] = Map.empty,
    remoteDomains: Map[String, RemoteDomainConfig] = Map.empty,
    remoteParticipants: Map[String, RemoteParticipantConfig] = Map.empty,
    monitoring: MonitoringConfig = MonitoringConfig(),
    parameters: CantonParameters = CantonParameters(),
    features: CantonFeatures = CantonFeatures(),
) extends CantonConfig
    with ConfigDefaults[CantonCommunityConfig] {

  override type DomainConfigType = CommunityDomainConfig
  override type ParticipantConfigType = CommunityParticipantConfig

  /** renders the config as json (used for dumping config for diagnostic purposes) */
  override def dumpString: String = CantonCommunityConfig.makeConfidentialString(this)

  override def validate: ValidatedNel[String, Unit] = CommunityConfigValidations.validate(this)

  override def withDefaults: CantonCommunityConfig =
    this
      .focus(_.domains)
      .modify(_.fmap(_.withDefaults))
      .focus(_.participants)
      .modify(_.fmap(_.withDefaults))
}

@nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
object CantonCommunityConfig {

  private val logger: Logger = LoggerFactory.getLogger(classOf[CantonCommunityConfig])
  private val elc = ErrorLoggingContext(
    TracedLogger(logger),
    NamedLoggerFactory.root.properties,
    TraceContext.empty,
  )
  import pureconfig.generic.semiauto._
  import CantonConfig._

  @nowarn("cat=unused")
  private lazy implicit val cantonCommunityConfigReader: ConfigReader[CantonCommunityConfig] = {
    import CantonConfig.ConfigReaders._
    implicit val communityDomainConfigReader: ConfigReader[CommunityDomainConfig] =
      deriveReader[CommunityDomainConfig]
    implicit val communityParticipantConfigReader: ConfigReader[CommunityParticipantConfig] =
      deriveReader[CommunityParticipantConfig]

    deriveReader[CantonCommunityConfig]
  }

  @nowarn("cat=unused")
  private lazy implicit val cantonCommunityConfigWriter: ConfigWriter[CantonCommunityConfig] = {
    val writers = new CantonConfig.ConfigWriters(confidential = true)
    import writers._
    implicit val communityDomainConfigWriter: ConfigWriter[CommunityDomainConfig] =
      deriveWriter[CommunityDomainConfig]
    implicit val communityParticipantConfigWriter: ConfigWriter[CommunityParticipantConfig] =
      deriveWriter[CommunityParticipantConfig]

    deriveWriter[CantonCommunityConfig]
  }

  def load(config: Config)(implicit
      elc: ErrorLoggingContext = elc
  ): Either[CantonConfigError, CantonCommunityConfig] =
    CantonConfig.loadAndValidate[CantonCommunityConfig](config)

  def loadOrExit(config: Config)(implicit elc: ErrorLoggingContext = elc): CantonCommunityConfig =
    CantonConfig.loadOrExit[CantonCommunityConfig](config)

  def parseAndLoad(files: Seq[File])(implicit
      elc: ErrorLoggingContext = elc
  ): Either[CantonConfigError, CantonCommunityConfig] =
    CantonConfig.parseAndLoad[CantonCommunityConfig](files)

  def parseAndLoadOrExit(files: Seq[File])(implicit
      elc: ErrorLoggingContext = elc
  ): CantonCommunityConfig =
    CantonConfig.parseAndLoadOrExit[CantonCommunityConfig](files)

  def makeConfidentialString(config: CantonCommunityConfig): String =
    "canton " + ConfigWriter[CantonCommunityConfig]
      .to(config)
      .render(CantonConfig.defaultConfigRenderer)

}
