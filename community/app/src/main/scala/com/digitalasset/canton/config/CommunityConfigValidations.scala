// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import cats.data.{Validated, ValidatedNel}
import cats.syntax.traverse._
import cats.syntax.foldable._
import cats.syntax.functor._
import cats.syntax.functorFilter._

private[config] trait ConfigValidations[C <: CantonConfig] {
  final def validate(config: C): ValidatedNel[String, Unit] = validations.traverse_(_(config))

  protected val validations: List[C => ValidatedNel[String, Unit]]
}

object CommunityConfigValidations extends ConfigValidations[CantonCommunityConfig] {
  case class DbAccess(url: String, user: Option[String])

  private val Valid: ValidatedNel[String, Unit] = Validated.validNel(())
  type Validation = CantonCommunityConfig => ValidatedNel[String, Unit]

  override protected val validations: List[Validation] =
    List[Validation](
      noDuplicateStorage,
      atLeastOneNode,
    ) ++ genericValidations[CantonCommunityConfig]

  private[config] def genericValidations[C <: CantonConfig]: List[C => ValidatedNel[String, Unit]] =
    List(
      failOnSimClockWithNonZeroTopologyChangeDelay
    )

  /** Group node configs by db access to find matching db storage configs.
    * Overcomplicated types used are to work around that at this point nodes could have conflicting names so we can't just
    * throw them all in a single map.
    */
  private[config] def extractNormalizedDbAccess[C <: CantonConfig](
      nodeConfigs: Map[String, LocalNodeConfig]*
  ): Map[DbAccess, List[(String, LocalNodeConfig)]] = {
    // Basic attempt to normalize JDBC URL-based configuration and explicit property configuration
    // Limitations: Does not parse nor normalize the JDBC URLs
    def normalize(dbConfig: DbConfig): Option[DbAccess] = {
      import slick.util.ConfigExtensionMethods._

      val slickConfig = dbConfig.config

      def getPropStr(prop: String): Option[String] =
        slickConfig.getStringOpt(prop).orElse(slickConfig.getStringOpt(s"properties.$prop"))

      def getPropInt(prop: String): Option[Int] =
        slickConfig.getIntOpt(prop).orElse(slickConfig.getIntOpt(s"properties.$prop"))

      def extractUrl: Option[String] =
        getPropStr("url").orElse(getPropStr("jdbcUrl"))

      def extractServerPortDbAsUrl: Option[String] =
        for {
          server <- getPropStr("serverName")
          port <- getPropInt("portNumber")
          dbName <- getPropStr("databaseName")
          url = dbConfig match {
            case _: H2DbConfig => DbConfig.h2Url(dbName)
            case _: PostgresDbConfig => DbConfig.postgresUrl(server, port, dbName)
            // Assume Oracle
            case _ => DbConfig.oracleUrl(server, port, dbName)
          }
        } yield url

      val user = getPropStr("user")
      extractUrl.orElse(extractServerPortDbAsUrl).map(url => DbAccess(url = url, user = user))
    }

    // combine into a single list of name to config
    val configs = nodeConfigs.map(_.toList).foldLeft(List[(String, LocalNodeConfig)]())(_ ++ _)

    val withStorageConfigs = configs.mapFilter { case (name, config) =>
      config.storage match {
        case dbConfig: DbConfig => normalize(dbConfig).map((_, name, config))
        case _ => None
      }
    }

    withStorageConfigs
      .groupBy(_._1)
      .fmap(_.map { case (_, name, config) =>
        (name, config)
      })
  }

  private[config] def formatNodeList(nodes: List[(String, LocalNodeConfig)]): String =
    nodes.map { case (name, config) => s"${config.nodeTypeName} $name" }.mkString(",")

  /** Validate the config that the storage configuration is not shared between nodes. */
  private def noDuplicateStorage(config: CantonCommunityConfig): ValidatedNel[String, Unit] = {
    val dbAccessToNodes = extractNormalizedDbAccess(config.participants, config.domains)

    dbAccessToNodes.toList
      .traverse_ {
        case (dbAccess, nodes) if nodes.lengthCompare(1) > 0 =>
          Validated.invalidNel(s"Nodes ${formatNodeList(nodes)} share same DB access: $dbAccess")
        case _ => Validated.validNel(())
      }
  }

  private def failOnSimClockWithNonZeroTopologyChangeDelay(
      config: CantonConfig
  ): ValidatedNel[String, Unit] = {
    def check(str: String): ValidatedNel[String, Unit] = {
      config.domains.values.toList
        .traverse { domain =>
          Validated.condNel(domain.domainParameters.topologyChangeDelay.duration.isZero, (), str)
        }
        .map(_ => ())
    }
    (config.parameters.nonStandardConfig, config.parameters.clock) match {
      case (true, _) | (_, ClockConfig.WallClock(_)) => Valid
      case (_, ClockConfig.SimClock) =>
        check(
          "Using sim-clock with non-zero topology-change-delay requires non-standard config mode."
        )
      case (_, ClockConfig.RemoteClock(_)) =>
        check(
          "Using remote-clock with non-zero topology-change-delay requires non-standard config mode."
        )
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  private def atLeastOneNode(
      config: CantonCommunityConfig
  ): ValidatedNel[String, Unit] = {
    val CantonCommunityConfig(domains, participants, remoteDomains, remoteParticipants, _, _, _) =
      config
    Validated.condNel(
      Seq(domains, participants, remoteDomains, remoteParticipants)
        .exists(_.nonEmpty),
      (),
      "At least one node must be defined in the configuration",
    )

  }

}
