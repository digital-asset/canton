// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.declarative

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.admin.api.client.commands.{GrpcAdminCommand, SequencerAdminCommands}
import com.digitalasset.canton.auth.CantonAdminToken
import com.digitalasset.canton.config
import com.digitalasset.canton.config.ClientConfig
import com.digitalasset.canton.console.GrpcAdminCommandRunner
import com.digitalasset.canton.console.declarative.DeclarativeApi.UpdateResult
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{CloseContext, LifeCycle, RunOnClosing}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.DeclarativeApiMetrics
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.sequencing.protocol.SubmissionRequestType
import com.digitalasset.canton.synchronizer.config.DeclarativeSequencerConfig
import com.digitalasset.canton.synchronizer.sequencer.BlockSequencerConfig.IndividualThroughputCapConfig
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

class DeclarativeSequencerApi(
    override val name: String,
    adminApiConfig: ClientConfig,
    override val consistencyTimeout: config.NonNegativeDuration,
    adminToken: => Option[CantonAdminToken],
    runnerFactory: String => GrpcAdminCommandRunner,
    val closeContext: CloseContext,
    val metrics: DeclarativeApiMetrics,
    val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends DeclarativeApi[DeclarativeSequencerConfig, Unit] {

  private val adminApiRunner = runnerFactory(CantonGrpcUtil.ApiName.AdminApi)
  closeContext.context
    .runOnOrAfterClose(new RunOnClosing {
      override def name: String = "stop-declarative-api"
      override def done: Boolean = false
      override def run()(implicit traceContext: TraceContext): Unit =
        LifeCycle.close(adminApiRunner)(logger)
    })(TraceContext.empty)
    .discard

  private def queryAdminApi[Result](
      command: GrpcAdminCommand[?, ?, Result]
  )(implicit traceContext: TraceContext): Either[String, Result] =
    queryApi(adminApiRunner, adminApiConfig, command).leftMap(_.str)

  override protected def activeAdminToken: Option[CantonAdminToken] = adminToken

  override protected def prepare(config: DeclarativeSequencerConfig)(implicit
      traceContext: TraceContext
  ): Either[String, Unit] = Right(())

  override protected def sync(config: DeclarativeSequencerConfig, prep: Unit)(implicit
      traceContext: TraceContext
  ): Either[String, DeclarativeApi.UpdateResult] = for {
    caps <- syncCaps(
      config.throughputCap,
      config.removeCaps,
      config.checkSelfConsistency,
    )
  } yield Seq(caps).foldLeft(UpdateResult())(_.merge(_))

  private def syncCaps(
      caps: Map[String, IndividualThroughputCapConfig],
      removeCaps: Boolean,
      checkSelfConsistent: Boolean,
  )(implicit traceContext: TraceContext): Either[String, UpdateResult] = {

    def toCap(str: String): Either[String, SubmissionRequestType] =
      SubmissionRequestType
        .fromStringForCap(str)
        .toRight(s"Invalid cap type $str in declarative config")

    def fetchCaps(): Either[String, Seq[(String, IndividualThroughputCapConfig)]] =
      SubmissionRequestType.capAdmissible
        .traverse { cap =>
          queryAdminApi(
            SequencerAdminCommands.GetThroughputCap(cap)
          ).map(_.map(c => (cap.name, c)))
        }
        .map(_.flatten)

    def getCap(capName: String): Either[String, Option[IndividualThroughputCapConfig]] =
      toCap(capName)
        .flatMap { cap =>
          queryAdminApi(
            SequencerAdminCommands.GetThroughputCap(cap)
          )
        }

    def add(name: String, config: IndividualThroughputCapConfig): Either[String, Unit] =
      toCap(name).flatMap { cap =>
        queryAdminApi(
          SequencerAdminCommands.SetThroughputCap(cap, Some(config))
        )
      }

    def update(name: String, config: IndividualThroughputCapConfig): Either[String, Unit] =
      toCap(name).flatMap { cap =>
        queryAdminApi(
          SequencerAdminCommands.SetThroughputCap(cap, Some(config))
        )
      }

    def remove(name: String): Either[String, Unit] =
      toCap(name).flatMap { cap =>
        queryAdminApi(
          SequencerAdminCommands.SetThroughputCap(cap, None)
        )
      }

    run[String, IndividualThroughputCapConfig](
      "throughput-caps",
      removeExcess = removeCaps,
      checkSelfConsistent = checkSelfConsistent,
      want = caps.toSeq,
      fetch = _ => fetchCaps(),
      get = getCap,
      add = { case (name, config) =>
        add(name, config)
      },
      upd = { case (name, config, _) =>
        update(name, config)
      },
      rm = (idp, _) => remove(idp),
    )
  }

}
