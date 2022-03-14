// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import akka.actor.ActorSystem
import cats.implicits._
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.refinements.{ApiTypes => A}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.configuration.CommandClientConfiguration
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.AdminWorkflowServicesErrorGroup
import com.digitalasset.canton.error.{CantonError, DecodedRpcStatus}
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.ledger.api.client.{LedgerConnection, LedgerSubscription}
import com.digitalasset.canton.lifecycle._
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.config.{
  LocalParticipantConfig,
  ParticipantNodeParameters,
}
import com.digitalasset.canton.participant.ledger.api.CantonAdminToken
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.sync.SyncServiceInjectionError.PassiveReplica
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.TopologyManagerError.NoAppropriateSigningKeyInStore
import com.digitalasset.canton.topology.UniqueIdentifier
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.tracing.{NoTracing, Spanning, TraceContext, TracerProvider}
import com.digitalasset.canton.util.ResourceUtil.withResource
import com.digitalasset.canton.util.{DamlPackageLoader, EitherTUtil}
import com.google.protobuf.ByteString
import com.google.rpc.status.Status
import io.opentelemetry.api.trace.Tracer

import java.io.InputStream
import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success}

/** Manages our admin workflow applications (ping, dar distribution).
  * Currently each is individual application with their own ledger connection and acting independently.
  */
class AdminWorkflowServices(
    config: LocalParticipantConfig,
    parameters: ParticipantNodeParameters,
    packageService: PackageService,
    syncService: CantonSyncService,
    adminPartyId: UniqueIdentifier,
    hashOps: HashOps,
    adminToken: CantonAdminToken,
    protected val loggerFactory: NamedLoggerFactory,
    protected val clock: Clock,
    tracerProvider: TracerProvider,
)(implicit
    ec: ExecutionContextExecutor,
    scheduledExecutorService: ScheduledExecutorService,
    actorSystem: ActorSystem,
    tracer: Tracer,
    executionSequencerFactory: ExecutionSequencerFactory,
) extends FlagCloseableAsync
    with NamedLogging
    with Spanning
    with NoTracing {

  override protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

  if (syncService.isActive() && parameters.adminWorkflow.autoloadDar) {
    withNewTraceContext { implicit traceContext =>
      logger.debug("Loading admin workflows DAR")
      // load the admin workflows daml archive before moving forward
      // We use the pre-packaged dar from the resources/dar folder instead of the compiled one.
      loadDamlArchiveResource()
    }
  }

  private val adminParty = Converters.toParty(adminPartyId)
  val (pingSubscription, ping) = createService("admin-ping") { connection =>
    new PingService(
      connection,
      adminParty,
      parameters.adminWorkflow.bongTestMaxLevel,
      syncService.maxDeduplicationDuration, // Set the deduplication duration for Ping command to the maximum allowed.
      syncService.isActive(),
      loggerFactory,
      clock,
    )
  }

  val (darDistributionSubscription, darDistribution) = createService("admin-dar-distribution") {
    connection =>
      new DarDistributionService(
        connection,
        darContent =>
          DamlPackageLoader
            .validateDar("DarShareAccept", darContent, parameters.maxUnzippedDarSize),
        adminParty,
        packageService,
        hashOps,
        isActive = syncService.isActive(),
        loggerFactory = loggerFactory,
      )
  }

  protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Seq[AsyncOrSyncCloseable](
    SyncCloseable(
      "services",
      Lifecycle.close(
        pingSubscription,
        darDistributionSubscription,
        ping,
        darDistribution,
      )(logger),
    )
  )

  /** For the admin workflows to run inside the participant we require their daml packages to be loaded.
    * This assumes that the daml archive has been included on the classpath and can be loaded as a resource.
    * @throws RuntimeException if the daml archive cannot be found on the classpath
    */
  private def loadDamlArchiveResource()(implicit traceContext: TraceContext): Unit = {
    val bytes =
      withResource(AdminWorkflowServices.adminWorkflowDarInputStream())(ByteString.readFrom)
    parameters.processingTimeouts.unbounded.await_(s"Load Daml packages")(
      EitherTUtil.toFuture(
        packageService
          .appendDarFromByteString(
            bytes,
            AdminWorkflowServices.AdminWorkflowDarResourceName,
            vetAllPackages = true,
            synchronizeVetting = false,
          )
          .transform {
            case Right(_) => Right(())
            case Left(
                  CantonPackageServiceError.IdentityManagerParentError(
                    ParticipantTopologyManagerError
                      .IdentityManagerParentError(NoAppropriateSigningKeyInStore.Failure(_))
                  )
                ) =>
              // Log error by creating error object, but continue processing.
              AdminWorkflowServices.CanNotAutomaticallyVetAdminWorkflowPackage.Error()
              Right(())
            case Left(err) =>
              Left(new IllegalStateException(CantonError.stringFromContext(err)))
          }
      )
    )
  }

  /** The admin workflow services are connected directly to a participant replica so we do not need to retry if the replica is passive. */
  private def noRetryOnPassiveReplica: PartialFunction[Status, Boolean] = {
    case status: Status
        if DecodedRpcStatus.fromScalaStatus(status).exists(s => s.id == PassiveReplica.id) =>
      false
  }

  private def createConnection(
      applicationId: String,
      workflowId: String,
  ): (LedgerOffset, LedgerConnection) = {
    val appId = A.ApplicationId(applicationId)
    val ledgerApiConfig = config.ledgerApi
    val connection = LedgerConnection(
      ledgerApiConfig.clientConfig,
      appId,
      parameters.adminWorkflow.retries,
      adminParty,
      A.WorkflowId(workflowId),
      CommandClientConfiguration.default.copy(
        maxCommandsInFlight = 0, // set this to a silly value, to enforce it is never used
        maxParallelSubmissions =
          1000000, // We need a high value to work around https://github.com/digital-asset/daml/issues/8017
        // This defines the maximum timeout that can be specified on admin workflow services such as the ping command
        // The parameter name is misleading; it does not affect the deduplication period for the commands.
        defaultDeduplicationTime = parameters.adminWorkflow.submissionTimeout.unwrap,
      ),
      Some(adminToken.secret),
      parameters.processingTimeouts,
      loggerFactory,
      tracerProvider,
      noRetryOnPassiveReplica,
    )
    (parameters.processingTimeouts.unbounded.await()(connection.ledgerEnd), connection)
  }

  private def createService[S <: AdminWorkflowService](
      applicationId: String
  )(createService: LedgerConnection => S): (LedgerSubscription, S) = {
    val (offset, connection) = createConnection(applicationId, applicationId)
    val service = createService(connection)

    val subscription = connection.subscribeAsync(subscriptionName = applicationId, offset)(tx =>
      withSpan(s"$applicationId.processTransaction") { implicit traceContext => _ =>
        service.processTransaction(tx)
      }
    )

    subscription.completed onComplete {
      case Success(_) =>
        logger.debug(s"ledger subscription for admin service [$service] has completed normally")
      case Failure(ex) =>
        logger.warn(
          s"ledger subscription for admin service [$service] has completed with error",
          ex,
        )
    }

    (subscription, service)
  }
}

object AdminWorkflowServices extends AdminWorkflowServicesErrorGroup {
  private val AdminWorkflowDarResourceName: String = "dar/AdminWorkflows.dar"
  private def adminWorkflowDarInputStream(): InputStream =
    Option(
      PingService.getClass.getClassLoader.getResourceAsStream(AdminWorkflowDarResourceName)
    ) match {
      case Some(is) => is
      case None =>
        throw new IllegalStateException(
          s"Failed to load [$AdminWorkflowDarResourceName] from classpath"
        )
    }

  val AdminWorkflowPackages: Map[PackageId, Ast.Package] =
    DamlPackageLoader
      .getPackagesFromInputStream("AdminWorkflows", adminWorkflowDarInputStream())
      .valueOr(err =>
        throw new IllegalStateException(s"Unable to load admin workflow packages: $err")
      )
  @Explanation(
    """This error indicates that the admin workflow package could not be vetted. The admin workflows is 
      |a set of packages that are pre-installed and can be used for administrative processes. 
      |The error can happen if the participant is initialised manually but is missing the appropriate 
      |signing keys or certificates in order to issue new topology transactions within the participants
      |namespace.
      |The admin workflows can not be used until the participant has vetted the package."""
  )
  @Resolution(
    """This error can be fixed by ensuring that an appropriate vetting transaction is issued in the 
      |name of this participant and imported into this participant node.
      |If the corresponding certificates have been added after the participant startup, then 
      |this error can be fixed by either restarting the participant node, issuing the vetting transaction manually
      |or re-uploading the Dar (leaving the vetAllPackages argument as true)"""
  )
  object CanNotAutomaticallyVetAdminWorkflowPackage
      extends ErrorCode(
        id = "CAN_NOT_AUTOMATICALLY_VET_ADMIN_WORKFLOW_PACKAGE",
        ErrorCategory.BackgroundProcessDegradationWarning,
      ) {
    case class Error()(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            "Unable to vet `AdminWorkflows` automatically. Please ensure you vet this package before using one of the admin workflows."
        )

  }

}
