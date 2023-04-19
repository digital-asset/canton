// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import cats.syntax.foldable.*
import cats.syntax.functorFilter.*
import cats.syntax.traverse.*
import com.daml.ledger.api
import com.daml.ledger.api.v1.admin.package_management_service.PackageDetails
import com.daml.ledger.api.v1.admin.party_management_service.PartyDetails as ProtoPartyDetails
import com.daml.ledger.api.v1.command_completion_service.Checkpoint
import com.daml.ledger.api.v1.commands.{Command, DisclosedContract}
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.event_query_service.{
  GetEventsByContractIdResponse,
  GetEventsByContractKeyResponse,
}
import com.daml.ledger.api.v1.ledger_configuration_service.LedgerConfiguration
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.client.binding.{Contract, TemplateCompanion}
import com.daml.lf.data.Ref
import com.daml.metrics.api.MetricHandle.{Histogram, Meter}
import com.daml.metrics.api.{MetricHandle, MetricName, MetricsContext}
import com.digitalasset.canton.admin.api.client.commands.LedgerApiTypeWrappers.WrappedCreatedEvent
import com.digitalasset.canton.admin.api.client.commands.{
  LedgerApiCommands,
  ParticipantAdminCommands,
}
import com.digitalasset.canton.admin.api.client.data.TemplateId.templateIds
import com.digitalasset.canton.admin.api.client.data.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{ConsoleCommandTimeout, NonNegativeDuration}
import com.digitalasset.canton.console.CommandErrors.GenericCommandError
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  CommandSuccessful,
  ConsoleCommandResult,
  ConsoleEnvironment,
  ConsoleMacros,
  FeatureFlag,
  FeatureFlagFilter,
  Help,
  Helpful,
  LedgerApiCommandRunner,
  LocalParticipantReference,
  ParticipantReference,
  RemoteParticipantReference,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.api.domain.{
  IdentityProviderConfig,
  IdentityProviderId,
  JwksUrl,
}
import com.digitalasset.canton.ledger.api.{DeduplicationPeriod, domain}
import com.digitalasset.canton.ledger.client.services.admin.IdentityProviderConfigClient
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.networking.grpc.{GrpcError, RecordingStreamObserver}
import com.digitalasset.canton.participant.ledger.api.client.DecodeUtil
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.{DomainId, ParticipantId, PartyId}
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.ResourceUtil
import com.digitalasset.canton.{LedgerTransactionId, LfPartyId}
import com.google.protobuf.field_mask.FieldMask
import io.grpc.StatusRuntimeException
import io.grpc.stub.StreamObserver

import java.time.Instant
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.nowarn
import scala.concurrent.{Await, ExecutionContext}

trait BaseLedgerApiAdministration extends NoTracing {

  this: LedgerApiCommandRunner with NamedLogging with FeatureFlagFilter =>
  implicit protected val consoleEnvironment: ConsoleEnvironment
  protected implicit lazy val executionContext: ExecutionContext =
    consoleEnvironment.environment.executionContext
  protected val name: String

  protected def domainOfTransaction(transactionId: String): DomainId
  protected def optionallyAwait[Tx](
      tx: Tx,
      txId: String,
      optTimeout: Option[NonNegativeDuration],
  ): Tx
  private def timeouts: ConsoleCommandTimeout = consoleEnvironment.commandTimeouts
  protected def defaultLimit: PositiveInt =
    consoleEnvironment.environment.config.parameters.console.defaultLimit

  @Help.Summary("Group of commands that access the ledger-api", FeatureFlag.Testing)
  @Help.Group("Ledger Api")
  object ledger_api extends Helpful {

    @Help.Summary("Read from transaction stream", FeatureFlag.Testing)
    @Help.Group("Transactions")
    object transactions extends Helpful {

      @Help.Summary("Get ledger end", FeatureFlag.Testing)
      def end(): LedgerOffset =
        check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(LedgerApiCommands.TransactionService.GetLedgerEnd())
        })

      @Help.Summary("Get transaction trees", FeatureFlag.Testing)
      @Help.Description(
        """This function connects to the transaction tree stream for the given parties and collects transaction trees
          |until either `completeAfter` transaction trees have been received or `timeout` has elapsed.
          |The returned transaction trees can be filtered to be between the given offsets (default: no filtering).
          |If the participant has been pruned via `pruning.prune` and if `beginOffset` is lower than the pruning offset,
          |this command fails with a `NOT_FOUND` error."""
      )
      def trees(
          partyIds: Set[PartyId],
          completeAfter: Int,
          beginOffset: LedgerOffset =
            new LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN),
          endOffset: Option[LedgerOffset] = None,
          verbose: Boolean = true,
          timeout: NonNegativeDuration = timeouts.ledgerCommand,
      ): Seq[TransactionTree] = check(FeatureFlag.Testing)({
        val observer = new RecordingStreamObserver[TransactionTree](completeAfter)
        val filter = TransactionFilter(partyIds.map(_.toLf -> Filters()).toMap)
        mkResult(
          subscribe_trees(observer, filter, beginOffset, endOffset, verbose),
          "getTransactionTrees",
          observer,
          timeout,
        )
      })

      private def mkResult[Res](
          call: => AutoCloseable,
          requestDescription: String,
          observer: RecordingStreamObserver[Res],
          timeout: NonNegativeDuration,
      ): Seq[Res] = consoleEnvironment.run {
        try {
          ResourceUtil.withResource(call) { _ =>
            // Not doing noisyAwaitResult here, because we don't want to log warnings in case of a timeout.
            CommandSuccessful(Await.result(observer.result, timeout.duration))
          }
        } catch {
          case sre: StatusRuntimeException =>
            GenericCommandError(GrpcError(requestDescription, name, sre).toString)
          case _: TimeoutException => CommandSuccessful(observer.responses)
        }
      }

      @Help.Summary("Subscribe to the transaction tree stream", FeatureFlag.Testing)
      @Help.Description(
        """This function connects to the transaction tree stream and passes transaction trees to `observer` until
          |the stream is completed.
          |Only transaction trees for parties in `filter.filterByParty.keys` will be returned.
          |Use `filter = TransactionFilter(Map(myParty.toLf -> Filters()))` to return all trees for `myParty: PartyId`.
          |The returned transactions can be filtered to be between the given offsets (default: no filtering).
          |If the participant has been pruned via `pruning.prune` and if `beginOffset` is lower than the pruning offset,
          |this command fails with a `NOT_FOUND` error."""
      )
      def subscribe_trees(
          observer: StreamObserver[TransactionTree],
          filter: TransactionFilter,
          beginOffset: LedgerOffset =
            new LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN),
          endOffset: Option[LedgerOffset] = None,
          verbose: Boolean = true,
      ): AutoCloseable = {
        check(FeatureFlag.Testing)(
          consoleEnvironment.run {
            ledgerApiCommand(
              LedgerApiCommands.TransactionService.SubscribeTrees(
                observer,
                beginOffset,
                endOffset,
                filter,
                verbose,
              )
            )
          }
        )
      }

      @Help.Summary("Get flat transactions", FeatureFlag.Testing)
      @Help.Description(
        """This function connects to the flat transaction stream for the given parties and collects transactions
          |until either `completeAfter` transaction trees have been received or `timeout` has elapsed.
          |The returned transactions can be filtered to be between the given offsets (default: no filtering).
          |If the participant has been pruned via `pruning.prune` and if `beginOffset` is lower than the pruning offset,
          |this command fails with a `NOT_FOUND` error."""
      )
      def flat(
          partyIds: Set[PartyId],
          completeAfter: Int,
          beginOffset: LedgerOffset =
            new LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN),
          endOffset: Option[LedgerOffset] = None,
          verbose: Boolean = true,
          timeout: NonNegativeDuration = timeouts.ledgerCommand,
      ): Seq[Transaction] = check(FeatureFlag.Testing)({
        val observer = new RecordingStreamObserver[Transaction](completeAfter)
        val filter = TransactionFilter(partyIds.map(_.toLf -> Filters()).toMap)
        mkResult(
          subscribe_flat(observer, filter, beginOffset, endOffset, verbose),
          "getTransactions",
          observer,
          timeout,
        )
      })

      @Help.Summary("Subscribe to the flat transaction stream", FeatureFlag.Testing)
      @Help.Description("""This function connects to the flat transaction stream and passes transactions to `observer` until
          |the stream is completed.
          |Only transactions for parties in `filter.filterByParty.keys` will be returned.
          |Use `filter = TransactionFilter(Map(myParty.toLf -> Filters()))` to return all transactions for `myParty: PartyId`.
          |The returned transactions can be filtered to be between the given offsets (default: no filtering).
          |If the participant has been pruned via `pruning.prune` and if `beginOffset` is lower than the pruning offset,
          |this command fails with a `NOT_FOUND` error.""")
      def subscribe_flat(
          observer: StreamObserver[Transaction],
          filter: TransactionFilter,
          beginOffset: LedgerOffset =
            new LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN),
          endOffset: Option[LedgerOffset] = None,
          verbose: Boolean = true,
      ): AutoCloseable = {
        check(FeatureFlag.Testing)(
          consoleEnvironment.run {
            ledgerApiCommand(
              LedgerApiCommands.TransactionService.SubscribeFlat(
                observer,
                beginOffset,
                endOffset,
                filter,
                verbose,
              )
            )
          }
        )
      }

      @Help.Summary("Starts measuring throughput at the transaction service", FeatureFlag.Testing)
      @Help.Description(
        """This function will subscribe on behalf of `parties` to the transaction tree stream and
          |notify various metrics:
          |The metric `<name>.<metricSuffix>` counts the number of transaction trees emitted.
          |The metric `<name>.<metricSuffix>-tx-node-count` tracks the number of root events emitted as part of transaction trees.
          |The metric `<name>.<metricSuffix>-tx-size` tracks the number of bytes emitted as part of transaction trees.
          |
          |To stop measuring, you need to close the returned `AutoCloseable`.
          |Use the `onTransaction` parameter to register a callback that is called on every transaction tree."""
      )
      def start_measuring(
          parties: Set[PartyId],
          metricSuffix: String,
          onTransaction: TransactionTree => Unit = _ => (),
      )(implicit consoleEnvironment: ConsoleEnvironment): AutoCloseable =
        check(FeatureFlag.Testing) {

          val metricName = MetricName(name, metricSuffix)

          val observer: StreamObserver[TransactionTree] = new StreamObserver[TransactionTree] {

            @nowarn("cat=deprecation")
            val metricsFactory: MetricHandle.MetricsFactory =
              consoleEnvironment.environment.metricsFactory.metricsFactory

            val metric: Meter = metricsFactory.meter(metricName)
            val nodeCount: Histogram = metricsFactory.histogram(metricName :+ "tx-node-count")
            val transactionSize: Histogram = metricsFactory.histogram(metricName :+ "tx-size")

            override def onNext(tree: TransactionTree): Unit = {
              val s = tree.rootEventIds.size.toLong
              metric.mark(s)(MetricsContext.Empty)
              nodeCount.update(s)
              transactionSize.update(tree.serializedSize)(MetricsContext.Empty)
              onTransaction(tree)
            }

            override def onError(t: Throwable): Unit = t match {
              case t: StatusRuntimeException =>
                val err = GrpcError("start_measuring", name, t)
                err match {
                  case gaveUp: GrpcError.GrpcClientGaveUp if gaveUp.isClientCancellation =>
                    logger.info(s"Client cancelled measuring throughput (metric: $metricName).")
                  case _ =>
                    logger.warn(
                      s"An error occurred while measuring throughput (metric: $metricName). Stop measuring. $err"
                    )
                }
              case _: Throwable =>
                logger.warn(
                  s"An exception occurred while measuring throughput (metric: $metricName). Stop measuring.",
                  t,
                )
            }

            override def onCompleted(): Unit =
              logger.info(s"Stop measuring throughput (metric: $metricName).")
          }

          val filterParty = TransactionFilter(parties.map(_.toLf -> Filters()).toMap)

          logger.info(s"Start measuring throughput (metric: $metricName).")
          subscribe_trees(observer, filterParty, end(), verbose = false)
        }

      @Help.Summary("Get a (tree) transaction by its ID", FeatureFlag.Testing)
      @Help.Description(
        """Get a transaction tree from the transaction stream by its ID. Returns None if the transaction is not (yet)
          |known at the participant or if the transaction has been pruned via `pruning.prune`."""
      )
      def by_id(parties: Set[PartyId], id: String): Option[TransactionTree] =
        check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.TransactionService.GetTransactionById(parties.map(_.toLf), id)(
              consoleEnvironment.environment.executionContext
            )
          )
        })

      @Help.Summary("Get the domain that a transaction was committed over.", FeatureFlag.Testing)
      @Help.Description(
        """Get the domain that a transaction was committed over. Throws an error if the transaction is not (yet) known
          |to the participant or if the transaction has been pruned via `pruning.prune`."""
      )
      def domain_of(transactionId: String): DomainId =
        check(FeatureFlag.Testing)(domainOfTransaction(transactionId))
    }

    @Help.Summary("Submit commands", FeatureFlag.Testing)
    @Help.Group("Command Submission")
    object commands extends Helpful {

      @Help.Summary(
        "Submit command and wait for the resulting transaction, returning the transaction tree or failing otherwise",
        FeatureFlag.Testing,
      )
      @Help.Description(
        """Submits a command on behalf of the `actAs` parties, waits for the resulting transaction to commit and returns it.
          | If the timeout is set, it also waits for the transaction to appear at all other configured
          | participants who were involved in the transaction. The call blocks until the transaction commits or fails;
          | the timeout only specifies how long to wait at the other participants.
          | Fails if the transaction doesn't commit, or if it doesn't become visible to the involved participants in
          | the allotted time.
          | Note that if the optTimeout is set and the involved parties are concurrently enabled/disabled or their
          | participants are connected/disconnected, the command may currently result in spurious timeouts or may
          | return before the transaction appears at all the involved participants."""
      )
      def submit(
          actAs: Seq[PartyId],
          commands: Seq[Command],
          workflowId: String = "",
          commandId: String = "",
          optTimeout: Option[NonNegativeDuration] = Some(timeouts.ledgerCommand),
          deduplicationPeriod: Option[DeduplicationPeriod] = None,
          submissionId: String = "",
          minLedgerTimeAbs: Option[Instant] = None,
          readAs: Seq[PartyId] = Seq.empty,
          disclosedContracts: Seq[DisclosedContract] = Seq.empty,
      ): TransactionTree = check(FeatureFlag.Testing) {
        val tx = consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.CommandService.SubmitAndWaitTransactionTree(
              actAs.map(_.toLf),
              readAs.map(_.toLf),
              commands,
              workflowId,
              commandId,
              deduplicationPeriod,
              submissionId,
              minLedgerTimeAbs,
              disclosedContracts,
            )
          )
        }
        optionallyAwait(tx, tx.transactionId, optTimeout)
      }

      @Help.Summary(
        "Submit command and wait for the resulting transaction, returning the flattened transaction or failing otherwise",
        FeatureFlag.Testing,
      )
      @Help.Description(
        """Submits a command on behalf of the `actAs` parties, waits for the resulting transaction to commit, and returns the "flattened" transaction.
          | If the timeout is set, it also waits for the transaction to appear at all other configured
          | participants who were involved in the transaction. The call blocks until the transaction commits or fails;
          | the timeout only specifies how long to wait at the other participants.
          | Fails if the transaction doesn't commit, or if it doesn't become visible to the involved participants in
          | the allotted time.
          | Note that if the optTimeout is set and the involved parties are concurrently enabled/disabled or their
          | participants are connected/disconnected, the command may currently result in spurious timeouts or may
          | return before the transaction appears at all the involved participants."""
      )
      def submit_flat(
          actAs: Seq[PartyId],
          commands: Seq[Command],
          workflowId: String = "",
          commandId: String = "",
          optTimeout: Option[NonNegativeDuration] = Some(timeouts.ledgerCommand),
          deduplicationPeriod: Option[DeduplicationPeriod] = None,
          submissionId: String = "",
          minLedgerTimeAbs: Option[Instant] = None,
          readAs: Seq[PartyId] = Seq.empty,
          disclosedContracts: Seq[DisclosedContract] = Seq.empty,
      ): Transaction = check(FeatureFlag.Testing) {
        val tx = consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.CommandService.SubmitAndWaitTransaction(
              actAs.map(_.toLf),
              readAs.map(_.toLf),
              commands,
              workflowId,
              commandId,
              deduplicationPeriod,
              submissionId,
              minLedgerTimeAbs,
              disclosedContracts,
            )
          )
        }
        optionallyAwait(tx, tx.transactionId, optTimeout)
      }

      @Help.Summary("Submit command asynchronously", FeatureFlag.Testing)
      @Help.Description(
        """Provides access to the command submission service of the Ledger APi.
          |See https://docs.daml.com/app-dev/services.html for documentation of the parameters."""
      )
      def submit_async(
          actAs: Seq[PartyId],
          commands: Seq[Command],
          workflowId: String = "",
          commandId: String = "",
          deduplicationPeriod: Option[DeduplicationPeriod] = None,
          submissionId: String = "",
          minLedgerTimeAbs: Option[Instant] = None,
          readAs: Seq[PartyId] = Seq.empty,
          disclosedContracts: Seq[DisclosedContract] = Seq.empty,
      ): Unit = check(FeatureFlag.Testing) {
        consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.CommandSubmissionService.Submit(
              actAs.map(_.toLf),
              readAs.map(_.toLf),
              commands,
              workflowId,
              commandId,
              deduplicationPeriod,
              submissionId,
              minLedgerTimeAbs,
              disclosedContracts,
            )
          )
        }
      }

    }

    @Help.Summary("Read active contracts", FeatureFlag.Testing)
    @Help.Group("Active Contracts")
    object acs extends Helpful {
      @Help.Summary("List the set of active contracts of a given party", FeatureFlag.Testing)
      @Help.Description(
        """This command will return the current set of active contracts for the given party.

           Supported arguments:
           - party: for which party you want to load the acs
           - limit: limit (default set via canton.parameter.console)
           - filterTemplate: list of templates ids to filter for
        """
      )
      def of_party(
          party: PartyId,
          limit: PositiveInt = defaultLimit,
          verbose: Boolean = true,
          filterTemplates: Seq[TemplateId] = Seq.empty,
          timeout: NonNegativeDuration = timeouts.unbounded,
      ): Seq[WrappedCreatedEvent] =
        check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.AcsService
              .GetActiveContracts(
                Set(party.toLf),
                limit,
                filterTemplates,
                verbose,
                timeout.asFiniteApproximation,
              )(consoleEnvironment.environment.scheduler)
          )
        })

      @Help.Summary(
        "List the set of active contracts for all parties hosted on this participant",
        FeatureFlag.Testing,
      )
      @Help.Description(
        "If the filterTemplates argument is not empty, the acs lookup will filter by the given templates."
      )
      def of_all(
          limit: PositiveInt = defaultLimit,
          verbose: Boolean = true,
          filterTemplates: Seq[TemplateId] = Seq.empty,
          timeout: NonNegativeDuration = timeouts.unbounded,
      ): Seq[WrappedCreatedEvent] = check(FeatureFlag.Testing)(
        consoleEnvironment.run {
          ConsoleCommandResult.fromEither(for {
            parties <- ledgerApiCommand(
              LedgerApiCommands.PartyManagementService.ListKnownParties()
            ).toEither
            localParties <- parties.filter(_.isLocal).map(_.party).traverse(LfPartyId.fromString)
            res <- {
              if (localParties.isEmpty) Right(Seq.empty)
              else {
                ledgerApiCommand(
                  LedgerApiCommands.AcsService.GetActiveContracts(
                    localParties.toSet,
                    limit,
                    filterTemplates,
                    verbose,
                    timeout.asFiniteApproximation,
                  )(consoleEnvironment.environment.scheduler)
                ).toEither
              }
            }
          } yield res)
        }
      )

      @Help.Summary(
        "Wait until the party sees the given contract in the active contract service",
        FeatureFlag.Testing,
      )
      @Help.Description(
        "Will throw an exception if the contract is not found to be active within the given timeout"
      )
      def await_active_contract(
          party: PartyId,
          contractId: LfContractId,
          timeout: NonNegativeDuration = timeouts.ledgerCommand,
      ): Unit = check(FeatureFlag.Testing) {
        ConsoleMacros.utils.retry_until_true(timeout) {
          of_party(party, verbose = false)
            .exists(_.event.contractId == contractId.coid)
        }
      }

      @Help.Summary("Wait until a contract becomes available", FeatureFlag.Testing)
      @Help.Description(
        """This function can be used for contracts with a code-generated Scala model.
          |You can refine your search using the `filter` function argument.
          |The command will wait until the contract appears or throw an exception once it times out."""
      )
      def await[T](
          partyId: PartyId,
          companion: TemplateCompanion[T],
          predicate: Contract[T] => Boolean = (x: Contract[T]) => true,
          timeout: NonNegativeDuration = timeouts.ledgerCommand,
      ): Contract[T] = check(FeatureFlag.Testing)({
        val result = new AtomicReference[Option[Contract[T]]](None)
        ConsoleMacros.utils.retry_until_true(timeout) {
          val tmp = filter(partyId, companion, predicate)
          result.set(tmp.headOption)
          tmp.nonEmpty
        }
        consoleEnvironment.run {
          ConsoleCommandResult.fromEither(
            result
              .get()
              .toRight(s"Failed to find contract of type ${companion.id} after ${timeout}")
          )
        }
      })

      @Help.Summary(
        "Filter the ACS for contracts of a particular Scala code-generated template",
        FeatureFlag.Testing,
      )
      @Help.Description(
        """To use this function, ensure a code-generated Scala model for the target template exists.
          |You can refine your search using the `predicate` function argument."""
      )
      def filter[T](
          partyId: PartyId,
          templateCompanion: TemplateCompanion[T],
          predicate: Contract[T] => Boolean = (x: Contract[T]) => true,
      ): Seq[Contract[T]] = check(FeatureFlag.Testing)(
        of_party(partyId, filterTemplates = templateIds(templateCompanion.id))
          .map(_.event)
          .flatMap(DecodeUtil.decodeCreated(templateCompanion)(_).toList)
          .filter(predicate)
      )

      @Help.Summary("Generic search for contracts", FeatureFlag.Testing)
      @Help.Description(
        """This search function returns an untyped ledger-api event.
          |The find will wait until the contract appears or throw an exception once it times out."""
      )
      def find_generic(
          partyId: PartyId,
          filter: WrappedCreatedEvent => Boolean,
          timeout: NonNegativeDuration = timeouts.ledgerCommand,
      ): WrappedCreatedEvent = check(FeatureFlag.Testing) {
        def scan: Option[WrappedCreatedEvent] = of_party(partyId).find(filter(_))

        ConsoleMacros.utils.retry_until_true(timeout)(scan.isDefined)

        consoleEnvironment.run {
          ConsoleCommandResult.fromEither(scan.toRight(s"Failed to find contract for ${partyId}."))
        }
      }
    }

    @Help.Summary("Manage parties through the Ledger API", FeatureFlag.Testing)
    @Help.Group("Party Management")
    object parties extends Helpful {

      @Help.Summary("Allocate a new party", FeatureFlag.Testing)
      @Help.Description(
        """Allocates a new party on the ledger.
          party: a hint for generating the party identifier
          displayName: a human-readable name of this party
          annotations: key-value pairs associated with this party and stored locally on this Ledger API server"""
      )
      def allocate(
          party: String,
          displayName: String,
          annotations: Map[String, String] = Map.empty,
      ): PartyDetails = {
        val proto = check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.PartyManagementService.AllocateParty(
              partyIdHint = party,
              displayName = displayName,
              annotations = annotations,
            )
          )
        })
        PartyDetails.fromProtoPartyDetails(proto)
      }

      @Help.Summary("List parties known by the Ledger API server", FeatureFlag.Testing)
      def list(): Seq[PartyDetails] = {
        val proto = check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(LedgerApiCommands.PartyManagementService.ListKnownParties())
        })
        proto.map(PartyDetails.fromProtoPartyDetails)
      }

      @Help.Summary("Update participant-local party details")
      @Help.Description(
        """Currently you can update only the annotations.
           |You cannot update other user attributes.
          party: party to be updated,
          modifier: a function to modify the party details, e.g.: `partyDetails => { partyDetails.copy(annotations = partyDetails.annotations.updated("a", "b").removed("c")) }`"""
      )
      def update(
          party: PartyId,
          modifier: PartyDetails => PartyDetails,
      ): PartyDetails = {
        val rawDetails = get(party = party)
        val srcDetails = PartyDetails.fromProtoPartyDetails(rawDetails)
        val modifiedDetails = modifier(srcDetails)
        verifyOnlyModifiableFieldsWhereModified(srcDetails, modifiedDetails)
        val annotationsUpdate = makeAnnotationsUpdate(
          original = srcDetails.annotations,
          modified = modifiedDetails.annotations,
        )
        val rawUpdatedDetails = check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.PartyManagementService.Update(
              party = party,
              annotationsUpdate = Some(annotationsUpdate),
              resourceVersionO = Some(rawDetails.localMetadata.fold("")(_.resourceVersion)),
            )
          )
        })
        PartyDetails.fromProtoPartyDetails(rawUpdatedDetails)
      }

      private def verifyOnlyModifiableFieldsWhereModified(
          srcDetails: PartyDetails,
          modifiedDetails: PartyDetails,
      ): Unit = {
        val withAllowedUpdatesReverted = modifiedDetails.copy(annotations = srcDetails.annotations)
        if (withAllowedUpdatesReverted != srcDetails) {
          throw ModifyingNonModifiablePartyDetailsPropertiesError()
        }
      }

      private def get(party: PartyId): ProtoPartyDetails = {
        check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(LedgerApiCommands.PartyManagementService.GetParty(party = party))
        })
      }

    }

    @Help.Summary("Manage packages", FeatureFlag.Testing)
    @Help.Group("Package Management")
    object packages extends Helpful {

      @Help.Summary("Upload packages from Dar file", FeatureFlag.Testing)
      @Help.Description("""Uploading the Dar can be done either through the ledger Api server or through the Canton admin Api.
          |The Ledger Api is the portable method across ledgers. The Canton admin Api is more powerful as it allows for
          |controlling Canton specific behaviour.
          |In particular, a Dar uploaded using the ledger Api will not be available in the Dar store and can not be downloaded again.
          |Additionally, Dars uploaded using the ledger Api will be vetted, but the system will not wait
          |for the Dars to be successfully registered with all connected domains. As such, if a Dar is uploaded and then
          |used immediately thereafter, a command might bounce due to missing package vettings.""")
      def upload_dar(darPath: String): Unit = check(FeatureFlag.Testing) {
        consoleEnvironment.run {
          ledgerApiCommand(LedgerApiCommands.PackageService.UploadDarFile(darPath))
        }
      }

      @Help.Summary("List Daml Packages", FeatureFlag.Testing)
      def list(limit: PositiveInt = defaultLimit): Seq[PackageDetails] =
        check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(LedgerApiCommands.PackageService.ListKnownPackages(limit))
        })

    }

    @Help.Summary("Monitor progress of commands", FeatureFlag.Testing)
    @Help.Group("Command Completions")
    object completions extends Helpful {

      @Help.Summary("Read the current command completion offset", FeatureFlag.Testing)
      def end(): LedgerOffset =
        check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.CommandCompletionService.CompletionEnd()
          )
        })

      @Help.Summary("Lists command completions following the specified offset", FeatureFlag.Testing)
      @Help.Description(
        """If the participant has been pruned via `pruning.prune` and if `offset` is lower than
          |the pruning offset, this command fails with a `NOT_FOUND` error."""
      )
      def list(
          partyId: PartyId,
          atLeastNumCompletions: Int,
          offset: LedgerOffset,
          applicationId: String = LedgerApiCommands.applicationId,
          timeout: NonNegativeDuration = timeouts.ledgerCommand,
          filter: Completion => Boolean = _ => true,
      ): Seq[Completion] =
        check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.CommandCompletionService.CompletionRequest(
              partyId.toLf,
              offset,
              atLeastNumCompletions,
              timeout.asJavaApproximation,
              applicationId,
            )(filter, consoleEnvironment.environment.scheduler)
          )
        })

      @Help.Summary(
        "Lists command completions following the specified offset along with the checkpoints included in the completions",
        FeatureFlag.Testing,
      )
      @Help.Description(
        """If the participant has been pruned via `pruning.prune` and if `offset` is lower than
          |the pruning offset, this command fails with a `NOT_FOUND` error."""
      )
      def list_with_checkpoint(
          partyId: PartyId,
          atLeastNumCompletions: Int,
          offset: LedgerOffset,
          applicationId: String = LedgerApiCommands.applicationId,
          timeout: NonNegativeDuration = timeouts.ledgerCommand,
          filter: Completion => Boolean = _ => true,
      ): Seq[(Completion, Option[Checkpoint])] =
        check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.CommandCompletionService.CompletionCheckpointRequest(
              partyId.toLf,
              offset,
              atLeastNumCompletions,
              timeout,
              applicationId,
            )(filter, consoleEnvironment.environment.scheduler)
          )
        })

      @Help.Summary("Subscribe to the command completion stream", FeatureFlag.Testing)
      @Help.Description(
        """This function connects to the command completion stream and passes command completions to `observer` until
          |the stream is completed.
          |Only completions for parties in `parties` will be returned.
          |The returned completions start at `beginOffset` (default: `LEDGER_BEGIN`).
          |If the participant has been pruned via `pruning.prune` and if `beginOffset` is lower than the pruning offset,
          |this command fails with a `NOT_FOUND` error."""
      )
      def subscribe(
          observer: StreamObserver[Completion],
          parties: Seq[PartyId],
          beginOffset: LedgerOffset =
            new LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN),
      ): AutoCloseable = {
        check(FeatureFlag.Testing)(
          consoleEnvironment.run {
            ledgerApiCommand(
              LedgerApiCommands.CommandCompletionService.Subscribe(
                observer,
                parties.map(_.toLf),
                Some(beginOffset),
              )
            )
          }
        )
      }
    }

    @Help.Summary("Retrieve the ledger configuration", FeatureFlag.Testing)
    @Help.Group("Ledger Configuration")
    object configuration extends Helpful {

      @Help.Summary("Obtain the ledger configuration", FeatureFlag.Testing)
      @Help.Description("""Returns the current ledger configuration and subsequent updates until
           the expected number of configs was retrieved or the timeout is over.""")
      def list(
          expectedConfigs: Int = 1,
          timeout: NonNegativeDuration = timeouts.ledgerCommand,
      ): Seq[LedgerConfiguration] =
        check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.LedgerConfigurationService.GetLedgerConfiguration(
              expectedConfigs,
              timeout.asFiniteApproximation,
            )(consoleEnvironment.environment.scheduler)
          )
        })
    }

    @Help.Summary("Identity Provider Configuration Management", FeatureFlag.Testing)
    @Help.Group("Ledger Api Identity Provider Configuration Management")
    object identity_provider_config extends Helpful {
      @Help.Summary("Create a new identity provider configuration", FeatureFlag.Testing)
      @Help.Description(
        """Create an identity provider configuration. The request will fail if the maximum allowed number of separate configurations is reached."""
      )
      def create(
          identityProviderId: String,
          isDeactivated: Boolean = false,
          jwksUrl: String,
          issuer: String,
          audience: Option[String],
      ): IdentityProviderConfig = {
        val config = check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.IdentityProviderConfigs.Create(
              identityProviderId =
                IdentityProviderId.Id(Ref.LedgerString.assertFromString(identityProviderId)),
              isDeactivated = isDeactivated,
              jwksUrl = JwksUrl.assertFromString(jwksUrl),
              issuer = issuer,
              audience = audience,
            )
          )
        })
        IdentityProviderConfigClient.fromProtoConfig(config)
      }

      @Help.Summary("Update an identity provider", FeatureFlag.Testing)
      @Help.Description("""Update identity provider""")
      def update(
          identityProviderId: String,
          isDeactivated: Boolean = false,
          jwksUrl: String,
          issuer: String,
          audience: Option[String],
          updateMask: FieldMask,
      ): IdentityProviderConfig = {
        val config = check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.IdentityProviderConfigs.Update(
              domain.IdentityProviderConfig(
                IdentityProviderId.Id(Ref.LedgerString.assertFromString(identityProviderId)),
                isDeactivated,
                JwksUrl(jwksUrl),
                issuer,
                audience,
              ),
              updateMask,
            )
          )
        })
        IdentityProviderConfigClient.fromProtoConfig(config)
      }

      @Help.Summary("Delete an identity provider configuration", FeatureFlag.Testing)
      @Help.Description("""Delete an existing identity provider configuration""")
      def delete(identityProviderId: String): Unit = {
        check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.IdentityProviderConfigs.Delete(
              IdentityProviderId.Id(Ref.LedgerString.assertFromString(identityProviderId))
            )
          )
        })
      }

      @Help.Summary("Get an identity provider configuration", FeatureFlag.Testing)
      @Help.Description("""Get identity provider configuration by id""")
      def get(identityProviderId: String): IdentityProviderConfig = {
        val config = check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.IdentityProviderConfigs.Get(
              IdentityProviderId.Id(Ref.LedgerString.assertFromString(identityProviderId))
            )
          )
        })
        IdentityProviderConfigClient.fromProtoConfig(config)
      }

      @Help.Summary("List identity provider configurations", FeatureFlag.Testing)
      @Help.Description("""List all existing identity provider configurations""")
      def list(): Seq[IdentityProviderConfig] = {
        val configs = check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.IdentityProviderConfigs.List()
          )
        })
        configs.map(IdentityProviderConfigClient.fromProtoConfig)
      }
    }

    @Help.Summary("Manage Ledger Api Users", FeatureFlag.Testing)
    @Help.Group("Ledger Api Users")
    object users extends Helpful {

      @Help.Summary("Create a user with the given id", FeatureFlag.Testing)
      @Help.Description(
        """Users are used to dynamically managing the rights given to Daml applications.
          |They allow us to link a stable local identifier (of an application) with a set of parties.
          id: the id used to identify the given user
          actAs: the set of parties this user is allowed to act as
          primaryParty: the optional party that should be linked to this user by default
          readAs: the set of parties this user is allowed to read as
          participantAdmin: flag (default false) indicating if the user is allowed to use the admin commands of the Ledger Api
          isActive: flag (default true) indicating if the user is active
          annotations: the set of key-value pairs linked to this user
          """
      )
      def create(
          id: String,
          actAs: Set[PartyId] = Set(),
          primaryParty: Option[PartyId] = None,
          readAs: Set[PartyId] = Set(),
          participantAdmin: Boolean = false,
          isActive: Boolean = true,
          annotations: Map[String, String] = Map.empty,
      ): User = {
        val lapiUser = check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.Users.Create(
              id = id,
              actAs = actAs.map(_.toLf),
              primaryParty = primaryParty.map(_.toLf),
              readAs = readAs.map(_.toLf),
              participantAdmin = participantAdmin,
              isDeactivated = !isActive,
              annotations = annotations,
            )
          )
        })
        User.fromLapiUser(lapiUser)
      }

      @Help.Summary("Update a user", FeatureFlag.Testing)
      @Help.Description(
        """Currently you can update the annotations, active status and primary party.
          |You cannot update other user attributes.
          id: id of the user to be updated
          modifier: a function for modifying the user; e.g: `user => { user.copy(isActive = false, primaryParty = None, annotations = user.annotations.updated("a", "b").removed("c")) }`
          """
      )
      def update(
          id: String,
          modifier: User => User,
      ): User = {
        val rawUser = doGet(id)
        val srcUser = User.fromLapiUser(rawUser)
        val modifiedUser = modifier(srcUser)
        verifyOnlyModifiableFieldsWhereModified(srcUser, modifiedUser)
        val annotationsUpdate =
          makeAnnotationsUpdate(original = srcUser.annotations, modified = modifiedUser.annotations)
        val rawUpdatedUser = check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.Users.Update(
              id = id,
              annotationsUpdate = Some(annotationsUpdate),
              primaryPartyUpdate = Some(modifiedUser.primaryParty),
              isDeactivatedUpdate = Some(!modifiedUser.isActive),
              resourceVersionO = Some(rawUser.metadata.resourceVersion),
            )
          )
        })
        User.fromLapiUser(rawUpdatedUser)
      }

      @Help.Summary("Get the user data of the user with the given id", FeatureFlag.Testing)
      @Help.Description(
        """Fetch the data associated with the given user id failing if there is no such user.
          |You will get the user's primary party, active status and annotations.
          |If you need the user rights, use rights.list instead."""
      )
      def get(id: String): User = User.fromLapiUser(doGet(id))

      @Help.Summary("Delete a user", FeatureFlag.Testing)
      @Help.Description("""Delete a user by id.""")
      def delete(id: String): Unit =
        check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.Users.Delete(
              id
            )
          )
        })

      @Help.Summary("List users", FeatureFlag.Testing)
      @Help.Description("""List users of this participant node
          filterUser: filter results using the given filter string
          pageToken: used for pagination (the result contains a page token if there are further pages)
          pageSize: default page size before the filter is applied""")
      def list(
          filterUser: String = "",
          pageToken: String = "",
          pageSize: Int = 100,
      ): UsersPage = {
        val page: ListLedgerApiUsersResult = check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.Users.List(
              filterUser,
              pageToken,
              pageSize,
            )
          )
        })
        UsersPage(
          users = page.users.map(User.fromLapiUser),
          nextPageToken = page.nextPageToken,
        )
      }

      def verifyOnlyModifiableFieldsWhereModified(srcUser: User, modifiedUser: User): Unit = {
        val withAllowedUpdatesReverted = modifiedUser.copy(
          primaryParty = srcUser.primaryParty,
          isActive = srcUser.isActive,
          annotations = srcUser.annotations,
        )
        if (withAllowedUpdatesReverted != srcUser) {
          throw ModifyingNonModifiableUserPropertiesError()
        }
      }

      private def doGet(id: String): LedgerApiUser = {
        check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(LedgerApiCommands.Users.Get(id))
        })
      }

      @Help.Summary("Manage Ledger Api User Rights", FeatureFlag.Testing)
      @Help.Group("Ledger Api User Rights")
      object rights extends Helpful {

        @Help.Summary("Grant new rights to a user", FeatureFlag.Testing)
        @Help.Description("""Users are used to dynamically managing the rights given to Daml applications.
          |This function is used to grant new rights to an existing user.
          id: the id used to identify the given user
          actAs: the set of parties this user is allowed to act as
          readAs: the set of parties this user is allowed to read as
          participantAdmin: flag (default false) indicating if the user is allowed to use the admin commands of the Ledger Api
          """)
        def grant(
            id: String,
            actAs: Set[PartyId],
            readAs: Set[PartyId] = Set(),
            participantAdmin: Boolean = false,
        ): UserRights =
          check(FeatureFlag.Testing)(consoleEnvironment.run {
            ledgerApiCommand(
              LedgerApiCommands.Users.Rights.Grant(
                id,
                actAs.map(_.toLf),
                readAs.map(_.toLf),
                participantAdmin,
              )
            )
          })

        @Help.Summary("Revoke user rights", FeatureFlag.Testing)
        @Help.Description("""Use to revoke specific rights from a user.
          id: the id used to identify the given user
          actAs: the set of parties this user should not be allowed to act as
          readAs: the set of parties this user should not be allowed to read as
          participantAdmin: if set to true, the participant admin rights will be removed
          """)
        def revoke(
            id: String,
            actAs: Set[PartyId],
            readAs: Set[PartyId] = Set(),
            participantAdmin: Boolean = false,
        ): UserRights =
          check(FeatureFlag.Testing)(consoleEnvironment.run {
            ledgerApiCommand(
              LedgerApiCommands.Users.Rights.Revoke(
                id,
                actAs.map(_.toLf),
                readAs.map(_.toLf),
                participantAdmin,
              )
            )
          })

        @Help.Summary("List rights of a user", FeatureFlag.Testing)
        @Help.Description("""Lists the rights of a user, or the rights of the current user.""")
        def list(id: String): UserRights =
          check(FeatureFlag.Testing)(consoleEnvironment.run {
            ledgerApiCommand(
              LedgerApiCommands.Users.Rights.List(
                id
              )
            )
          })

      }

    }

    @Help.Summary("Retrieve the ledger metering", FeatureFlag.Testing)
    @Help.Group("Metering")
    object metering extends Helpful {

      @Help.Summary("Get the ledger metering report", FeatureFlag.Testing)
      @Help.Description("""Returns the current ledger metering report
           from: required from timestamp (inclusive)
           to: optional to timestamp
           application_id: optional application id to which we want to restrict the report
          """)
      def get_report(
          from: CantonTimestamp,
          to: Option[CantonTimestamp] = None,
          applicationId: Option[String] = None,
      ): String =
        check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.Metering.GetReport(
              from,
              to,
              applicationId,
            )
          )
        })
    }

    @Help.Summary("Interact with the time service", FeatureFlag.Testing)
    @Help.Group("Time")
    object time {
      @Help.Summary("Get the participants time", FeatureFlag.Testing)
      @Help.Description("""Returns the current timestamp of the participant which is either the
                         system clock or the static time""")
      def get(): CantonTimestamp =
        check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.Time.Get(timeouts.ledgerCommand.asFiniteApproximation)(
              consoleEnvironment.environment.scheduler
            )
          )
        })

      @Help.Summary("Set the participants time", FeatureFlag.Testing)
      @Help.Description(
        """Sets the participants time if the participant is running in static time mode"""
      )
      def set(currentTime: CantonTimestamp, nextTime: CantonTimestamp): Unit =
        check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(LedgerApiCommands.Time.Set(currentTime, nextTime))
        })

    }

    @Help.Summary("Query event details", FeatureFlag.Testing)
    @Help.Group("EventQuery")
    object event_query extends Helpful {

      @Help.Summary("Get events by contract Id", FeatureFlag.Testing)
      @Help.Description("""Return events associated with the given contract Id""")
      def by_contract_id(
          contractId: String,
          requestingParties: Seq[PartyId],
      ): GetEventsByContractIdResponse =
        check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.QueryService
              .GetEventsByContractId(contractId, requestingParties.map(_.toLf))
          )
        })

      @Help.Summary("Get events by contract key", FeatureFlag.Testing)
      @Help.Description("""Return events associated with the given contract key""")
      def by_contract_key(
          contractKey: api.v1.value.Value,
          requestingParties: Seq[PartyId],
          templateId: TemplateId,
          continuationToken: Option[String] = None,
      ): GetEventsByContractKeyResponse =
        check(FeatureFlag.Testing)(consoleEnvironment.run {
          ledgerApiCommand(
            LedgerApiCommands.QueryService
              .GetEventsByContractKey(
                contractKey,
                requestingParties.map(_.toLf),
                templateId,
                continuationToken,
              )
          )
        })
    }

  }

  /** @return The modified map where deletion from the original are represented as keys with empty values
    */
  private def makeAnnotationsUpdate(
      original: Map[String, String],
      modified: Map[String, String],
  ): Map[String, String] = {
    val deletions = original.removedAll(modified.keys).view.mapValues(_ => "").toMap
    modified.concat(deletions)
  }

}

trait LedgerApiAdministration extends BaseLedgerApiAdministration {
  this: LedgerApiCommandRunner with AdminCommandRunner with NamedLogging with FeatureFlagFilter =>
  implicit protected val consoleEnvironment: ConsoleEnvironment
  protected val name: String

  override protected def domainOfTransaction(transactionId: String): DomainId = {
    val txId = LedgerTransactionId.assertFromString(transactionId)
    consoleEnvironment.run {
      adminCommand(ParticipantAdminCommands.Inspection.LookupTransactionDomain(txId))
    }
  }

  import com.digitalasset.canton.util.ShowUtil.*

  private def awaitTransaction(
      transactionId: String,
      at: Map[ParticipantReference, PartyId],
      timeout: NonNegativeDuration,
  ): Unit = {
    def scan() = {
      at.map { case (participant, party) =>
        (
          participant,
          party,
          participant.ledger_api.transactions.by_id(Set(party), transactionId).isDefined,
        )
      }
    }
    ConsoleMacros.utils.retry_until_true(timeout)(
      scan().forall(_._3), {
        val res = scan().map { case (participant, party, res) =>
          s"${party.toString}@${participant.toString}: ${if (res) "observed" else "not observed"}"
        }
        s"Failed to observe transaction on all nodes: ${res.mkString(", ")}"
      },
    )

  }

  private[console] def involvedParticipants(
      transactionId: String
  ): Map[ParticipantReference, PartyId] = {
    val txDomain = ledger_api.transactions.domain_of(transactionId)
    // TODO(#6317)
    // There's a race condition here, in the unlikely circumstance that the party->participant mapping on the domain
    // changes during the command's execution. We'll have to live with it for the moment, as there's no convenient
    // way to get the record time of the transaction to pass to the parties.list call.
    val domainPartiesAndParticipants = consoleEnvironment.participants.all.iterator
      .filter(x => x.health.running() && x.health.initialized() && x.name == name)
      .flatMap(_.parties.list(filterDomain = txDomain.filterString))
      .toSet
    val domainParties = domainPartiesAndParticipants.map(_.party)
    // Read the transaction under the authority of all parties on the domain, in order to get the witness_parties
    // to be all the actual witnesses of the transaction. There's no other convenient way to get the full witnesses,
    // as the Exercise events don't contain the informees of the Exercise action.
    val tree = ledger_api.transactions
      .by_id(domainParties, transactionId)
      .getOrElse(
        throw new IllegalStateException(
          s"Can't find transaction by ID: ${transactionId}. Queried parties: $domainParties"
        )
      )
    val witnesses = tree.eventsById.values
      .flatMap { ev =>
        ev.kind.created.fold(Seq.empty[String])(ev => ev.witnessParties) ++
          ev.kind.exercised.fold(Seq.empty[String])(ev => ev.witnessParties)
      }
      .map(PartyId.tryFromProtoPrimitive)
      .toSet

    // A participant identity equality check that doesn't blow up if the participant isn't running
    def identityIs(pRef: ParticipantReference, id: ParticipantId): Boolean = pRef match {
      case lRef: LocalParticipantReference =>
        lRef.is_running && lRef.health.initialized() && lRef.id == id
      case rRef: RemoteParticipantReference =>
        rRef.health.initialized() && rRef.id == id
      case _ => false
    }

    // Map each involved participant to some party that witnessed the transaction (it doesn't matter which one)
    domainPartiesAndParticipants.toList.foldMapK { cand =>
      if (witnesses.contains(cand.party)) {
        val involvedConsoleParticipants = cand.participants.mapFilter { pd =>
          for {
            participantReference <- consoleEnvironment.participants.all
              .filter(x => x.health.running() && x.health.initialized())
              .find(identityIs(_, pd.participant))
            _ <- pd.domains.find(_.domain == txDomain)
          } yield participantReference
        }
        involvedConsoleParticipants
          .map(_ -> cand.party)
          .toMap
      } else Map.empty
    }
  }

  protected def optionallyAwait[Tx](
      tx: Tx,
      txId: String,
      optTimeout: Option[NonNegativeDuration],
  ): Tx = {
    optTimeout match {
      case None => tx
      case Some(timeout) =>
        val involved = involvedParticipants(txId)
        logger.debug(show"Awaiting transaction ${txId.unquoted} at ${involved.keys.mkShow()}")
        awaitTransaction(txId, involved, timeout)
        tx
    }
  }

}