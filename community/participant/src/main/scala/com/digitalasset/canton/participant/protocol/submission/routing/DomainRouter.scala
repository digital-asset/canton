// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import com.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.ProcessedDisclosedContract
import com.digitalasset.canton.ledger.participant.state.v2.{SubmitterInfo, TransactionMeta}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.domain.DomainAliasManager
import com.digitalasset.canton.participant.protocol.TransactionProcessor.{
  TransactionSubmissionError,
  TransactionSubmitted,
}
import com.digitalasset.canton.participant.protocol.submission.routing.DomainRouter.inputContractRoutingParties
import com.digitalasset.canton.participant.protocol.{
  SerializableContractAuthenticator,
  SerializableContractAuthenticatorImpl,
}
import com.digitalasset.canton.participant.store.DomainConnectionConfigStore
import com.digitalasset.canton.participant.sync.TransactionRoutingError.ConfigurationErrors.{
  MultiDomainSupportNotEnabled,
  SubmissionDomainNotReady,
}
import com.digitalasset.canton.participant.sync.TransactionRoutingError.TopologyErrors.{
  NotConnectedToAllContractDomains,
  SubmitterAlwaysStakeholder,
  UnknownContractDomains,
}
import com.digitalasset.canton.participant.sync.TransactionRoutingError.{
  MalformedInputErrors,
  RoutingInternalError,
  UnableToQueryTopologySnapshot,
}
import com.digitalasset.canton.participant.sync.{
  ConnectedDomainsLookup,
  SyncDomain,
  TransactionRoutingError,
}
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DomainAlias, LfKeyResolver, LfPartyId}

import scala.concurrent.{ExecutionContext, Future}

/** The domain router routes transaction submissions from upstream to the right domain.
  *
  * Submitted transactions are inspected for which domains are involved based on the location of the involved contracts.
  *
  * @param submit Submits the given transaction to the given domain.
  *               The outer future completes after the submission has been registered as in-flight.
  *               The inner future completes after the submission has been sequenced or if it will never be sequenced.
  */
class DomainRouter(
    domainOfContracts: Seq[LfContractId] => Future[Map[LfContractId, DomainId]],
    domainIdResolver: DomainAlias => Option[DomainId],
    submit: DomainId => (
        SubmitterInfo,
        TransactionMeta,
        LfKeyResolver,
        WellFormedTransaction[WithoutSuffixes],
        TraceContext,
        Map[LfContractId, SerializableContract],
    ) => EitherT[Future, TransactionRoutingError, FutureUnlessShutdown[TransactionSubmitted]],
    contractsTransferer: ContractsTransfer,
    snapshotProvider: DomainId => Either[UnableToQueryTopologySnapshot.Failed, TopologySnapshot],
    serializableContractAuthenticator: SerializableContractAuthenticator,
    autoTransferTransaction: Boolean,
    domainSelectorFactory: DomainSelectorFactory,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends FlagCloseable
    with NamedLogging {

  import com.digitalasset.canton.util.ShowUtil.*

  def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      transaction: LfSubmittedTransaction,
      explicitlyDisclosedContracts: ImmArray[ProcessedDisclosedContract],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionRoutingError, FutureUnlessShutdown[TransactionSubmitted]] = {

    for {
      // do some sanity checks for invalid inputs (to not conflate these with broken nodes)
      _ <- EitherT.fromEither[Future](
        WellFormedTransaction.sanityCheckInputs(transaction).leftMap {
          case WellFormedTransaction.InvalidInput.InvalidParty(err) =>
            MalformedInputErrors.InvalidPartyIdentifier.Error(err)
        }
      )

      inputDisclosedContracts <- EitherT
        .fromEither[Future](
          for {
            inputDisclosedContracts <-
              explicitlyDisclosedContracts.toList
                .parTraverse(SerializableContract.fromDisclosedContract)
                .leftMap(MalformedInputErrors.InvalidDisclosedContract.Error)
            _ <- inputDisclosedContracts
              .traverse_(serializableContractAuthenticator.authenticate)
              .leftMap(MalformedInputErrors.DisclosedContractAuthenticationFailed.Error)
          } yield inputDisclosedContracts
        )

      metadata <- EitherT
        .fromEither[Future](
          TransactionMetadata.fromTransactionMeta(
            metaLedgerEffectiveTime = transactionMeta.ledgerEffectiveTime,
            metaSubmissionTime = transactionMeta.submissionTime,
            metaOptNodeSeeds = transactionMeta.optNodeSeeds,
          )
        )
        .leftMap(RoutingInternalError.IllformedTransaction)

      wfTransaction <- EitherT.fromEither[Future](
        WellFormedTransaction
          .normalizeAndCheck(transaction, metadata, WithoutSuffixes)
          .leftMap(RoutingInternalError.IllformedTransaction)
      )

      contractRoutingParties = inputContractRoutingParties(wfTransaction.unwrap)

      transactionData <- TransactionData.create(
        submitterInfo,
        transaction,
        transactionMeta.workflowId,
        domainOfContracts,
        domainIdResolver,
        contractRoutingParties,
        transactionMeta.optDomainId,
      )

      domainSelector <- domainSelectorFactory.create(transactionData)

      inputDomains = transactionData.inputContractsDomainData.domains

      isMultiDomainTx <- EitherT.liftF(isMultiDomainTx(inputDomains, transactionData.informees))

      domainRankTarget <-
        if (!isMultiDomainTx) {
          logger.debug(
            s"Choosing the domain as single-domain workflow for ${submitterInfo.commandId}"
          )
          domainSelector.forSingleDomain
        } else if (autoTransferTransaction) {
          logger.debug(
            s"Choosing the domain as multi-domain workflow for ${submitterInfo.commandId}"
          )
          chooseDomainForMultiDomain(domainSelector)
        } else {
          EitherT.leftT[Future, DomainRank](
            MultiDomainSupportNotEnabled.Error(
              transactionData.inputContractsDomainData.domains
            ): TransactionRoutingError
          )
        }
      _ <- contractsTransferer.transfer(
        domainRankTarget,
        submitterInfo,
      )
      _ = logger.info(s"submitting the transaction to the ${domainRankTarget.domainId}")
      transactionSubmittedF <- submit(domainRankTarget.domainId)(
        submitterInfo,
        transactionMeta,
        keyResolver,
        wfTransaction,
        traceContext,
        inputDisclosedContracts.view.map(sc => sc.contractId -> sc).toMap,
      )
    } yield transactionSubmittedF
  }

  private def allInformeesOnDomain(
      informees: Set[LfPartyId]
  )(domainId: DomainId)(implicit traceContext: TraceContext): Future[Boolean] = {
    snapshotProvider(domainId).bimap(
      (err: UnableToQueryTopologySnapshot.Failed) => {
        logger.warn(
          s"Unable to get topology snapshot to check whether informees are hosted on the domain: $err"
        )
        Future.successful(false)
      },
      _.allHaveActiveParticipants(informees, _.isActive).value.map(_.isRight),
    )
  }.merge

  private def chooseDomainForMultiDomain(
      domainSelector: DomainSelector
  )(implicit traceContext: TraceContext): EitherT[Future, TransactionRoutingError, DomainRank] =
    for {
      _ <- checkValidityOfMultiDomain(
        domainSelector.transactionData
      )
      domainRankTarget <- domainSelector.forMultiDomain
    } yield domainRankTarget

  /** We have a multi-domain transaction if the input contracts are on more than one domain
    * or if the (single) input domain does not host all informees
    * (because we will need to transfer the contracts to a domain that that *does* host all informees.
    * Transactions without input contracts are always single-domain.
    */
  private def isMultiDomainTx(
      inputDomains: Set[DomainId],
      informees: Set[LfPartyId],
  )(implicit traceContext: TraceContext): Future[Boolean] = {
    if (inputDomains.sizeCompare(2) >= 0) Future.successful(true)
    else
      inputDomains.toList
        .parTraverse(allInformeesOnDomain(informees)(_))
        .map(!_.forall(identity))
  }

  private def checkValidityOfMultiDomain(
      transactionData: TransactionData
  ): EitherT[Future, TransactionRoutingError, Unit] = {
    val inputContractsDomainData = transactionData.inputContractsDomainData

    val allContractsHaveDomainData: Boolean = inputContractsDomainData.withoutDomainData.isEmpty
    val contractData = inputContractsDomainData.withDomainData
    val contractsDomainNotConnected = contractData.filter { contractData =>
      snapshotProvider(contractData.domain).left.exists { _: UnableToQueryTopologySnapshot.Failed =>
        true
      }
    }

    // Check that at least one submitter is a stakeholder so that we can transfer the contract if needed. This check
    // is overly strict on behalf of contracts that turn out not to need to be transferred.
    val submitterNotBeingStakeholder = contractData.filter { data =>
      data.stakeholders.intersect(transactionData.submitters).isEmpty
    }

    for {
      // Check: submitter
      _ <- EitherTUtil.condUnitET[Future](
        submitterNotBeingStakeholder.isEmpty,
        SubmitterAlwaysStakeholder.Error(submitterNotBeingStakeholder.map(_.id)),
      )

      // Check: all contracts have domain data
      _ <- EitherTUtil.condUnitET[Future](
        allContractsHaveDomainData, {
          val ids = inputContractsDomainData.withoutDomainData.map(_.show)
          UnknownContractDomains.Error(ids.toList)
        },
      )

      // Check: connected domains
      _ <- EitherTUtil
        .condUnitET[Future](
          contractsDomainNotConnected.isEmpty, {
            val contractsAndDomains: Map[String, DomainId] = contractsDomainNotConnected.map {
              contractData => contractData.id.show -> contractData.domain
            }.toMap

            NotConnectedToAllContractDomains.Error(contractsAndDomains)
          },
        )
        .leftWiden[TransactionRoutingError]

    } yield ()
  }
}

object DomainRouter {
  def apply(
      connectedDomains: ConnectedDomainsLookup,
      domainConnectionConfigStore: DomainConnectionConfigStore,
      domainAliasManager: DomainAliasManager,
      cryptoPureApi: CryptoPureApi,
      participantId: ParticipantId,
      autoTransferTransaction: Boolean,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): DomainRouter = {

    val transfer =
      new ContractsTransfer(
        connectedDomains,
        submittingParticipant = participantId,
        loggerFactory,
      )

    val domainIdResolver = recoveredDomainOfAlias(connectedDomains, domainAliasManager) _
    val stateProviderWithProtocolVersion = (domainId: DomainId) =>
      domainStateProvider(connectedDomains)(domainId)(TraceContext.empty)
    val stateProvider = (domainId: DomainId) =>
      domainStateProvider(connectedDomains)(domainId)(TraceContext.empty).map(_._1)

    val domainRankComputation = new DomainRankComputation(
      participantId = participantId,
      priorityOfDomain = priorityOfDomain(domainConnectionConfigStore, domainAliasManager),
      snapshotProvider = stateProvider,
      loggerFactory = loggerFactory,
    )

    val domainSelectorFactory = new DomainSelectorFactory(
      admissibleDomains = new AdmissibleDomains(participantId, connectedDomains, loggerFactory),
      priorityOfDomain = priorityOfDomain(domainConnectionConfigStore, domainAliasManager),
      domainRankComputation = domainRankComputation,
      domainStateProvider = stateProviderWithProtocolVersion,
      loggerFactory = loggerFactory,
    )

    val serializableContractAuthenticator = new SerializableContractAuthenticatorImpl(
      // This unicum generator is used for all domains uniformly. This means that domains cannot specify
      // different unicum generator strategies (e.g., different hash functions).
      new UnicumGenerator(cryptoPureApi)
    )

    new DomainRouter(
      domainOfContract(connectedDomains)(_)(ec, TraceContext.empty),
      domainIdResolver,
      submit(connectedDomains),
      transfer,
      stateProvider,
      serializableContractAuthenticator,
      autoTransferTransaction = autoTransferTransaction,
      domainSelectorFactory,
      timeouts,
      loggerFactory,
    )
  }

  private def domainOfContract(connectedDomains: ConnectedDomainsLookup)(
      coids: Seq[LfContractId]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Map[LfContractId, DomainId]] = {
    type Acc = (Seq[LfContractId], Map[LfContractId, DomainId])
    connectedDomains.snapshot
      .collect {
        // only look at domains that are ready for submission
        case (_, syncDomain: SyncDomain) if syncDomain.readyForSubmission => syncDomain
      }
      .toList
      .foldM[Future, Acc]((coids, Map.empty[LfContractId, DomainId]): Acc) {
        // if there are no more cids for which we don't know the domain, we are done
        case ((pending, acc), _) if pending.isEmpty => Future.successful((pending, acc))
        case ((pending, acc), syncDomain) =>
          // grab the approximate state and check if the contract is currently active on the given domain
          syncDomain.ephemeral.requestTracker.getApproximateStates(pending).map { res =>
            val done = acc ++ res.collect {
              case (cid, status) if status.status.isActive => (cid, syncDomain.domainId)
            }
            (pending.filterNot(cid => done.contains(cid)), done)
          }
      }
      .map(_._2)
  }

  private def recoveredDomainOfAlias(
      connectedDomains: ConnectedDomainsLookup,
      domainAliasManager: DomainAliasManager,
  )(domainAlias: DomainAlias): Option[DomainId] = {
    domainAliasManager
      .domainIdForAlias(domainAlias)
      .filter(domainId => connectedDomains.get(domainId).exists(_.ready))
  }

  private def domainStateProvider(connectedDomains: ConnectedDomainsLookup)(domain: DomainId)(
      implicit traceContext: TraceContext
  ): Either[UnableToQueryTopologySnapshot.Failed, (TopologySnapshot, ProtocolVersion)] =
    connectedDomains
      .get(domain)
      .toRight(UnableToQueryTopologySnapshot.Failed(domain))
      .map { syncDomain =>
        (
          syncDomain.topologyClient.currentSnapshotApproximation,
          syncDomain.staticDomainParameters.protocolVersion,
        )
      }

  private def priorityOfDomain(
      domainConnectionConfigStore: DomainConnectionConfigStore,
      domainAliasManager: DomainAliasManager,
  )(domainId: DomainId): Int = {
    val maybePriority = for {
      domainAlias <- domainAliasManager.aliasForDomainId(domainId)
      config <- domainConnectionConfigStore.get(domainAlias).toOption.map(_.config)
    } yield config.priority

    // If the participant is disconnected from the domain while this code is evaluated,
    // we may fail to determine the priority.
    // Choose the lowest possible priority, as it will be unlikely that a submission to the domain succeeds.
    maybePriority.getOrElse(Integer.MIN_VALUE)
  }

  /** We intentionally do not store the `keyResolver` in [[com.digitalasset.canton.protocol.WellFormedTransaction]]
    * because we do not (yet) need to deal with merging the mappings
    * in [[com.digitalasset.canton.protocol.WellFormedTransaction.merge]].
    */
  private def submit(connectedDomains: ConnectedDomainsLookup)(domainId: DomainId)(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      tx: WellFormedTransaction[WithoutSuffixes],
      traceContext: TraceContext,
      disclosedContracts: Map[LfContractId, SerializableContract],
  )(implicit
      ec: ExecutionContext
  ): EitherT[Future, TransactionRoutingError, FutureUnlessShutdown[TransactionSubmitted]] =
    for {
      domain <- EitherT.fromEither[Future](
        connectedDomains
          .get(domainId)
          .toRight[TransactionRoutingError](SubmissionDomainNotReady.Error(domainId))
      )
      _ <- EitherT.cond[Future](domain.ready, (), SubmissionDomainNotReady.Error(domainId))
      result <- wrapSubmissionError(domain.domainId)(
        domain.submitTransaction(
          submitterInfo,
          transactionMeta,
          keyResolver,
          tx,
          disclosedContracts,
        )(traceContext)
      )
    } yield result

  private def wrapSubmissionError[T](domainId: DomainId)(
      eitherT: EitherT[Future, TransactionSubmissionError, T]
  )(implicit ec: ExecutionContext): EitherT[Future, TransactionRoutingError, T] =
    eitherT.leftMap(subm => TransactionRoutingError.SubmissionError(domainId, subm))

  private[routing] def inputContractRoutingParties(
      tx: LfVersionedTransaction
  ): Map[LfContractId, Set[Ref.Party]] = {

    val keyLookupMap = tx.nodes.values.collect { case LfNodeLookupByKey(_, key, Some(cid), _) =>
      cid -> key.maintainers
    }.toMap

    val mainMap = tx.nodes.values.collect {
      case n: LfNodeFetch => n.coid -> n.stakeholders
      case n: LfNodeExercises => n.targetCoid -> n.stakeholders
    }.toMap

    (keyLookupMap ++ mainMap) -- tx.localContracts.keySet

  }

}
