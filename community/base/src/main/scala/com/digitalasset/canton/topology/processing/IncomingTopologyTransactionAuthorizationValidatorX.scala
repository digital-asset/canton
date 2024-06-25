// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.Monoid
import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.foldable.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.AuthorizedTopologyTransactionX.{
  AuthorizedDecentralizedNamespaceDefinitionX,
  AuthorizedIdentifierDelegationX,
  AuthorizedNamespaceDelegationX,
}
import com.digitalasset.canton.topology.store.ValidatedTopologyTransactionX.GenericValidatedTopologyTransactionX
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyMappingX.RequiredAuthXAuthorizations
import com.digitalasset.canton.topology.transaction.TopologyTransactionX.GenericTopologyTransactionX
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

/** Data collection providing information with respect to what is affected by this update
  *
  * @param authChecks the set of Uids that is mentioned in the transaction such that we can load the certificates for the respective uids
  * @param cascadingNamespaces the set of namespaces where we had a namespace delegation change requiring a cascading update
  * @param cascadingUids the set of uids where we had a identifier delegation change requiring a cascading update
  */
private[processing] final case class UpdateAggregationX(
    authChecks: Set[UniqueIdentifier] = Set(),
    cascadingNamespaces: Set[Namespace] = Set(),
    cascadingUids: Set[UniqueIdentifier] = Set(),
) {

  /** returns all cascading uids which are not already covered by the cascading namespaces */
  def filteredCascadingUids: Set[UniqueIdentifier] =
    cascadingUids.filterNot(x => cascadingNamespaces.contains(x.namespace))

  /** returns true if the given uid is affected by a cascading update */
  def isCascading(uid: UniqueIdentifier): Boolean =
    cascadingNamespaces.contains(uid.namespace) || cascadingUids.contains(uid)

  def add(
      mapping: TopologyMappingX,
      currentTransaction: Option[GenericTopologyTransactionX],
  ): UpdateAggregationX = mapping match {
    case NamespaceDelegationX(ns, _, _) =>
      // change in certificate requires full recompute for namespace (add could unlock existing certificates, remove could make anything obsolete)
      this.copy(cascadingNamespaces = cascadingNamespaces + ns)
    case IdentifierDelegationX(uid, _) =>
      // change in identifier delegation requires full recompute for uid
      this.copy(cascadingUids = cascadingUids + uid, authChecks = authChecks + uid)
    case DecentralizedNamespaceDefinitionX(ns, _, owners) =>
      // change in decentralized namespace definition requires full recompute
      this.copy(cascadingNamespaces = cascadingNamespaces + ns ++ owners)
    case x =>
      this.copy(authChecks =
        authChecks ++ mapping.requiredAuth(currentTransaction).authorizations.uids
      )
  }

  def nothingCascading: Boolean = cascadingNamespaces.isEmpty && cascadingUids.isEmpty

  def authNamespaces: Set[Namespace] = authChecks.map(_.namespace) ++ cascadingNamespaces
}

object UpdateAggregationX {
  implicit val monoid: Monoid[UpdateAggregationX] = new Monoid[UpdateAggregationX] {
    override def empty: UpdateAggregationX = UpdateAggregationX()

    override def combine(x: UpdateAggregationX, y: UpdateAggregationX): UpdateAggregationX =
      UpdateAggregationX(
        authChecks = x.authChecks ++ y.authChecks,
        cascadingNamespaces = x.cascadingNamespaces ++ y.cascadingNamespaces,
        cascadingUids = x.cascadingUids ++ y.cascadingUids,
      )
  }
}

/** validate incoming topology transactions
  *
  * NOT THREAD SAFE. Note that this class is not thread safe
  *
  * we check three things:
  * (1) are the signatures valid
  * (2) are the signatures properly authorized
  *     a. load current set of authorized keys
  *     b. for each transaction, verify that the authorization keys are valid. a key is a valid authorization if there
  *        is a certificate chain that originates from the root certificate at the time when the
  *        transaction is added (one by one).
  *     c. if the transaction is a namespace or identifier delegation, update its impact on the authorization set
  *        this means that if we add or remove a namespace delegation, then we need to perform a cascading
  *        update that activates or deactivates states that depend on this delegation.
  * (3) finally, what we compute as the "authorized graph" is then used to compute the derived table
  *     of "namespace delegations"
  */
class IncomingTopologyTransactionAuthorizationValidatorX(
    val pureCrypto: CryptoPureApi,
    val store: TopologyStoreX[TopologyStoreId],
    domainId: Option[DomainId],
    validationIsFinal: Boolean,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with TransactionAuthorizationValidatorX {

  def reset(): Unit = {
    namespaceCache.clear()
    identifierDelegationCache.clear()
    decentralizedNamespaceCache.clear()
  }

  /** Validates the provided topology transactions and applies the certificates to the auth state
    *
    * When receiving topology transactions we have to evaluate them and continuously apply any
    * update to the namespace delegations or identifier delegations to the "head state".
    *
    * And we use that "head state" to verify if the transactions are authorized or not.
    */
  def validateAndUpdateHeadAuthState(
      timestamp: CantonTimestamp,
      toValidate: GenericSignedTopologyTransactionX,
      inStore: Option[GenericSignedTopologyTransactionX],
      expectFullAuthorization: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Future[(UpdateAggregationX, GenericValidatedTopologyTransactionX)] = {
    for {
      authCheckResult <- determineRelevantUidsAndNamespaces(toValidate, inStore.map(_.transaction))
      (updateAggregation, targetDomainVerified) = authCheckResult
      loadGraphsF = loadAuthorizationGraphs(timestamp, updateAggregation.authNamespaces)
      loadUidsF = loadIdentifierDelegationsCascading(
        timestamp,
        updateAggregation,
        updateAggregation.authChecks,
      )
      _ <- loadGraphsF
      cascadingUidsFromNamespace <- loadUidsF
    } yield {

      logger.debug(s"Update aggregation yielded ${updateAggregation}")
      val validated = targetDomainVerified match {
        case ValidatedTopologyTransactionX(tx, None, _) =>
          processTransaction(
            tx,
            inStore,
            expectFullAuthorization,
          )
        case v => v
      }
      // add any uid for which we have a valid identifier delegation to the cascading set (as a new namespace
      // certificate might activate an identifier delegation)
      (
        updateAggregation.copy(cascadingUids =
          updateAggregation.cascadingUids ++ cascadingUidsFromNamespace
        ),
        validated,
      )
    }
  }

  /** Validates a topology transaction as follows:
    * <ol>
    *   <li>check that the transaction has valid signatures and is sufficiently authorized. if not, reject.</li>
    *   <li>if there are no missing authorizers, as is the case for proposals, we update internal caches for NSD, IDD, and DND</li>
    *   <li>if this validation is run to determine a final verdict, as is the case for processing topology transactions coming from the domain,
    *   automatically clear the proposal flag for transactions with sufficent authorizing signatures.</li>
    * </ol>
    */
  private def processTransaction(
      toValidate: GenericSignedTopologyTransactionX,
      inStore: Option[GenericSignedTopologyTransactionX],
      expectFullAuthorization: Boolean,
  )(implicit traceContext: TraceContext): GenericValidatedTopologyTransactionX = {
    // See validateRootCertificate why we need to check the removal of a root certificate explicitly here.
    val signatureCheckResult = validateRootCertificate(toValidate)
      .getOrElse(validateSignaturesAndDetermineMissingAuthorizers(toValidate, inStore))

    signatureCheckResult match {
      // propagate the rejection reason
      case Left(rejectionReason) => ValidatedTopologyTransactionX(toValidate, Some(rejectionReason))

      // if a transaction wasn't outright rejected, run some additional checks
      case Right((validatedTx, missingAuthorizers)) =>
        handleSuccessfulSignatureChecks(
          validatedTx,
          missingAuthorizers,
          expectFullAuthorization,
        )
    }
  }

  private def handleSuccessfulSignatureChecks(
      toValidate: GenericSignedTopologyTransactionX,
      missingAuthorizers: RequiredAuthXAuthorizations,
      expectFullAuthorization: Boolean,
  )(implicit
      traceContext: TraceContext
  ): ValidatedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX] = {
    // if there are no missing authorizers, we can update the internal caches
    val isFullyAuthorized = if (missingAuthorizers.isEmpty) {
      val processedNSD = toValidate
        .selectMapping[NamespaceDelegationX]
        .forall { sigTx => processNamespaceDelegation(AuthorizedTopologyTransactionX(sigTx)) }

      val processedIDD = toValidate.selectMapping[IdentifierDelegationX].forall { sigTx =>
        processIdentifierDelegation(AuthorizedTopologyTransactionX(sigTx))
      }

      val processedDND =
        toValidate.selectMapping[DecentralizedNamespaceDefinitionX].forall { sigTx =>
          processDecentralizedNamespaceDefinition(AuthorizedTopologyTransactionX(sigTx))
        }
      val mappingSpecificCheck = processedNSD && processedIDD && processedDND
      if (!mappingSpecificCheck) {
        logger.debug(s"Mapping specific check failed")
      }
      mappingSpecificCheck
    } else { false }

    val acceptMissingAuthorizers =
      toValidate.isProposal && !expectFullAuthorization

    // if the result of this validation is final (when processing transactions for the authorized store
    // or sequenced transactions from the domain) we set the proposal flag according to whether the transaction
    // is fully authorized or not.
    // This must not be done when preliminarily validating transactions via the DomainTopologyManager, because
    // the validation outcome might change when validating the transaction again after it has been sequenced.
    val finalTransaction =
      if (validationIsFinal) toValidate.copy(isProposal = !isFullyAuthorized)
      else toValidate

    // Either the transaction is fully authorized or the request allows partial authorization
    if (isFullyAuthorized || acceptMissingAuthorizers) {
      ValidatedTopologyTransactionX(finalTransaction, None)
    } else {
      if (!missingAuthorizers.isEmpty) {
        logger.debug(s"Missing authorizers: $missingAuthorizers")
      }
      ValidatedTopologyTransactionX(
        toValidate,
        Some(TopologyTransactionRejection.NotAuthorized),
      )
    }
  }

  /**  Validates the signature of the removal of a root certificate.
    *  This check is done separately from the mechanism used for other topology transactions (ie isCurrentlyAuthorized),
    *  because removing a root certificate removes it from the authorization graph and therefore
    *  isCurrentlyAuthorized would not find the key to validate it.
    */
  private def validateRootCertificate(
      toValidate: GenericSignedTopologyTransactionX
  ): Option[Either[
    TopologyTransactionRejection,
    (GenericSignedTopologyTransactionX, RequiredAuthXAuthorizations),
  ]] = {
    toValidate
      .selectMapping[NamespaceDelegationX]
      .filter(NamespaceDelegationX.isRootCertificate)
      .map { rootCert =>
        val result = rootCert.signatures.toSeq.forgetNE
          .traverse_(
            pureCrypto
              .verifySignature(
                rootCert.hash.hash,
                rootCert.mapping.target,
                _,
              )
          )
          .bimap(
            TopologyTransactionRejection.SignatureCheckFailed,
            _ => (toValidate, RequiredAuthXAuthorizations.empty /* no missing authorizers */ ),
          )
        result
      }

  }

  /** loads all identifier delegations into the identifier delegation cache
    *
    * This function has two "modes". On a cascading update affecting namespaces, we have
    * to reload all identifier delegation certificates in order to figure out the affected
    * uids. The return Set then contains all the uids that were loaded as a result of the
    * namespace query.
    *
    * If there is no cascading namespace update, we just load the affected uids and return an empty set.
    */
  private def loadIdentifierDelegationsCascading(
      timestamp: CantonTimestamp,
      cascadingUpdate: UpdateAggregationX,
      transactionUids: Set[UniqueIdentifier],
  )(implicit traceContext: TraceContext): Future[Set[UniqueIdentifier]] = {
    // we need to load the identifier delegations for all the uids that are mentioned by a transactions
    val loadUids =
      (transactionUids ++ cascadingUpdate.cascadingUids) -- identifierDelegationCache.keySet
    if (loadUids.isEmpty && cascadingUpdate.cascadingNamespaces.isEmpty) {
      Future.successful(Set.empty[UniqueIdentifier])
    } else loadIdentifierDelegations(timestamp, cascadingUpdate.cascadingNamespaces.toSeq, loadUids)
  }

  private def processIdentifierDelegation(
      tx: AuthorizedIdentifierDelegationX
  ): Boolean = {
    // check authorization
    val check = getAuthorizationCheckForNamespace(tx.mapping.identifier.namespace)
    val keysAreValid = check.existsAuthorizedKeyIn(tx.signingKeys, requireRoot = false)
    // update identifier delegation cache if necessary
    if (keysAreValid) {
      val updateOp: Set[AuthorizedIdentifierDelegationX] => Set[AuthorizedIdentifierDelegationX] =
        tx.operation match {
          case TopologyChangeOpX.Replace =>
            x => x + tx
          case TopologyChangeOpX.Remove =>
            x => // using a filter as the key that authorized the removal might be different that authorized the addition
              x.filter(cur => cur.mapping != tx.mapping)
        }
      updateIdentifierDelegationCache(tx.mapping.identifier, updateOp)
    }
    keysAreValid
  }

  private def processNamespaceDelegation(
      tx: AuthorizedNamespaceDelegationX
  )(implicit traceContext: TraceContext): Boolean = {
    val graph = getAuthorizationGraphForNamespace(tx.mapping.namespace)
    // add or remove including authorization check
    tx.operation match {
      case TopologyChangeOpX.Replace => graph.add(tx)
      case TopologyChangeOpX.Remove => graph.remove(tx)
    }
  }

  /** Process decentralized namespace definition
    *
    * return whether decentralized namespace definition mapping is authorizable along with a "cache-update function" to be invoked
    * by the caller once the mapping is to be committed.
    */
  private def processDecentralizedNamespaceDefinition(
      tx: AuthorizedDecentralizedNamespaceDefinitionX
  )(implicit traceContext: TraceContext): Boolean = {
    val decentralizedNamespace = tx.mapping.namespace
    val dnsGraph = decentralizedNamespaceCache
      .get(decentralizedNamespace)
      .map { case (_, dnsGraph) => dnsGraph }
      .getOrElse {
        val serialToValidate = tx.serial
        if (serialToValidate > PositiveInt.one) {
          logger.warn(
            s"decentralizedNamespaceCache did not contain namespace $decentralizedNamespace even though the serial to validate is $serialToValidate"
          )
        }
        val directDecentralizedNamespaceGraph = namespaceCache.getOrElseUpdate(
          decentralizedNamespace,
          new AuthorizationGraphX(
            decentralizedNamespace,
            extraDebugInfo = false,
            loggerFactory,
          ),
        )
        val ownerGraphs = tx.mapping.owners.forgetNE.toSeq.map(getAuthorizationGraphForNamespace)
        val newDecentralizedNamespaceGraph = DecentralizedNamespaceAuthorizationGraphX(
          tx.mapping,
          directDecentralizedNamespaceGraph,
          ownerGraphs,
        )
        newDecentralizedNamespaceGraph
      }
    val isAuthorized = dnsGraph.existsAuthorizedKeyIn(tx.signingKeys, requireRoot = false)

    if (isAuthorized) {
      tx.operation match {
        case TopologyChangeOpX.Remove =>
          decentralizedNamespaceCache.remove(decentralizedNamespace).discard

        case TopologyChangeOpX.Replace =>
          val ownerGraphs = tx.mapping.owners.forgetNE.toSeq.map(getAuthorizationGraphForNamespace)
          decentralizedNamespaceCache
            .put(
              decentralizedNamespace,
              (tx.mapping, dnsGraph.copy(dnd = tx.mapping, ownerGraphs = ownerGraphs)),
            )
            .discard
      }
    }
    isAuthorized
  }

  private def determineRelevantUidsAndNamespaces(
      toValidate: GenericSignedTopologyTransactionX,
      inStore: Option[GenericTopologyTransactionX],
  ): Future[(UpdateAggregationX, GenericValidatedTopologyTransactionX)] = {
    def verifyDomain(
        tx: GenericSignedTopologyTransactionX
    ): Either[TopologyTransactionRejection, Unit] =
      tx.restrictedToDomain match {
        case Some(txDomainId) =>
          Either.cond(
            domainId.forall(_ == txDomainId),
            (),
            TopologyTransactionRejection.WrongDomain(txDomainId),
          )
        case None => Right(())
      }

    // we need to figure out for which namespaces and uids we need to load the validation checks
    // and for which uids and namespaces we'll have to perform a cascading update
    EitherT
      .fromEither[Future](verifyDomain(toValidate))
      .fold(
        rejection =>
          (UpdateAggregationX(), ValidatedTopologyTransactionX(toValidate, Some(rejection))),
        _ =>
          (
            UpdateAggregationX().add(
              toValidate.mapping,
              inStore,
            ),
            ValidatedTopologyTransactionX(toValidate, None),
          ),
      )
  }
}
