// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.daml.error.definitions.DamlError
import com.daml.ledger.api.v1.commands.Command
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.client.binding.{Contract, Primitive => P}
import com.digitalasset.canton.crypto.{Hash, HashOps, HashPurpose}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.AcceptRejectError.OfferNotFound
import com.digitalasset.canton.participant.admin.ShareError.DarNotFound
import com.digitalasset.canton.participant.admin.workflows.{DarDistribution => M}
import com.digitalasset.canton.participant.ledger.api.client.CommandSubmitterWithRetry.{
  CommandResult,
  Success => CommandSuccess,
}
import com.digitalasset.canton.participant.ledger.api.client.DecodeUtil.{
  decodeAllArchived,
  decodeAllCreated,
}
import com.digitalasset.canton.participant.ledger.api.client.LedgerSubmit
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureUtil
import com.google.protobuf.ByteString

import java.util.Base64
import scala.concurrent.{ExecutionContext, Future}

/** Store for DAR sharing requests I've sent that haven't yet been accepted or rejected */
trait ShareRequestStore extends SimpleContractTrackerStore[M.ShareDar]

/** Store for received DAR sharing offers I've not yet responded to */
trait ShareOfferStore extends SimpleContractTrackerStore[M.ShareDar]

/** Store for parties I have whitelisted DARs from (permitting them to be automatically installed) */
trait WhitelistStore {

  /** Whitelist the given party. Whitelisting the same party more than once will not produce an error. */
  def whitelist(party: P.Party): Future[Unit]

  /** Revoke whitelisting status from this party. Revoking a party that is not whitelisted will not produce an error. */
  def revoke(party: P.Party): Future[Unit]

  /** List all parties currently whitelisted. */
  def list(): Future[Seq[P.Party]]

  /** Check whether the party is currently whitelisted. */
  def isWhitelisted(party: P.Party): Future[Boolean]
}

class InMemoryShareRequestStore
    extends InMemorySimpleTrackerStore[M.ShareDar]
    with ShareRequestStore
class InMemoryShareOfferStore extends InMemorySimpleTrackerStore[M.ShareDar] with ShareOfferStore
class InMemoryWhitelistStore extends WhitelistStore {
  import scala.jdk.CollectionConverters._
  private val parties = new java.util.concurrent.ConcurrentHashMap[P.Party, Unit]
  override def whitelist(party: P.Party): Future[Unit] = Future.successful {
    parties.put(party, ())
  }
  override def revoke(party: P.Party): Future[Unit] = Future.successful { parties remove party }
  override def list(): Future[Seq[P.Party]] = Future.successful { parties.keySet().asScala.toList }
  override def isWhitelisted(party: P.Party): Future[Boolean] = Future.successful {
    parties containsKey party
  }
}

sealed trait ShareError
object ShareError {

  /** The DAR was not found in the local package service.
    * A DAR must first be loaded into this participant before it can be shared with others.
    */
  object DarNotFound extends ShareError

  /** Failed to submit the Share request to the ledger */
  case class SubmissionFailed(result: CommandResult) extends ShareError
}

sealed trait AcceptRejectError
object AcceptRejectError {

  /** Could not find a matching offer to respond to. */
  object OfferNotFound extends AcceptRejectError

  /** We could not append the DAR to our ledger. May indicate a problem with the DAR. */
  case class FailedToAppendDar(reason: DamlError) extends AcceptRejectError

  /** Failed to submit the response to the share offer to the ledger */
  case class SubmissionFailed(result: CommandResult) extends AcceptRejectError

  case class InvalidOffer(error: String) extends AcceptRejectError
}

/** Exception to carry the accept reject error where a `Future[Unit]` return is expected (transaction processing) */
class AcceptRejectException(error: AcceptRejectError)
    extends RuntimeException(s"Failed to accept or reject with error [$error]")

/** Dar distribution operations */
trait DarDistribution {

  /** Share a DAR identified by its hash to the given recipient party.
    * @param darHash Hash of a DAR already installed in this participant. The hash can be found with the admin command dars.list().
    * @param recipient Recipient party to receive the share request. This only makes sense to the be a admin party of another participant but is not enforced.
    * @return Error or Unit
    */
  def share(darHash: Hash, recipient: P.Party)(implicit
      traceContext: TraceContext
  ): Future[Either[ShareError, Unit]]

  /** Accept a previously received share request */
  def accept(id: P.ContractId[M.ShareDar])(implicit
      traceContext: TraceContext
  ): Future[Either[AcceptRejectError, Unit]]

  /** Reject a previously received share request */
  def reject(id: P.ContractId[M.ShareDar], reason: String)(implicit
      traceContext: TraceContext
  ): Future[Either[AcceptRejectError, Unit]]

  /** List all pending requests to share a DAR with other participants */
  def listRequests(): Future[Seq[Contract[M.ShareDar]]]

  /** List all pending offers to receive a DAR */
  def listOffers(): Future[Seq[Contract[M.ShareDar]]]

  /** Whitelist a party so DARs are automatically installed without a manual approval of the offer */
  def whitelistAdd(owner: P.Party): Future[Unit]

  /** Remove a party from the whitelist. Will not error if the given party is not whitelisted. */
  def whitelistRemove(owner: P.Party): Future[Unit]

  /** List all currently whitelisted parties */
  def whitelistList(): Future[Seq[P.Party]]
}

class DarDistributionService(
    connection: LedgerSubmit,
    darValidation: ByteString => Either[String, Unit],
    adminParty: P.Party,
    darService: DarService,
    hashOps: HashOps,
    shareRequestStore: ShareRequestStore = new InMemoryShareRequestStore,
    shareOfferStore: ShareOfferStore = new InMemoryShareOfferStore,
    whitelistStore: WhitelistStore = new InMemoryWhitelistStore,
    isActive: => Boolean,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends AdminWorkflowService
    with DarDistribution
    with NamedLogging {
  import cats.data._
  import cats.implicits._

  /** Async processing of the transaction.
    * TODO(danilo): promote async processing to [[AdminWorkflowService]] and our LedgerConnection
    */
  override private[admin] def processTransaction(
      tx: Transaction
  )(implicit traceContext: TraceContext): Unit = {
    if (isActive) {
      val actions = Seq(
        decodeAllCreated(M.ShareDar)(tx) map processShareCreated,
        decodeAllArchived(M.ShareDar)(tx) map processShareArchived,
        decodeAllCreated(M.AcceptedDar)(tx) map processAcceptedDarCreated,
        decodeAllCreated(M.RejectedDar)(tx) map processRejectedDarCreated,
      )
      val flatActions = actions.flatten

      FutureUtil.doNotAwait(
        flatActions.sequence_,
        s"failed to process Dar distribution service transaction ${tx.transactionId}",
      )
    }
  }

  private def processShareCreated(
      share: Contract[M.ShareDar]
  )(implicit traceContext: TraceContext): Future[Unit] =
    if (share.value.owner == adminParty) {
      shareRequestStore.add(share)
    } else {
      import M.ShareDar._
      // make sure the DAR request is legit
      val content = decode(share.value.content)
      val hash = Hash.tryFromHexString(share.value.hash)
      if (!checkDarHashMatches(hash, content)) {
        // automatically reject
        val rejectCommand =
          share.contractId
            .exerciseReject("Hash does not match DAR content")
            .command
        submitOrFail(rejectCommand)
      } else
        darValidation(content) match {
          case Right(()) =>
            for {
              // record offer
              _ <- shareOfferStore.add(share)
              _ <- autoAcceptIfWhitelisted(share)
            } yield ()
          case Left(msg) =>
            val rejectCommand = share.contractId.exerciseReject(msg).command
            logger.warn(s"Shared DAR is invalid. Reason: $msg")
            submitOrFail(rejectCommand)
        }
    }

  private def autoAcceptIfWhitelisted(
      share: Contract[M.ShareDar]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    def autoAccept(): Future[Unit] = {
      logger.info(s"auto accepting shared dar [${share.value.hash}:${share.value.name}]")
      // auto-accept, but translate Either[AcceptRejectError, Unit] to a Future that will fail if AcceptRejectError is returned
      accept(share)
        .leftMap(new AcceptRejectException(_))
        .value
        .transform(_.flatMap(_.toTry))
    }

    for {
      isOwnerWhitelisted <- whitelistStore.isWhitelisted(share.value.owner)
      _ = logger.debug(
        s"shared dar [${share.value.hash}:${share.value.name}] has whitelisted owner: ${isOwnerWhitelisted}"
      )
      _ <- if (isOwnerWhitelisted) autoAccept() else Future.unit
    } yield ()
  }

  private def processShareArchived(shareId: P.ContractId[M.ShareDar]): Future[Unit] =
    List(shareRequestStore, shareOfferStore).traverse_(store => store.remove(shareId))

  private def processAcceptedDarCreated(
      acceptance: Contract[M.AcceptedDar]
  )(implicit traceContext: TraceContext): Future[Unit] =
    if (acceptance.value.darOwner == adminParty) {
      import M.AcceptedDar._
      logger.info(
        s"Dar [${acceptance.value.hash}] has been accepted by [${acceptance.value.recipient}]"
      )
      val ackCommand = acceptance.contractId.exerciseAcknowledgeAcceptance().command
      submitOrFail(ackCommand)
    } else Future.unit

  private def processRejectedDarCreated(
      rejection: Contract[M.RejectedDar]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    if (rejection.value.darOwner == adminParty) {
      import M.RejectedDar._
      logger.warn(
        s"Dar [${rejection.value.hash}] has been rejected by [${rejection.value.recipient}]: ${rejection.value.reason}"
      )
      val ackCommand = rejection.contractId.exerciseAcknowledgeRejection().command
      submitOrFail(ackCommand)
    } else Future.unit
  }

  override def share(hash: Hash, recipient: P.Party)(implicit
      traceContext: TraceContext
  ): Future[Either[ShareError, Unit]] =
    (for {
      // check the package exists on this participant before sharing with others
      dar <- EitherT.fromOptionF(darService.getDar(hash), DarNotFound: ShareError)
      shareContract = M.ShareDar(
        adminParty,
        recipient,
        hash.toHexString,
        dar.descriptor.name.toProtoPrimitive,
        encode(ByteString.copyFrom(dar.bytes)),
      )
      _ <- submit[ShareError](shareContract.create.command)(ShareError.SubmissionFailed)
    } yield ()).value

  override def accept(
      shareId: P.ContractId[M.ShareDar]
  )(implicit traceContext: TraceContext): Future[Either[AcceptRejectError, Unit]] = {
    (for {
      offer <- EitherT.fromOptionF(shareOfferStore.get(shareId), OfferNotFound: AcceptRejectError)
      _ <- accept(offer)
    } yield ()).value
  }

  override def reject(shareId: P.ContractId[M.ShareDar], reason: String)(implicit
      traceContext: TraceContext
  ): Future[Either[AcceptRejectError, Unit]] = {
    import M.ShareDar._
    (for {
      offer <- EitherT.fromOptionF(shareOfferStore.get(shareId), OfferNotFound: AcceptRejectError)
      rejectCommand = offer.contractId.exerciseReject(reason).command
      _ <- submit[AcceptRejectError](rejectCommand)(AcceptRejectError.SubmissionFailed)
    } yield ()).value
  }

  override def listRequests(): Future[Seq[Contract[M.ShareDar]]] = shareRequestStore.list()
  override def listOffers(): Future[Seq[Contract[M.ShareDar]]] = shareOfferStore.list()
  override def whitelistAdd(owner: P.Party): Future[Unit] = whitelistStore.whitelist(owner)
  override def whitelistRemove(owner: P.Party): Future[Unit] = whitelistStore.revoke(owner)
  override def whitelistList(): Future[Seq[P.Party]] = whitelistStore.list()

  override def close(): Unit = connection.close()

  private def accept(
      offer: Contract[M.ShareDar]
  )(implicit traceContext: TraceContext): EitherT[Future, AcceptRejectError, Unit] = {
    import M.ShareDar._
    for {
      _hash <- Hash
        .fromHexString(offer.value.hash)
        .leftMap(err => AcceptRejectError.InvalidOffer(err.message))
        .toEitherT[Future]
      _ <- appendDar(offer.value.name, decode(offer.value.content))
      acceptCommand = offer.contractId.exerciseAccept().command
      _ <- submit[AcceptRejectError](acceptCommand)(AcceptRejectError.SubmissionFailed)
    } yield ()
  }

  private def submit[E](command: Command)(
      failureHandler: CommandResult => E
  )(implicit traceContext: TraceContext): EitherT[Future, E, Unit] =
    for {
      submissionResult <- EitherT.right[E](connection.submitCommand(Seq(command)))
      resultEither: Either[E, Unit] = submissionResult match {
        case CommandSuccess(_) => Right(())
        case failure => Left(failureHandler(failure))
      }
      _ <- EitherT.fromEither[Future](resultEither)
    } yield ()

  private def submitOrFail(command: Command)(implicit traceContext: TraceContext): Future[Unit] =
    connection.submitCommand(Seq(command)) flatMap {
      case CommandSuccess(_) => Future.unit
      case failure =>
        Future.failed(new RuntimeException(s"Command submission failed with [$failure]"))
    }

  private def encode(content: ByteString): String =
    Base64.getEncoder.encodeToString(content.toByteArray)
  private def decode(content: String): ByteString =
    ByteString.copyFrom(Base64.getDecoder.decode(content))

  private def checkDarHashMatches(expectedHash: Hash, content: ByteString)(implicit
      traceContext: TraceContext
  ): Boolean = {
    val actualHash = hashOps.digest(HashPurpose.DarIdentifier, content)
    val matches = expectedHash == actualHash

    if (!matches) {
      logger.warn(
        s"Shared DAR content hash [$actualHash] does not match provided hash [$expectedHash]"
      )
    }

    matches
  }

  private def appendDar(name: String, content: ByteString)(implicit
      traceContext: TraceContext
  ): EitherT[Future, AcceptRejectError, Unit] =
    darService
      .appendDarFromByteString(content, name, vetAllPackages = true, synchronizeVetting = false)
      .map(_ => ())
      .leftMap(AcceptRejectError.FailedToAppendDar)

}
