// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.scenarios

import better.files.File
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.performance.scenarios.OfflinePartyReplication.log
import com.digitalasset.canton.topology.transaction.{ParticipantPermission, PartyToParticipant}
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{SynchronizerAlias, config}

import java.util.concurrent.atomic.AtomicReference
import scala.util.chaining.scalaUtilChainingOps
import scala.util.control.NonFatal

/** Usage:
  *
  *   1. Invoke `val partyReplication = OfflinePartyReplication.getPartyToReplicate(SP, TP,
  *      partyHint)` to obtain a candidate party to replicate
  *   1. Invoke `partyReplication.replicateParty()` to start the party replication process
  *   1. After the party replication has finished, invoke `partyReplication.clearOnboardingFlag()`.
  *
  * For example:
  * {{{
  *   val partyInfo = OfflinePartyReplication.getPartyToReplicate(participant1, participant3, "Trader").get
  *   partyInfo.replicateParty()
  *   partyInfo.clearOnboardingFlag()
  * }}}
  */
final class OfflinePartyReplication private (
    val partyId: PartyId,
    val sourceParticipant: ParticipantReference,
    val targetParticipant: ParticipantReference,
    val synchronizerId: SynchronizerId,
    val synchronizerAlias: SynchronizerAlias,
    override protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  val ptpOnboarding = new AtomicReference[Option[PartyToParticipant]]

  /** Replicate the party using the offline party replication in a slightly modified form (e.g.
    * skipping backups, and also with the option of not clearing the onboarding flag).
    *
    * @param reconnectTimeoutO
    *   how long to wait for synchronizer reconnect, should be None usually
    * @param clearOnboardingFlagAfterOffPR
    *   whether to clear the onboarding flag explicitly after onpr (off by default)
    */
  def replicateParty(
      reconnectTimeoutO: Option[config.NonNegativeDuration] = None,
      clearOnboardingFlagAfterOffPR: Boolean = false,
  ): Unit = {
    implicit val traceContext: TraceContext =
      TraceContext.createNew("OfflinePartyReplication.replicateParty")
    val fromLedgerEnd = sourceParticipant.ledger_api.state.end()

    // OffPR steps from https://archived.docs.digitalasset.com/operate/3.5/howtos/operate/parties/party_replication.html#offline-party-replication-steps
    log_("skip: 1. Package Vetting")(())
    log_("skip: 2. Source: Data Retention")(())

    val ptp = log_("3. Target: Authorization")(propose(targetParticipant))
    ptpOnboarding.set(Some(ptp))

    log_("4. Target: Isolation")(targetParticipant.synchronizers.disconnect(synchronizerAlias))

    try {
      log_("5. Source: Party Authorization")(propose(sourceParticipant).discard)

      File.usingTemporaryFile() { file =>
        log_(s"6. Source: ACS Export to temp-file ${file.path}")(
          sourceParticipant.parties.export_party_acs(
            party = partyId,
            synchronizerId = synchronizerId,
            targetParticipantId = targetParticipant.id,
            beginOffsetExclusive = fromLedgerEnd,
            exportFilePath = file.canonicalPath,
          )
        )

        log_("skip: 7. Target: Backup")(())

        log_(s"8. Target: ACS Import file temp-file of size ${file.size}")(
          targetParticipant.parties
            .import_party_acs(synchronizerId, Some(partyId), file.canonicalPath)
        )
      }
    } finally {
      // Finally reconnect regardless to return the system to its original state
      // even if OffPR steps 4-8 fail.
      log_("9. Target: Reconnect")(
        targetParticipant.synchronizers
          .reconnect(synchronizerAlias, synchronize = reconnectTimeoutO)
          .discard
      )
    }

    if (clearOnboardingFlagAfterOffPR) {
      clearOnboardingFlag()
    } else {
      log_("skip: 10. Target: Onboarding Flag Clearance clearance")(())
    }

    /* Disabled auto-clearance of onboarding flag, so don't wait:
    log_("10. Await: Target: Onboarding Flag Clearance")(
      ConsoleMacros.utils.retry_until_true(clearOnboardingFlagTimeout)(
        targetParticipant.topology.party_to_participant_mappings
          .list(
            synchronizerId = synchronizerId,
            filterParty = partyId.filterString,
          )
          .exists(
            _.item.participants
              .exists(hp => hp.participantId == targetParticipant.id && !hp.onboarding)
          )
      )
    )
     */
  }

  /** Clear the onboarding flag via topology. This allows controlling the point in time at which the
    * new AcsCommitmentProcessor on the TP is impacted load-wise.
    */
  def clearOnboardingFlag(): Unit = {
    implicit val traceContext: TraceContext =
      TraceContext.createNew("OfflinePartyReplication.replicateParty")

    val ptp = ptpOnboarding.get.getOrElse(
      throw new RuntimeException("clearOnboardingFlag() only allowed after relicateParty()")
    )

    log_("10. Target: Onboarding Flag Clearance explicit clearance")(
      targetParticipant.topology.party_to_participant_mappings
        .propose(
          party = partyId,
          store = synchronizerId,
          newParticipants = ptp.participants.map(hp => hp.participantId -> hp.permission),
        )
        .discard
    )
  }

  private def propose(p: ParticipantReference): PartyToParticipant =
    p.topology.party_to_participant_mappings
      .propose_delta(
        partyId,
        adds = List((targetParticipant.id, ParticipantPermission.Observation)),
        store = synchronizerId,
        requiresPartyToBeOnboarded = true,
      )
      .mapping

  // Log helper to follow along in the console what is happening and to gauge timing in logs
  // after the fact.
  private def log_[T](msg: String)(code: => T)(implicit traceContext: TraceContext): T =
    try {
      log(s"$partyId: begin $msg", logger)
      code
    } catch {
      case NonFatal(e) =>
        log(s"$partyId: ERROR(${e.getMessage}): $msg ", logger)
        throw e
    } finally {
      log(s"$partyId:   end $msg", logger)
    }
}

object OfflinePartyReplication {

  /** Choose a party that exists on the source participant (SP), but not on the target participant
    * (TP), i.e. a party that is suitable to be replicated from the SP to the TP.
    *
    * @param partyHint
    *   substring that has to be part of the name of a party, e.g. "Trader"
    * @return
    *   OfflinePartyReplication class to invoke replicateParty() and clearOnboardingFlag() on if a
    *   common, connected synchronizer exists and if a party to replicate can be found from the
    *   sourceParticipant to the targetParticipant.
    */
  def getPartyToReplicate(
      sourceParticipant: ParticipantReference,
      targetParticipant: ParticipantReference,
      partyHint: String = "",
  ): Option[OfflinePartyReplication] = {
    implicit val traceContext: TraceContext =
      TraceContext.createNew("OfflinePartyReplication.getPartyToReplicate")
    val logger = NamedLoggerFactory.root.getTracedLogger(OfflinePartyReplication.getClass)
    val spSynchronizers = sourceParticipant.synchronizers
      .list_connected()
      .map(res => res.synchronizerId -> res.synchronizerAlias)
      .toMap
    val tpSynchronizers = targetParticipant.synchronizers
      .list_connected()
      .map(res => res.synchronizerId -> res.synchronizerAlias)
      .toMap
    val commonSynchronizers = spSynchronizers.keySet intersect tpSynchronizers.keySet
    for {
      synchronizerId <- commonSynchronizers.headOption.tap(synchronizerIdO =>
        if (synchronizerIdO.isEmpty)
          log(
            s"No common synchronizer and source (${spSynchronizers
                .mkString(", ")}) and target (${tpSynchronizers.mkString(", ")}).",
            logger,
          )
      )
      synchronizerAlias = spSynchronizers.getOrElse(
        synchronizerId,
        throw new IllegalStateException(
          "should find alias in synchronizer sp and tp intersection"
        ),
      )
      partyId <-
        sourceParticipant.topology.party_to_participant_mappings
          .list(
            synchronizerId
          )
          .map(_.item)
          .collectFirst {
            case PartyToParticipant(partyId, _, participants, _)
                if partyId.uid.toProtoPrimitive.contains(partyHint) && participants.exists(
                  _.participantId == sourceParticipant.id
                ) && !participants
                  .exists(_.participantId == targetParticipant.id) =>
              partyId
          }
          .tap(partyIdO =>
            if (partyIdO.isEmpty)
              log(
                s"No party found that is hosted on the source $sourceParticipant but not on the target ($targetParticipant).",
                logger,
              )
          )
    } yield new OfflinePartyReplication(
      partyId,
      sourceParticipant,
      targetParticipant,
      synchronizerId,
      synchronizerAlias,
      NamedLoggerFactory.root,
    )
  }

  private def log(msg: String, logger: TracedLogger)(implicit traceContext: TraceContext): Unit = {
    println(s"OffPR $msg")
    logger.info(msg)
  }
}
