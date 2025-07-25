// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.demo

import com.daml.ledger.javaapi.data.codegen.{Contract, ContractCompanion, ContractId}
import com.daml.ledger.javaapi.data.{Template, Transaction}
import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  ConsoleMacros,
  ParticipantReference,
  SequencerReference,
}
import com.digitalasset.canton.demo.Step.{Action, Noop}
import com.digitalasset.canton.demo.model.ai.java as ME
import com.digitalasset.canton.demo.model.doctor.java as M
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.protocol.DynamicSynchronizerParameters
import com.digitalasset.canton.sequencing.{SequencerConnection, SequencerConnections}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{SynchronizerAlias, config}

import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future, blocking}
import scala.jdk.CollectionConverters.*

class ReferenceDemoScript(
    participants: Seq[ParticipantReference],
    bankingConnection: SequencerConnection,
    medicalConnection: SequencerConnection,
    rootPath: String,
    maxWaitForPruning: Duration,
    editionSupportsPruning: Boolean,
    darPath: Option[String] = None,
    additionalChecks: Boolean = false,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends BaseScript
    with NamedLogging {

  import scala.language.implicitConversions

  import ReferenceDemoScript.*

  implicit def toPrimitive(partyId: PartyId): String = partyId.toProtoPrimitive
  implicit def toScalaSeq[A](l: java.util.List[A]): Seq[A] = l.asScala.toSeq
  implicit def toJavaList[A](l: List[A]): java.util.List[A] = l.asJava

  require(participants.lengthIs > 5, "I need 6 participants for this demo")
  private val sorted = participants.sortBy(_.name)

  private val participant1 = sorted(0)
  private val participant2 = sorted(1)
  private val participant3 = sorted(2)
  private val participant4 = sorted(3)
  private val participant5 = sorted(4)
  private val participant6 = sorted(5)

  import com.digitalasset.canton.console.ConsoleEnvironment.Implicits.*

  val maxImage: Int = 28

  private val readyToSubscribeM = new AtomicReference[Map[String, Long]](Map())

  override def subscriptions(): Map[String, Long] = readyToSubscribeM.get()

  def imagePath: String = s"file:$rootPath/images/"

  private val SequencerBankingAndConnection = (SequencerBanking, bankingConnection)
  private val SequencerMedicalAndConnection = (SequencerMedical, medicalConnection)

  private val settings = Seq(
    (
      "Alice",
      participant1,
      Seq(SequencerMedicalAndConnection),
      Seq("bank", "medical-records", "health-insurance", "doctor"),
    ),
    (
      "Doctor",
      participant2,
      Seq(SequencerMedicalAndConnection, SequencerBankingAndConnection),
      Seq("bank", "medical-records", "health-insurance", "doctor"),
    ),
    (
      "Insurance",
      participant3,
      Seq(SequencerBankingAndConnection, SequencerMedicalAndConnection),
      Seq("bank", "health-insurance"),
    ),
    ("Bank", participant4, Seq(SequencerBankingAndConnection), Seq("bank")),
    ("Registry", participant5, Seq(SequencerMedicalAndConnection), Seq("medical-records")),
  )

  override def parties(): Seq[(String, ParticipantReference)] = partyIdCache.toSeq.map {
    case (name, (_, participant)) => (name, participant)
  }

  private val partyIdCache = mutable.LinkedHashMap[String, (PartyId, ParticipantReference)]()
  private def partyId(name: String): PartyId =
    partyIdCache.getOrElse(name, sys.error(s"Failed to lookup party $name"))._1

  private def darFile(dar: String): String =
    darPath.map(path => s"$path/$dar.dar").getOrElse(s"$rootPath/dars/$dar.dar")

  private val lookupTimeoutSeconds: Long =
    System.getProperty("canton-demo.lookup-timeout-seconds", "40").toLong
  private val lookupTimeout =
    config.NonNegativeDuration.tryFromJavaDuration(
      java.time.Duration.ofSeconds(lookupTimeoutSeconds)
    )
  private val syncTimeout = Some(
    config.NonNegativeDuration.tryFromJavaDuration(
      java.time.Duration.ofSeconds(
        System.getProperty("canton-demo.sync-timeout-seconds", "30").toLong
      )
    )
  )

  private lazy val alice = partyId("Alice")
  private lazy val registry = partyId("Registry")
  private lazy val insurance = partyId("Insurance")
  private lazy val doctor = partyId("Doctor")
  private lazy val bank = partyId("Bank")
  private lazy val processor = partyId("Processor")

  private def aliceLookup[
      TC <: Contract[TCid, T],
      TCid <: ContractId[T],
      T <: Template,
  ](
      companion: ContractCompanion[TC, TCid, T]
  ): TC =
    participant1.ledger_api.javaapi.state.acs.await(companion)(alice, timeout = lookupTimeout)
  private def doctorLookup[
      TC <: Contract[TCid, T],
      TCid <: ContractId[T],
      T <: Template,
  ](
      companion: ContractCompanion[TC, TCid, T]
  ): TC =
    participant2.ledger_api.javaapi.state.acs.await(companion)(doctor, timeout = lookupTimeout)
  private def insuranceLookup[
      TC <: Contract[TCid, T],
      TCid <: ContractId[T],
      T <: Template,
  ](
      companion: ContractCompanion[TC, TCid, T]
  ): TC =
    participant3.ledger_api.javaapi.state.acs
      .await(companion)(insurance, timeout = lookupTimeout)
  private def processorLookup[
      TC <: Contract[TCid, T],
      TCid <: ContractId[T],
      T <: Template,
  ](
      companion: ContractCompanion[TC, TCid, T]
  ): TC =
    participant6.ledger_api.javaapi.state.acs
      .await(companion)(processor, timeout = lookupTimeout)
  private def registryLookup[
      TC <: Contract[TCid, T],
      TCid <: ContractId[T],
      T <: Template,
  ](
      companion: ContractCompanion[TC, TCid, T]
  ): TC =
    participant5.ledger_api.javaapi.state.acs.await(companion)(registry, timeout = lookupTimeout)

  private def execute[T](futs: Seq[Future[T]]): Seq[T] = {
    import scala.concurrent.duration.*
    val seq = Future.sequence(futs)
    Await.result(seq, 120.seconds)
  }

  private def connectSynchronizer(
      participant: ParticipantReference,
      name: SynchronizerAlias,
      connection: SequencerConnection,
  ): Unit = {
    participant.synchronizers.connect_by_config(
      SynchronizerConnectionConfig(
        name,
        SequencerConnections.single(connection),
      )
    )
    participant.synchronizers.reconnect(name).discard
  }

  private val pruningOffset = new AtomicReference[Option[(Long, Instant)]](None)
  val steps = TraceContext.withNewTraceContext("init") { implicit traceContext =>
    List[Step](
      Noop, // pres page nr = page * 2 - 1
      Noop,
      Noop,
      Noop,
      Action(
        "Participants connect to synchronizer(s)",
        "admin-api",
        "participant.synchronizers.register(<name>, \"http(s)://hostname:port\")",
        () => {
          logger.info("Connecting participants to synchronizers")
          val res = settings.flatMap { case (_, participant, synchronizers, _) =>
            synchronizers.map { case (name, connection) =>
              Future {
                blocking {
                  connectSynchronizer(participant, name, connection)
                }
              }
            }
          }
          val _ = execute(res)
        },
      ),
      Noop,
      Action(
        "Participants set up parties",
        "admin-api",
        "participant.parties.enable(NAME)",
        () => {
          execute(settings.map { case (name, participant, synchronizers, _) =>
            logger.info(s"Enabling party $name on participant ${participant.id.toString}")
            MonadUtil
              .sequentialTraverse(synchronizers) { case (synchronizerAlias, _) =>
                Future {
                  blocking {
                    participant.parties.enable(name, synchronizer = Some(synchronizerAlias))
                  }
                }
              }
              .map(parties =>
                (
                  name,
                  parties.headOption
                    .getOrElse(throw new IllegalStateException(s"unable to allocate party $name")),
                  participant,
                )
              )
          }).foreach { case (name, pid, participant) =>
            partyIdCache.put(name, (pid, participant)).discard
            readyToSubscribeM
              .updateAndGet(cur => cur + (name -> ParticipantTab.LedgerBegin))
              .discard[Map[String, Long]]
          }

        },
      ),
      Noop,
      Noop,
      Noop,
      Noop,
      Noop,
      Action(
        "Participants upload DARs",
        "admin-api",
        "participant.dars.upload(<filename>)",
        () => {
          settings.foreach { case (name, participant, synchronizers, dars) =>
            dars.map(darFile).foreach { dar =>
              logger.debug(s"Uploading dar $dar for $name")
              val _ = participant.dars.upload(dar)
            }
            // wait until parties are registered with all synchronizers
            ConsoleMacros.utils.retry_until_true(lookupTimeout) {
              participant.parties
                .hosted(filterParty = name)
                .flatMap(_.participants)
                .flatMap(_.synchronizers)
                .sizeIs == synchronizers.size
            }
            // Force the time proofs to be updated after topology transactions
            // TODO(i13200) The following line can be removed once the ticket is closed
            participant.testing.fetch_synchronizer_times()
          }
        },
      ),
      Action(
        "Create initial state by registering some cash, insurance policies and medical records",
        "ledger-api",
        "create Cash with issuer = Bank, owner = ... ; create Policy with insurance = ...; create Register with ...",
        () => {
          // create cash
          def cashFor(owner: String, qty: Int) =
            new M.bank.Cash(
              bank,
              partyId(owner),
              new M.bank.Amount(qty.toLong, "EUR"),
            ).create.commands

          val a = Future {
            blocking {
              participant4.ledger_api.javaapi.commands
                .submit(
                  Seq(bank),
                  cashFor("Insurance", 100) ++ cashFor("Doctor", 5),
                  optTimeout = syncTimeout,
                )
            }
          }

          // create policy
          val treatments = List("Flu-shot", "Hip-replacement", "General counsel")
          val b = Future {
            blocking {
              participant3.ledger_api.javaapi.commands.submit(
                Seq(insurance),
                new M.healthinsurance.Policy(
                  insurance,
                  alice,
                  bank,
                  treatments,
                  List(),
                ).create.commands,
                optTimeout = syncTimeout,
              )
            }
          }
          // create register
          val c = Future {
            blocking {
              participant5.ledger_api.javaapi.commands.submit(
                Seq(registry),
                new M.medicalrecord.Register(
                  registry,
                  alice,
                  List(),
                  List(),
                ).create.commands,
                optTimeout = syncTimeout,
              )
            }
          }
          val _ = execute(Seq(a, b, c))
        },
      ),
      Noop,
      Action(
        "Doctor offering appointment",
        "ledger-api",
        "Doctor: create OfferAppointment with patient = Alice, doctor = Doctor",
        () => {
          val offer =
            new M.doctor.OfferAppointment(doctor, alice).create.commands
          val _ =
            participant2.ledger_api.javaapi.commands
              .submit(Seq(doctor), offer, optTimeout = syncTimeout)
        },
      ),
      Action(
        "Patient Alice accepts offer",
        "ledger-api",
        "Alice: exercise <offerId> AcceptAppointment with registerId = <registerId>, policyId = <policyId>",
        () => {
          val appointmentEv = aliceLookup(M.doctor.OfferAppointment.COMPANION)
          val policyId = aliceLookup(M.healthinsurance.Policy.COMPANION).id
          val registerId = aliceLookup(M.medicalrecord.Register.COMPANION).id
          val acceptOffer =
            appointmentEv.id
              .exerciseAcceptAppointment(registerId, policyId)
              .commands
          val _ = participant1.ledger_api.javaapi.commands
            .submit(Seq(alice), acceptOffer, optTimeout = syncTimeout)
        },
      ),
      Action(
        "Doctor finalises appointment",
        "ledger-api",
        "Doctor: exercise <appointmentId> TickOff with description=...",
        () => {
          val tickOff = doctorLookup(M.doctor.Appointment.COMPANION).id
            .exerciseTickOff(
              "Did a hip replacement",
              "Hip-replacement",
              new M.bank.Amount(15, "EUR"),
            )
            .commands
          val _ = participant2.ledger_api.javaapi.commands
            .submit(Seq(doctor), tickOff, optTimeout = syncTimeout)
        },
      ),
      Action(
        "Insurance settles claim",
        "ledger-api",
        "Insurance: exercise <claimId> AcceptAndSettleClaim with cashId = <cashId>",
        () => {
          // Force the time proofs to be updated after topology transactions
          // TODO(i13200) The following line can be removed once the ticket is closed
          participant3.testing.fetch_synchronizer_times()
          val withdraw =
            insuranceLookup(M.bank.Cash.COMPANION).id.exerciseSplit(15).commands
          participant3.ledger_api.javaapi.commands
            .submit(Seq(insurance), withdraw, optTimeout = syncTimeout)
            .discard[Transaction]

          def findCashCid =
            participant3.ledger_api.javaapi.state.acs
              .await(M.bank.Cash.COMPANION)(insurance, _.data.amount.quantity == 15)

          // settle claim (will invoke automatic reassignment to the banking synchronizer)
          val settleClaim =
            insuranceLookup(M.healthinsurance.Claim.COMPANION).id
              .exerciseAcceptAndSettleClaim(findCashCid.id)
              .commands
          participant3.ledger_api.javaapi.commands
            .submit(Seq(insurance), settleClaim, optTimeout = syncTimeout)
            .discard[Transaction]
        },
      ),
      Noop,
      Action(
        "Alice takes control over medical records",
        "ledger-api",
        "exercise <registerId> TransferRecords with newRegistry = Alice",
        () => {
          val archiveRequest = aliceLookup(M.medicalrecord.Register.COMPANION).id
            .exerciseTransferRecords(alice)
            .commands
          participant1.ledger_api.javaapi.commands
            .submit(Seq(alice), archiveRequest, optTimeout = syncTimeout)
            .discard[Transaction]
          // wait until the acs of the registry is empty
          ConsoleMacros.utils.retry_until_true(lookupTimeout) {
            participant5.ledger_api.state.acs.of_party(registry).isEmpty
          }
          // now, remember the offset to prune at
          val participantOffset =
            participant5.ledger_api.state.end()
          // Trigger advancement of the clean head, so the previous contracts become safe to prune
          if (editionSupportsPruning) {
            participant5.health
              .ping(
                participant5,
                timeout = 60.seconds,
              )
              .discard // sequencer integrations can be veeeerrrry slow
          }
          pruningOffset.set(Some((participantOffset, Instant.now)))
        },
      ),
      Action(
        "Registry Prunes Ledger",
        "admin-api",
        "pruning prune ledgerEndOffset",
        () => {
          // Wait for the previous contracts to exit the time window needed for crash recovery
          if (editionSupportsPruning) {

            val prunedOffset = pruningOffset
              .get()
              .map { case (offset, started) =>
                val waitUntil = started.plus(maxWaitForPruning).plusSeconds(1)
                // now wait until mediator & participant timeouts elapsed
                val now = Instant.now()
                val waitDurationMaybeNegative = Duration.between(now, waitUntil)
                val waitDuration =
                  if (waitDurationMaybeNegative.isNegative) Duration.ZERO
                  else waitDurationMaybeNegative
                logger.info(s"I have to wait for $waitDuration before I can kick off pruning")
                Threading.sleep(waitDuration.toMillis)
                // now, flush all participants that have some business with this node
                Seq(participant1, participant2, participant5).foreach(p =>
                  participant5.health
                    .ping(p, timeout = 60.seconds)
                    .discard[scala.concurrent.duration.Duration]
                )
                // give the ACS commitment processor some time to catchup
                Threading.sleep(5.seconds.toMillis)
                logger.info(s"Pruning ledger up to offset $offset inclusively")
                participant5.pruning.prune(offset)
                logger.info(s"Pruned ledger up to offset $offset inclusively.")
                offset
              }
              .getOrElse(throw new RuntimeException("Unable to prune the ledger."))
            if (additionalChecks) {
              val transactions =
                participant5.ledger_api.updates
                  .transactions(
                    Set(registry),
                    completeAfter = 5,
                    beginOffsetExclusive = prunedOffset,
                  )
              // ensure we don't see any transactions
              require(transactions.isEmpty, s"transactions should be empty but was $transactions")
            }
          }
          // ensure registry tab resubscribes after the pruning offset
          pruningOffset
            .get()
            .foreach { prunedOffset =>
              readyToSubscribeM.updateAndGet(_ + ("Registry" -> prunedOffset._1))
            }
        },
      ),
      Noop,
      Noop,
      Noop,
      Noop,
      Action(
        "New AI processor participant joins",
        "admin-api",
        "participant parties.enable | synchronizers.connect | upload_dar ai-analysis.dar",
        () => {
          val registerSynchronizerF = Future {
            blocking {
              connectSynchronizer(participant6, SequencerMedical, medicalConnection)
            }
          }
          val filename = darFile("ai-analysis")
          val allF = Seq(participant5, participant1, participant6).map { participant =>
            Future {
              blocking {
                participant.dars.upload(filename)
              }
            }
          } :+ Future {
            blocking {}
          } :+ registerSynchronizerF
          // once all dars are uploaded and we've connected the synchronizer, register the party (as we can flush everything there ...)
          val sf = Future
            .sequence(allF)
            .flatMap(_ =>
              Future {
                blocking {
                  val processorId =
                    participant6.parties
                      .enable(
                        "Processor",
                        synchronizeParticipants = Seq(participant5),
                      )
                  partyIdCache.put("Processor", (processorId, participant6))
                }
              }
            )
          execute(Seq(sf.map { _ =>
            val offer = new ME.aianalysis.OfferAnalysis(
              registry,
              alice,
              processor,
            ).create.commands
            participant5.ledger_api.javaapi.commands
              .submit(Seq(registry), offer, optTimeout = syncTimeout)
              .discard[Transaction]
          })).discard
        },
      ),
      Action(
        "Alice accepts AI Analytics service offer",
        "ledger-api",
        "exercise offer AcceptAnalysis with registerId",
        () => {
          val registerId = aliceLookup(ME.medicalrecord.Register.COMPANION)
          val accept = aliceLookup(ME.aianalysis.OfferAnalysis.COMPANION).id
            .exerciseAcceptAnalysis(registerId.id)
            .commands
          participant1.ledger_api.javaapi.commands
            .submit(Seq(alice), accept, optTimeout = syncTimeout)
            .discard[Transaction]
        },
      ),
      Action(
        "Records are processed and result is recorded",
        "ledger-api",
        "exercise records ProcessingDone with diagnosis = ...; exercise pendingAnalysis RecordResult",
        () => {
          val processingDone = processorLookup(ME.aianalysis.AnonymizedRecords.COMPANION).id
            .exerciseProcessingDone("The patient is very healthy.")
            .commands

          participant6.ledger_api.javaapi.commands
            .submit(Seq(processor), processingDone, optTimeout = syncTimeout)
            .discard[Transaction]

          val resultId = registryLookup(ME.aianalysis.AnalysisResult.COMPANION)
          val recordedResult = registryLookup(ME.aianalysis.PendingAnalysis.COMPANION).id
            .exerciseRecordResult(resultId.id)
            .commands

          participant5.ledger_api.javaapi.commands
            .submit(Seq(registry), recordedResult, optTimeout = syncTimeout)
            .discard[Transaction]
        },
      ),
    )
  }
}

object ReferenceDemoScript {

  private val SequencerBanking = "sequencerBanking"
  private val SequencerMedical = "sequencerMedical"

  private def computeMaxWaitForPruning = {
    val defaultDynamicSynchronizerParameters = DynamicSynchronizerParameters.initialValues(
      topologyChangeDelay = NonNegativeFiniteDuration.tryOfMillis(250),
      protocolVersion = ProtocolVersion.latest,
    )
    val mediatorReactionTimeout = defaultDynamicSynchronizerParameters.mediatorReactionTimeout
    val confirmationResponseTimeout =
      defaultDynamicSynchronizerParameters.confirmationResponseTimeout

    mediatorReactionTimeout.unwrap.plus(confirmationResponseTimeout.unwrap)
  }

  def startup(adjustPath: Boolean, testScript: Boolean)(implicit
      consoleEnvironment: ConsoleEnvironment
  ): Unit = {

    def getSequencer(str: String): SequencerReference =
      consoleEnvironment.sequencers.all
        .find(_.name == str)
        .getOrElse(sys.error(s"can not find synchronizer named $str"))

    val bankingSequencers = consoleEnvironment.sequencers.all.filter(_.name == SequencerBanking)
    val bankingMediators = consoleEnvironment.mediators.all.filter(_.name == "mediatorBanking")
    val bankingSynchronizerId = ConsoleMacros.bootstrap
      .synchronizer(
        synchronizerName = SequencerBanking,
        sequencers = bankingSequencers,
        mediators = bankingMediators,
        synchronizerOwners = bankingSequencers ++ bankingMediators,
        synchronizerThreshold = PositiveInt.one,
        staticSynchronizerParameters =
          StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.forSynchronizer),
      )
      .logical
    val medicalSequencers = consoleEnvironment.sequencers.all.filter(_.name == SequencerMedical)
    val medicalMediators = consoleEnvironment.mediators.all.filter(_.name == "mediatorMedical")
    val medicalSynchronizerId = ConsoleMacros.bootstrap
      .synchronizer(
        synchronizerName = SequencerMedical,
        sequencers = medicalSequencers,
        mediators = medicalMediators,
        synchronizerOwners = medicalSequencers ++ medicalMediators,
        synchronizerThreshold = PositiveInt.one,
        staticSynchronizerParameters =
          StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.forSynchronizer),
      )
      .logical

    val banking = getSequencer(SequencerBanking)
    val medical = getSequencer(SequencerMedical)

    // determine where the assets are
    val location = sys.env.getOrElse("DEMO_ROOT", "demo")
    val noPhoneHome = sys.env.keys.exists(_ == "NO_PHONE_HOME")

    // start all nodes before starting the ui (the ui requires this)
    val (maxWaitForPruning, bankingConnection, medicalConnection) = (
      ReferenceDemoScript.computeMaxWaitForPruning,
      banking.sequencerConnection,
      medical.sequencerConnection,
    )
    val loggerFactory = consoleEnvironment.environment.loggerFactory

    // update synchronizer parameters
    banking.topology.synchronizer_parameters.propose_update(
      bankingSynchronizerId,
      _.update(reconciliationInterval = config.PositiveDurationSeconds.ofSeconds(1)),
    )
    medical.topology.synchronizer_parameters.propose_update(
      medicalSynchronizerId,
      _.update(reconciliationInterval = config.PositiveDurationSeconds.ofSeconds(1)),
    )

    val script = new ReferenceDemoScript(
      consoleEnvironment.participants.all,
      bankingConnection,
      medicalConnection,
      location,
      maxWaitForPruning,
      editionSupportsPruning = consoleEnvironment.environment.isEnterprise,
      darPath =
        if (adjustPath) Some("./community/demo/target/scala-2.13/resource_managed/main") else None,
      additionalChecks = testScript,
      loggerFactory = loggerFactory,
    )(consoleEnvironment.environment.executionContext)
    if (testScript) {
      script.run()
      println("The last emperor is always the worst.")
    } else {
      if (!noPhoneHome) {
        Notify.send()
      }
      val runner = new DemoRunner(new DemoUI(script, loggerFactory))
      runner.startBackground()
    }
    ()
  }
}
