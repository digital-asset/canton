// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import com.daml.error.definitions.{DamlError, PackageServiceError}
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.refinements.ApiTypes.WorkflowId
import com.daml.ledger.api.v1.commands.Command
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.event.Event.Event.{Archived, Created}
import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.client.binding.{Primitive => P}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.String255
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.ledger.api.client.{CommandSubmitterWithRetry, LedgerSubmit}
import com.digitalasset.canton.lifecycle.AsyncOrSyncCloseable
import com.digitalasset.canton.logging.{SuppressingLogger, TracedLogger}
import com.digitalasset.canton.participant.admin.PackageService.{Dar, DarDescriptor}
import com.digitalasset.canton.participant.admin.workflows.{DarDistribution => M}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.{immutable, mutable}
import scala.concurrent.Future

class DarDistributionServiceTest extends AsyncWordSpec with BaseTest {

  val Alice = Converters.toParty("alice")
  val Bob = Converters.toParty("bob")

  val SuccessfulSetup = TestSetup("successful")
  val IncorrectHashSetup = TestSetup("incorrect-hash")
  val FailToAppendSetup = TestSetup("fail-to-append")
  val setups = Seq(SuccessfulSetup, IncorrectHashSetup, FailToAppendSetup)

  "DarDistributionService" can {
    "successfully share a dar to another participant" in {
      val f = new Fixture
      import f._

      for {
        _ <- alices.service.share(SuccessfulSetup.hash, Bob)
        requests <- alices.service.listRequests()
        _ = requests should have size 1
        offers <- bobs.service.listOffers()
        _ = offers should have size 1
        offer = offers.headOption.getOrElse(fail("expected an offer"))
        _ <- bobs.service.accept(offer.contractId)
        bobsDars <- bobs.darService.listDars()
      } yield bobsDars.map(_.hash) should contain(SuccessfulSetup.hash)
    }

    "rejecting removes the request from both sides" in {
      val f = new Fixture
      import f._

      for {
        _ <- alices.service.share(SuccessfulSetup.hash, Bob)
        offer <- bobs.service.listOffers().map(_.headOption.getOrElse(fail("missing offer")))
        _ <- alices.logger.assertLogs(
          bobs.service.reject(offer.contractId, "Nah - I'm fine"),
          _.warningMessage should include("has been rejected"),
        )

        requests <- alices.service.listRequests()
        offers <- bobs.service.listOffers()
      } yield {
        requests shouldBe empty
        offers shouldBe empty
      }
    }

    "error if the dar to share isn't registered with the participant" in {
      val f = new Fixture
      import f._

      for {
        result <- alices.service.share(TestHash.digest("this dar doesn't exist"), Bob)
      } yield result.left.value should be(ShareError.DarNotFound)
    }

    "not register the dar sharing request as pending if we fail to submit to the ledger" in {
      val f = new Fixture
      import f._

      setupFailingSubmitBehavior()

      for {
        result <- alices.service.share(SuccessfulSetup.hash, Bob)
        _ = result.left.value should matchPattern { case ShareError.SubmissionFailed(_) => }
        requests <- alices.service.listRequests()
        offers <- bobs.service.listOffers()
      } yield {
        requests shouldBe empty
        offers shouldBe empty
      }
    }

    "leave share request and offer pending if appending shared dar the ledger fails" in {
      val f = new Fixture
      import f._

      for {
        _ <- alices.service.share(FailToAppendSetup.hash, Bob)
        offer <- bobs.service.listOffers().map(_.headOption.getOrElse(fail("expected offer")))

        acceptResult <- bobs.service.accept(offer.contractId)

        requests <- alices.service.listRequests()
        offers <- bobs.service.listOffers()
      } yield {
        acceptResult.left.value should matchPattern { case AcceptRejectError.FailedToAppendDar(_) =>
        }
        requests should have size 1
        offers should have size 1
      }
    }

    "dar with incorrect hash is automatically rejected" in {
      val f = new Fixture
      import f._

      for {
        _ <- bobs.logger.assertLogs(
          {
            alices.logger.assertLogs(
              alices.service.share(IncorrectHashSetup.hash, Bob),
              _.warningMessage should include("has been rejected"),
            )
          },
          _.warningMessage should include("does not match provided hash"),
        )
        requests <- alices.service.listRequests()
        offers <- bobs.service.listRequests()
      } yield {
        requests shouldBe empty
        offers shouldBe empty
      }
    }

    "invalid dar is automatically rejected" in {
      val errorMsg = "Error message"
      val f = new Fixture(bobDarValidation = _ => Left(errorMsg))
      import f._

      for {
        _ <- bobs.logger.assertLogs(
          {
            alices.logger.assertLogs(
              alices.service.share(SuccessfulSetup.hash, Bob),
              _.warningMessage should include("has been rejected"),
            )
          },
          _.warningMessage should include(errorMsg),
        )
        requests <- alices.service.listRequests()
        offers <- bobs.service.listRequests()
      } yield {
        requests shouldBe empty
        offers shouldBe empty
      }
    }

    "automatically install dar if owner is whitelisted" in {
      val f = new Fixture
      import f._

      for {
        _ <- bobs.whitelistStore.whitelist(Alice)
        _ <- alices.service.share(SuccessfulSetup.hash, Bob)
        installedDars <- bobs.darService.listDars()
      } yield {
        installedDars.map(_.hash) should contain.only(SuccessfulSetup.hash)
      }
    }
  }

  case class TestSetup(name: String) {
    lazy val content: ByteString = ByteString.copyFromUtf8(name)
    lazy val hash: Hash = Hash.digest(HashPurpose.DarIdentifier, content, HashAlgorithm.Sha256)
    lazy val dar: Dar = Dar(DarDescriptor(hash, String255.tryCreate(name)), content.toByteArray)
  }

  private class PartySetup(
      owner: P.Party,
      mockLedgerConnector: MockLedgerConnector,
      validation: ByteString => Either[String, Unit] = _ => Right(()),
  ) {
    val shareRequestStore = new InMemoryShareRequestStore
    val shareOfferStore = new InMemoryShareOfferStore
    val whitelistStore = new InMemoryWhitelistStore
    val darService = new MockDarService
    val logger = SuppressingLogger(classOf[DarDistributionServiceTest])
      .append("party", Converters.toString(owner))

    val service = new DarDistributionService(
      mockLedgerConnector.createSubmit(),
      validation,
      owner,
      darService,
      new MockHashOps,
      shareRequestStore,
      shareOfferStore,
      whitelistStore,
      isActive = true,
      logger,
    )
  }

  /** The bulk of support in this fixture is to support mocking the ledger content in response to submitted commands.
    * If this proves useful extract to a general utility for testing admin services.
    */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private class Fixture(
      aliceDarValidation: ByteString => Either[String, Unit] = _ => Right(()),
      bobDarValidation: ByteString => Either[String, Unit] = _ => Right(()),
  ) {
    val nextContractId = new AtomicInteger()
    val templateTracker = mutable.Map[String, Identifier]()
    val ledgerConnector = new MockLedgerConnector
    val alices = new PartySetup(Alice, ledgerConnector, aliceDarValidation)
    val bobs = new PartySetup(Bob, ledgerConnector, bobDarValidation)

    // we're heavily relying on the ScalaTest default serial execution context to
    // execute all futures during one test step before starting the next
    def processTransaction(tx: Transaction): Unit = executionContext.execute { () =>
      List(alices, bobs).map(_.service.processTransaction(tx))
    }

    def processTransaction(events: Event.Event*): Unit =
      processTransaction(Transaction("tx", "cmd", "wf", None, events.map(Event(_)), "offset"))
    def newContractId(templateId: Identifier): String = {
      val id = s"c${nextContractId.getAndIncrement}"
      templateTracker.put(id, templateId)
      id
    }
    def newContractId(templateId: Option[Identifier]): String =
      newContractId(templateId.getOrElse(sys.error("templateId required")))

    def setupDefaultSubmitBehavior(): Unit = {
      ledgerConnector.onSubmit { cmd =>
        cmd.command.create.foreach { create =>
          val tx = Transaction(
            "tx",
            "cmd",
            "wf",
            None,
            Seq(
              Event(
                Created(
                  CreatedEvent(
                    "e1",
                    newContractId(create.templateId),
                    create.templateId,
                    None,
                    create.createArguments,
                  )
                )
              )
            ),
          )

          processTransaction(tx)
        }
        cmd.command.exercise.foreach { exercise =>
          exercise.choice match {
            case "Reject" =>
              val rejectArgs = M.RejectedDar(Alice, Bob, "hash", "reason")
              processTransaction(
                Archived(
                  ArchivedEvent(
                    "e2",
                    exercise.contractId,
                    Some(templateTracker(exercise.contractId)),
                  )
                ),
                Created(
                  CreatedEvent(
                    "e3",
                    newContractId(tid(M.RejectedDar.id)),
                    Some(tid(M.RejectedDar.id)),
                    None,
                    Some(M.RejectedDar.toNamedArguments(rejectArgs)),
                  )
                ),
              )
            case "Accept" =>
              val acceptArgs = M.AcceptedDar(Alice, Bob, "hash")
              processTransaction(
                Archived(
                  ArchivedEvent(
                    "e2",
                    exercise.contractId,
                    Some(templateTracker(exercise.contractId)),
                  )
                ),
                Created(
                  CreatedEvent(
                    "e3",
                    newContractId(tid(M.RejectedDar.id)),
                    Some(tid(M.AcceptedDar.id)),
                    None,
                    Some(M.AcceptedDar.toNamedArguments(acceptArgs)),
                  )
                ),
              )
            case _ =>
            // not stubbing transaction for command
          }
        }
        Future.successful(CommandSubmitterWithRetry.Success(Completion()))
      }
    }

    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    def setupFailingSubmitBehavior(): Unit = {
      ledgerConnector.onSubmit(_ => Future.successful(CommandSubmitterWithRetry.Failed(null)))
    }

    setupDefaultSubmitBehavior()
  }

  def tid[T](id: P.TemplateId[T]): Identifier = ApiTypes.TemplateId.unwrap(id)

  private class MockLedgerConnector {
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    private var submitAction: Option[Command => Future[CommandSubmitterWithRetry.CommandResult]] =
      None

    def onSubmit(action: Command => Future[CommandSubmitterWithRetry.CommandResult]): Unit = {
      submitAction = Some(action)
    }

    def createSubmit(): LedgerSubmit = new LedgerSubmit {
      override val timeouts = DarDistributionServiceTest.this.timeouts

      override def submitCommand(
          command: Seq[Command],
          commandId: Option[String] = None,
          workflowId: Option[WorkflowId] = None,
          deduplicationTime: Option[NonNegativeFiniteDuration] = None,
      )(implicit traceContext: TraceContext): Future[CommandSubmitterWithRetry.CommandResult] =
        submitAction.flatMap(act => command.headOption.map(cmd => (act, cmd))) match {
          case Some((action, cmd)) => action(cmd)
          case None => sys.error("submitAction not yet set in MockLedgerConnector or weird command")
        }

      override def submitAsync(
          commands: Seq[Command],
          commandId: Option[String],
          workflowId: Option[WorkflowId],
          deduplicationTime: Option[NonNegativeFiniteDuration] = None,
      )(implicit traceContext: TraceContext): Future[Unit] = throw new IllegalArgumentException(
        "Method not defined"
      )

      override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = List.empty

      override protected def logger: TracedLogger = DarDistributionServiceTest.this.logger
    }
  }

  private class MockDarService extends DarService {
    private val dars = mutable.Buffer[DarDescriptor]()

    def suppressInternalError(reason: String): PackageServiceError.InternalError.Generic = {
      loggerFactory.assertLogs(
        PackageServiceError.InternalError.Generic(reason),
        logEntry => {
          logEntry.message should include(
            "PACKAGE_SERVICE_INTERNAL_ERROR(4,0): Generic error (please check the reason string)."
          )
          logEntry.mdc("err-context") should include(s"reason=${reason}")
        },
      )
    }

    override def appendDarFromByteString(
        dar: ByteString,
        name: String,
        vetAllPackages: Boolean,
        synchronizeVetting: Boolean,
    )(implicit traceContext: TraceContext): EitherT[Future, DamlError, Hash] = {

      name match {
        case SuccessfulSetup.name =>
          dars.append(DarDescriptor(SuccessfulSetup.hash, String255.tryCreate(name)))
          EitherT.rightT(SuccessfulSetup.hash)
        case FailToAppendSetup.name =>
          EitherT.leftT(suppressInternalError("Append failed"))
        case _ =>
          EitherT.leftT(suppressInternalError(s"Unknown dar: $name"))
      }
    }

    override def getDar(
        hash: Hash
    )(implicit traceContext: TraceContext): Future[Option[PackageService.Dar]] =
      Future.successful {
        setups
          .find(_.hash == hash)
          .map(setup =>
            PackageService.Dar(
              DarDescriptor(setup.hash, String255.tryCreate(setup.name)),
              setup.content.toByteArray,
            )
          )
      }

    override def listDars(
        limit: Option[Int] = None
    )(implicit traceContext: TraceContext): Future[immutable.Seq[PackageService.DarDescriptor]] =
      Future.successful {
        dars.toList
      }
  }

  private class MockHashOps extends HashOps {
    override def digest(
        purpose: HashPurpose,
        bytes: ByteString,
        algorithm: HashAlgorithm = defaultHashAlgorithm,
    ): Hash = {

      bytes match {
        case IncorrectHashSetup.content => TestHash.digest("a different hash")
        case _ => super.digest(purpose, bytes)
      }
    }

    override def defaultHashAlgorithm: HashAlgorithm = HashAlgorithm.Sha256
  }
}
