// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrading

import com.daml.ledger.api.v2.transaction_filter.TransactionShape.{
  TRANSACTION_SHAPE_ACS_DELTA,
  TRANSACTION_SHAPE_LEDGER_EFFECTS,
}
import com.daml.ledger.api.v2.transaction_filter.{
  EventFormat,
  Filters,
  TransactionFormat,
  UpdateFormat,
}
import com.daml.ledger.javaapi.data.Transaction
import com.digitalasset.base.error.ErrorCode
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.damltests.bar.v1.java.bar.Bar as BarV1
import com.digitalasset.canton.damltests.bar.v2.java.bar.Bar as BarV2
import com.digitalasset.canton.damltests.baz.v1.java.baz.Baz as BazV1
import com.digitalasset.canton.damltests.baz.v2.java.baz.Baz as BazV2
import com.digitalasset.canton.damltests.foo.v1.java.foo.Foo
import com.digitalasset.canton.damltests.foo.v1.java.foo.Foo as FooV1
import com.digitalasset.canton.damltests.foo.v2.java.foo.Foo as FooV2
import com.digitalasset.canton.damltests.foo.v3.java.foo.Foo as FooV3
import com.digitalasset.canton.damltests.foo.v4.java.foo.Foo as FooV4
import com.digitalasset.canton.damltests.ibaz.v1.java.ibaz.IBaz
import com.digitalasset.canton.damltests.qux.v1.java.qux.Qux
import com.digitalasset.canton.damltests.qux.v1.java.qux.Qux as QuxV1
import com.digitalasset.canton.damltests.qux.v2.java.qux.Qux as QuxV2
import com.digitalasset.canton.error.TransactionRoutingError
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.upgrading.UpgradingBaseTest.Syntax.*
import com.digitalasset.canton.integration.util.PartiesAllocator
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors.{
  Interpreter,
  PackageSelectionFailed,
}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.topology.transaction.VettedPackage
import com.digitalasset.canton.util.SetupPackageVetting
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{LfPackageId, LfPackageName}

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.OptionConverters.RichOption
import scala.util.chaining.scalaUtilChainingOps

// TODO(#25385): This systematic test suite is a stub. Enrich with the following cases:
//               - exercise Foo_Exe receives an interface contract from the outside instead of
//                 creating the contract locally
//               - Add multi-root node tests
//               - Add multi-synchronizer tests
//               - Use TestSubmissionService to mock the package-map creation and avoid
//                 spending the time on the required vetting setups
class SystematicTopologyAwareUpgradingIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  @volatile private var aliceParticipant, bobParticipant,
      charlieParticipant: LocalParticipantReference = _
  @volatile private var alice, bob, charlie: PartyId = _
  private val AllDars = Set(
    UpgradingBaseTest.IBaz,
    UpgradingBaseTest.IBar,
    UpgradingBaseTest.UtilV1,
    UpgradingBaseTest.UtilV2,
    UpgradingBaseTest.BazV1,
    UpgradingBaseTest.BazV2,
    UpgradingBaseTest.BarV1,
    UpgradingBaseTest.BarV2,
    UpgradingBaseTest.FooV1,
    UpgradingBaseTest.FooV2,
    UpgradingBaseTest.FooV3,
    UpgradingBaseTest.FooV4,
  )

  private lazy val AllVettedUpToV3: Map[ParticipantReference, Set[VettedPackage]] =
    Map(
      aliceParticipant -> Set(
        // These packages below are vetted transitively by FooV2 and FooV3.
        // Vet them explicitly only when no implicit implementation is vetted
        //   - IBar.PACKAGE_ID.toPackageId.withNoVettingBounds,
        //   - IBaz.PACKAGE_ID.toPackageId.withNoVettingBounds,
        //   - BarV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
        //   - BazV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
        FooV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
        FooV2.PACKAGE_ID.toPackageId.withNoVettingBounds,
        FooV3.PACKAGE_ID.toPackageId.withNoVettingBounds,
        // Baz and Bar V2 need to be vetted explicitly since they are not a static dependency of any other package
        BarV2.PACKAGE_ID.toPackageId.withNoVettingBounds,
        BazV2.PACKAGE_ID.toPackageId.withNoVettingBounds,
      ),
      bobParticipant -> Set(
        // These two are vetted implicitly by Baz and Bar.
        // Vet them explicitly only when no implicit implementation is vetted
        //   - IBar.PACKAGE_ID.toPackageId.withNoVettingBounds,
        //   - IBaz.PACKAGE_ID.toPackageId.withNoVettingBounds,
        BarV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
        BarV2.PACKAGE_ID.toPackageId.withNoVettingBounds,
        BazV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
        BazV2.PACKAGE_ID.toPackageId.withNoVettingBounds,
      ),
    )

  private lazy val AllVetted: Map[ParticipantReference, Set[VettedPackage]] =
    AllVettedUpToV3
      .updatedWith(aliceParticipant)(_.map(_ + FooV4.PACKAGE_ID.toPackageId.withNoVettingBounds))
      .updated(
        charlieParticipant,
        Set(
          // These two are vetted implicitly by Baz and Bar.
          // Vet them explicitly only when no implicit implementation is vetted
          //   - IBar.PACKAGE_ID.toPackageId.withNoVettingBounds,
          //   - IBaz.PACKAGE_ID.toPackageId.withNoVettingBounds,
          BarV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
          BarV2.PACKAGE_ID.toPackageId.withNoVettingBounds,
          BazV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
          BazV2.PACKAGE_ID.toPackageId.withNoVettingBounds,
        ),
      )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4_S1M1
      .withSetup { implicit env =>
        import env.*

        // Disambiguate participants
        aliceParticipant = participant1
        bobParticipant = participant2
        charlieParticipant = participant3

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)

        // Setup the party topology state
        inside(
          PartiesAllocator(Set(aliceParticipant, bobParticipant, charlieParticipant))(
            newParties = Seq(
              "alice" -> aliceParticipant,
              "bob" -> bobParticipant,
              "charlie" -> charlieParticipant,
            ),
            targetTopology = Map(
              "alice" -> Map(
                daId -> (PositiveInt.one, Set(aliceParticipant.id -> Submission))
              ),
              "bob" -> Map(
                daId -> (PositiveInt.one, Set(bobParticipant.id -> Submission))
              ),
              "charlie" -> Map(
                daId -> (PositiveInt.one, Set(charlieParticipant.id -> Submission))
              ),
            ),
          )
        ) { case Seq(p_alice, p_bob, p_charlie) =>
          alice = p_alice
          bob = p_bob
          charlie = p_charlie
        }
      }

  // In this test, multiple combinations of vetting setups are evaluated against the Foo_Exe exercise submitted by alice, which
  // brings in bob (and in V4 charlie) as informee(s) in a child transaction depending on the vetting state.
  //
  // The test aims at purely evaluating the effect of different vetting combinations
  // on the chosen packages in the transaction resulting from running the Foo_Exe by alice (if successful).
  // For this reason, the templates are as simple as possible, with no changing record types.
  // For the same simplicity reason, both versions of Bar and Baz use the same template definition.
  //
  // Below, the transaction tree is succinctly shown, for each version of Foo
  //
  // Note:
  // - IBar_Exe pertains to IBar interface and is implemented by Bar V1 and V2
  // - IBaz_Exe pertains to IBaz interface and is implemented by Baz V1 and V2
  //
  // (V1)        Foo_Exe (alice)
  //                |
  //
  // (V2)        Foo_Exe (alice)
  //                |
  //                Create Bar (alice)
  //                |
  //                Exercise IBar_Exe ~ Bar_Vx (alice, bob)
  //
  // (V3)        Foo_Exe  (alice)
  //               |
  //               Create Baz_V1 (alice)
  //               |
  //               Exercise IBaz_Exe ~ Baz_Vx (alice, bob)
  //
  // (V4)        Foo_Exe  (alice)
  //               |
  //               Create Baz_V1 (alice)
  //               |
  //               Exercise IBaz_Exe ~ Baz_Vx (alice, bob)
  //               |
  //               Create Bar_V1 (alice)
  //               |
  //               Exercise IBar_Exe ~ Bar_Vx (alice, charlie)
  //
  // Note:
  //       - Bar V1,V2 have a static dependency to Util V1 - utility package
  //       - Baz V1,V2 have a static dependency to Util V2 - non-schema package with serializable types
  //       - Charlie is introduced only in tests involving Foo V4 to assert multiple versions of a static dependency
  //         in an exercise
  "Systematic topology-aware upgrading test" when {
    "All Vetted up to V3" should {
      "succeed using Foo V3 - Baz V2" in { implicit env =>
        SetupPackageVetting(AllDars, Map(env.daId -> AllVettedUpToV3))
        test(
          bobSees = Some(BazV2.PACKAGE_ID),
          expectedExerciseVersion = FooV3.PACKAGE_ID,
        )
      }
    }

    "alice only vetted Foo V4, bob vetted Baz V1 and charlie vetted Bar V1" should {
      "succeed using Foo V4 - Baz V1 - Bar V1" in { implicit env =>
        SetupPackageVetting(
          AllDars,
          Map(
            env.daId -> Map(
              aliceParticipant -> Set(
                FooV4.PACKAGE_ID.toPackageId.withNoVettingBounds,
                // Baz and Bar V2 need to be vetted explicitly since they are not a static dependency of any other package
                BarV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
                BarV2.PACKAGE_ID.toPackageId.withNoVettingBounds,
                BazV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
                BazV2.PACKAGE_ID.toPackageId.withNoVettingBounds,
              ),
              bobParticipant -> Set(
                BazV1.PACKAGE_ID.toPackageId.withNoVettingBounds
              ),
              charlieParticipant -> Set(
                BarV1.PACKAGE_ID.toPackageId.withNoVettingBounds
              ),
            )
          ),
        )
        test(
          bobSees = Some(BazV1.PACKAGE_ID),
          expectedExerciseVersion = FooV4.PACKAGE_ID,
          charlieSees = Some(BarV1.PACKAGE_ID),
        )
      }
    }

    "bob vetted only Baz V1" should {
      "succeed using Foo V3 - Baz V1" in { implicit env =>
        SetupPackageVetting(
          AllDars,
          Map(
            env.daId -> AllVettedUpToV3.updated(
              bobParticipant,
              Set(BazV1.PACKAGE_ID.toPackageId.withNoVettingBounds),
            )
          ),
        )
        test(
          bobSees = Some(BazV1.PACKAGE_ID),
          expectedExerciseVersion = FooV3.PACKAGE_ID,
        )
      }
    }

    "bob vets only Bar V1" should {
      "succeed using Foo V2 - Bar V1 / new package (Baz) introduced for bob in V3 can be forgotten in pass 2" in {
        implicit env =>
          SetupPackageVetting(
            AllDars,
            Map(
              env.daId -> AllVettedUpToV3.updated(
                bobParticipant,
                Set(BarV1.PACKAGE_ID.toPackageId.withNoVettingBounds),
              )
            ),
          )
          // Fails because it needs 3 passes
          testError(
            expectedErrorCode =
              TransactionRoutingError.ConfigurationErrors.InvalidPrescribedSynchronizerId,
            expectedErrorMessage = "Some packages are not known to all informees on synchronizer",
            tapsMaxPasses = Some(2),
          )

          // 1st pass selects Foo V3 and Baz V2 -> Routing fails because Bob has not vetted Baz V2.
          // 2nd pass selects Foo V2 and Bar V2 -> Routing fails because Bob has not vetted Bar V2.
          // 3rd pass selects Foo V2 and Bar V1 -> Success
          test(
            bobSees = Some(BarV1.PACKAGE_ID),
            expectedExerciseVersion = FooV2.PACKAGE_ID,
          )
      }
    }

    // Negative test case
    "alice did not vet Foo V1 and bob vetted only Bar V2" should {
      "succeed using Foo V2 and Bar V2" in { implicit env =>
        SetupPackageVetting(
          AllDars,
          Map(
            env.daId -> Map(
              aliceParticipant -> Set(
                FooV2.PACKAGE_ID.toPackageId.withNoVettingBounds,
                FooV3.PACKAGE_ID.toPackageId.withNoVettingBounds,
                BarV2.PACKAGE_ID.toPackageId.withNoVettingBounds,
                BazV2.PACKAGE_ID.toPackageId.withNoVettingBounds,
              ),
              bobParticipant -> Set(BarV2.PACKAGE_ID.toPackageId.withNoVettingBounds),
            )
          ),
        )
        // 1st pass selects Foo V3 and Baz V2 -> Routing fails because Bob has not vetted Baz V1.
        // 2nd pass selects Foo V2 and Bar V2 -> Success
        test(
          bobSees = Some(BarV2.PACKAGE_ID),
          expectedExerciseVersion = FooV2.PACKAGE_ID,
        )
      }
    }

    "bob vets Bar V1 and IBaz (no Baz)" should {
      "succeed using Foo V2 - Bar V1 / IBaz dynamic dependency introduced for bob in V3 can be forgotten in pass 2" in {
        implicit env =>
          SetupPackageVetting(
            AllDars,
            Map(
              env.daId -> AllVettedUpToV3.updated(
                bobParticipant,
                Set(
                  IBaz.PACKAGE_ID.toPackageId.withNoVettingBounds,
                  BarV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
                ),
              )
            ),
          )

          // Fails because it needs 3 passes
          testError(
            expectedErrorCode =
              TransactionRoutingError.ConfigurationErrors.InvalidPrescribedSynchronizerId,
            expectedErrorMessage = "Some packages are not known to all informees on synchronizer",
            tapsMaxPasses = Some(2),
          )

          // 1st pass selects Foo V3 and Baz V2 -> Routing fails because Bob has not vetted Baz V2.
          // 2nd pass selects Foo V2 and Bar V2 -> Routing fails because Bob has not vetted Bar V2.
          // 3rd pass selects Foo V2 and Bar V1 -> Success
          test(
            bobSees = Some(BarV1.PACKAGE_ID),
            expectedExerciseVersion = FooV2.PACKAGE_ID,
          )
      }
    }

    "alice vets FooV1 and FooV3/BazV1 and bob vets only Bar V1" should {
      "succeed using Foo V1 - no implication for bob" in { implicit env =>
        SetupPackageVetting(
          AllDars,
          Map(
            env.daId -> Map(
              aliceParticipant -> Set(
                FooV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
                FooV3.PACKAGE_ID.toPackageId.withNoVettingBounds,
              ),
              bobParticipant -> Set(
                BarV1.PACKAGE_ID.toPackageId.withNoVettingBounds
              ),
            )
          ),
        )
        test(
          bobSees = None,
          expectedExerciseVersion = FooV1.PACKAGE_ID,
        )
      }
    }

    "alice vets Foo V1 and Foo V2/Bar V1 and bob vets only Baz V1" should {
      "succeed using Foo V1 - no implication for bob" in { implicit env =>
        SetupPackageVetting(
          AllDars,
          Map(
            env.daId -> Map(
              aliceParticipant -> Set(
                FooV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
                FooV2.PACKAGE_ID.toPackageId.withNoVettingBounds,
              ),
              bobParticipant -> Set(
                BazV1.PACKAGE_ID.toPackageId.withNoVettingBounds
              ),
            )
          ),
        )
        test(
          bobSees = None,
          expectedExerciseVersion = FooV1.PACKAGE_ID,
        )
      }
    }

    "bob does not vet Baz V2" should {
      "succeed with Foo V3 - Baz V1 exercised" in { implicit env =>
        SetupPackageVetting(
          AllDars,
          Map(
            env.daId -> AllVettedUpToV3.updated(
              bobParticipant,
              Set(
                BarV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
                BazV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
              ),
            )
          ),
        )
        test(
          bobSees = Some(BazV1.PACKAGE_ID),
          expectedExerciseVersion = FooV3.PACKAGE_ID,
        )
      }
    }

    // Negative test case
    "bob doesn't vet anything" should {
      "succeed using Foo V1" in { implicit env =>
        SetupPackageVetting(
          AllDars,
          Map(env.daId -> AllVettedUpToV3.updated(bobParticipant, Set.empty)),
        )

        // Fails because it needs 3 passes
        testError(
          expectedErrorCode =
            TransactionRoutingError.ConfigurationErrors.InvalidPrescribedSynchronizerId,
          expectedErrorMessage = "Some packages are not known to all informees on synchronizer",
          tapsMaxPasses = Some(2),
        )

        // 1st pass selects Foo V3 and Baz V2 -> Routing fails because Bob has not vetted Baz V2.
        // 2nd pass selects Foo V2 and Bar V2 -> Routing fails because Bob has not vetted Bar V2.
        // 3rd pass selects Foo V1 -> Success.
        test(
          bobSees = None,
          expectedExerciseVersion = FooV1.PACKAGE_ID,
        )
      }
    }

    // Test cases involving Charlie
    "All Vetted" should {
      "succeed using Foo V4 - Baz V2 - Bar V2" in { implicit env =>
        SetupPackageVetting(AllDars, Map(env.daId -> AllVetted))
        test(
          bobSees = Some(BazV2.PACKAGE_ID),
          expectedExerciseVersion = FooV4.PACKAGE_ID,
          charlieSees = Some(BarV2.PACKAGE_ID),
        )
      }
    }

    "commands are run with package preferences injection" when {
      "All Vetted up to V3" should {
        "succeed using Foo V3 - Baz V2" in { implicit env =>
          SetupPackageVetting(AllDars, Map(env.daId -> AllVettedUpToV3))
          test(
            bobSees = Some(BazV2.PACKAGE_ID),
            expectedExerciseVersion = FooV3.PACKAGE_ID,
            vettingRequirementsForPreferencesInjection = Some(
              Map(
                Foo.PACKAGE_NAME.toPackageName -> Set(alice),
                BazV1.PACKAGE_NAME.toPackageName -> Set(bob),
              )
            ),
          )
        }
      }

      // Negative test case
      "bob vets only a Bar package" should {
        "fail with wrong preferences package_id_selection_preferences provided" in { implicit env =>
          import env.*
          SetupPackageVetting(
            AllDars,
            Map(
              env.daId -> AllVettedUpToV3.updated(
                bobParticipant,
                Set(BarV1.PACKAGE_ID.toPackageId.withNoVettingBounds),
              )
            ),
          )
          testError(
            expectedErrorCode = PackageSelectionFailed,
            expectedErrorMessage =
              s"""|No synchronizers satisfy the topology requirements for the submitted command: Discarded synchronizers:.*
                  |.*$daId: Failed to select package-id for package-name '${FooV1.PACKAGE_NAME}' appearing in a command root node due to: No vetted package candidate satisfies the package-id filter 'Commands.package_id_selection_preference'=.*foo -> ${FooV3.PACKAGE_ID.toPackageId.show}.*
                  |.*Candidates:.*${FooV2.PACKAGE_ID.toPackageId.show}.*""".stripMargin,
            vettingRequirementsForPreferencesInjection = Some(
              Map(
                Foo.PACKAGE_NAME.toPackageName -> Set(alice),
                BarV1.PACKAGE_NAME.toPackageName -> Set(bob),
              )
            ),
          )
        }
      }

      "All Vetted" should {
        "succeed using Foo V4 - Baz V2 - Bar V2" in { implicit env =>
          SetupPackageVetting(AllDars, Map(env.daId -> AllVetted))
          test(
            bobSees = Some(BazV2.PACKAGE_ID),
            expectedExerciseVersion = FooV4.PACKAGE_ID,
            charlieSees = Some(BarV2.PACKAGE_ID),
            vettingRequirementsForPreferencesInjection = Some(
              Map(
                Foo.PACKAGE_NAME.toPackageName -> Set(alice),
                BazV1.PACKAGE_NAME.toPackageName -> Set(bob),
                BarV1.PACKAGE_NAME.toPackageName -> Set(charlie),
              )
            ),
          )
        }
      }

      "alice only vetted Foo V4, bob vetted Baz V1 and charlie vetted Bar V1" should {
        "succeed using Foo V4 - Baz V1 - Bar V1" in { implicit env =>
          SetupPackageVetting(
            AllDars,
            Map(
              env.daId -> Map(
                aliceParticipant -> Set(
                  FooV4.PACKAGE_ID.toPackageId.withNoVettingBounds,
                  // Baz and Bar V2 need to be vetted explicitly since they are not a static dependency of any other package
                  BarV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
                  BarV2.PACKAGE_ID.toPackageId.withNoVettingBounds,
                  BazV1.PACKAGE_ID.toPackageId.withNoVettingBounds,
                  BazV2.PACKAGE_ID.toPackageId.withNoVettingBounds,
                ),
                bobParticipant -> Set(
                  BazV1.PACKAGE_ID.toPackageId.withNoVettingBounds
                ),
                charlieParticipant -> Set(
                  BarV1.PACKAGE_ID.toPackageId.withNoVettingBounds
                ),
              )
            ),
          )
          test(
            bobSees = Some(BazV1.PACKAGE_ID),
            expectedExerciseVersion = FooV4.PACKAGE_ID,
            charlieSees = Some(BarV1.PACKAGE_ID),
            vettingRequirementsForPreferencesInjection = Some(
              Map(
                Foo.PACKAGE_NAME.toPackageName -> Set(alice),
                BazV1.PACKAGE_NAME.toPackageName -> Set(bob),
                BarV1.PACKAGE_NAME.toPackageName -> Set(charlie),
              )
            ),
          )
        }
      }
    }
  }

  private def testError(
      expectedErrorCode: ErrorCode,
      expectedErrorMessage: String,
      vettingRequirementsForPreferencesInjection: Option[Map[LfPackageName, Set[PartyId]]] = None,
      tapsMaxPasses: Option[Int] = None,
  ): Unit = {
    val fooCid = createFoo()
    assertThrowsAndLogsCommandFailures(
      exerciseFoo(
        fooCid,
        vettingRequirementsForPreferencesInjection,
        addCharlie = false,
        tapsMaxPasses,
      ),
      entry => {
        entry.shouldBeCantonErrorCode(expectedErrorCode)
        entry.message should include regex expectedErrorMessage
      },
    )
  }

  private def test(
      bobSees: Option[String],
      expectedExerciseVersion: String,
      vettingRequirementsForPreferencesInjection: Option[Map[LfPackageName, Set[PartyId]]] = None,
      charlieSees: Option[String] = None,
      tapsMaxPasses: Option[Int] = None,
  ): Unit = {
    val addCharlie = charlieSees.isDefined
    val fooCid = createFoo()
    val tx =
      exerciseFoo(fooCid, vettingRequirementsForPreferencesInjection, addCharlie, tapsMaxPasses)
    tx.getEvents.asScala.headOption.value.toProtoEvent.getExercised.getTemplateId.getPackageId shouldBe expectedExerciseVersion

    val updateId = tx.getUpdateId
    val bobsCreatePkgId =
      // There is one template per package-id, so we can just check the package-id of the create event
      // for a deterministic assertion
      bobParticipant.ledger_api.updates
        .update_by_id(
          updateId,
          updateFormat = UpdateFormat(
            Some(
              TransactionFormat(
                eventFormat = Some(
                  EventFormat(
                    filtersByParty = Map(bob.toProtoPrimitive -> Filters(Nil)),
                    filtersForAnyParty = None,
                    verbose = false,
                  )
                ),
                transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
              )
            ),
            None,
            None,
          ),
        )
        .flatMap(_.createEvents.toSeq.loneElement.templateId.map(_.packageId))

    // If bob doesn't see any of the packages, we expect the update to not contain any events
    bobsCreatePkgId shouldBe bobSees

    if (addCharlie) {
      val charliesCreatePkgId =
        charlieParticipant.ledger_api.updates
          .update_by_id(
            updateId,
            updateFormat = UpdateFormat(
              Some(
                TransactionFormat(
                  eventFormat = Some(
                    EventFormat(
                      filtersByParty = Map(charlie.toProtoPrimitive -> Filters(Nil)),
                      filtersForAnyParty = None,
                      verbose = false,
                    )
                  ),
                  transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
                )
              ),
              None,
              None,
            ),
          )
          .flatMap(_.createEvents.toSeq.loneElement.templateId.map(_.packageId))

      charliesCreatePkgId shouldBe charlieSees
    }
  }

  private def createFoo(): FooV4.ContractId = {
    val contractId = aliceParticipant.ledger_api.javaapi.commands
      .submit(
        Seq(alice),
        new Foo(alice.toProtoPrimitive).create().commands().asScala.toList,
      )
      .getEvents
      .asScala
      .loneElement
      .toProtoEvent
      .getCreated
      .getContractId
    new FooV4.ContractId(contractId)
  }

  private def exerciseFoo(
      fooCid: FooV4.ContractId,
      vettingRequirementsForPreferencesInjection: Option[Map[LfPackageName, Set[PartyId]]],
      addCharlie: Boolean,
      tapsMaxPasses: Option[Int],
  ): Transaction = {
    val packagePreferencesO = vettingRequirementsForPreferencesInjection
      .map(vettingRequirements =>
        aliceParticipant.ledger_api.interactive_submission.preferred_packages(vettingRequirements)
      )
      .map(_.packageReferences.map(_.packageId.toPackageId))
    aliceParticipant.ledger_api.javaapi.commands
      .submit(
        Seq(alice),
        fooCid
          .exerciseFoo_Exe(
            alice.toProtoPrimitive,
            bob.toProtoPrimitive,
            Option.when(addCharlie)(charlie.toProtoPrimitive).toJava,
          )
          .commands()
          .asScala
          .toList,
        transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
        userPackageSelectionPreference = packagePreferencesO.getOrElse(Seq.empty),
        tapsMaxPasses = tapsMaxPasses,
      )
  }

  /* Limitation: TAPS fails as soon as it interprets a transaction requiring a missing submitter,
   * even if it could fallback on another version of the same package that do not require this new
   * submitter.
   *
   * This test uses a simplistic Qux template with a single signatory and a single observer:
   * `template Qux with owner: Party, obs: Party`
   *
   * Qux_Exe is a choice on Qux. The number of controllers change between two versions of the package:
   * (V1) `controller owner`
   * (V2) `controller [owner, obs]`
   */
  "Systematic topology-aware upgrading test with added controllers" when {
    "fails because of missing submitter during interpretation" in { implicit env =>
      val quxDars = Set(UpgradingBaseTest.QuxV1, UpgradingBaseTest.QuxV2)
      val vettedPackages =
        Set(QuxV1.PACKAGE_ID, QuxV2.PACKAGE_ID).map(_.toPackageId.withNoVettingBounds)
      val vettingState: Map[ParticipantReference, Set[VettedPackage]] =
        Seq(aliceParticipant, bobParticipant).map(_ -> vettedPackages).toMap
      SetupPackageVetting(quxDars, Map(env.daId -> vettingState))

      val quxCid = aliceParticipant.ledger_api.javaapi.commands
        .submit(
          Seq(alice),
          new Qux(alice.toProtoPrimitive, bob.toProtoPrimitive).create().commands().asScala.toList,
        )
        .getEvents
        .asScala
        .loneElement
        .toProtoEvent
        .getCreated
        .getContractId
        .pipe(new QuxV2.ContractId(_))

      def exercise(packagePreferences: Seq[LfPackageId]): Transaction =
        aliceParticipant.ledger_api.javaapi.commands.submit(
          Seq(alice),
          quxCid.exerciseQux_Exe().commands().asScala.toList,
          transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
          userPackageSelectionPreference = packagePreferences,
        )

      // Failed interpretation because TAPS pass 1 selects Qux V2
      assertThrowsAndLogsCommandFailures(
        exercise(packagePreferences = Seq.empty),
        entry => {
          entry.shouldBeCantonErrorCode(Interpreter.AuthorizationError)
          entry.message should include regex s"Interpretation error: Error: node .* \\(${QuxV2.PACKAGE_ID}:Qux:Qux\\) requires authorizers ${alice.toProtoPrimitive},${bob.toProtoPrimitive}, but only ${alice.toProtoPrimitive} were given"
        },
      )

      // Succeed with Qux V1 as preferred package
      val tx = exercise(packagePreferences = Seq(LfPackageId.assertFromString(QuxV1.PACKAGE_ID)))
      val exercisedPackageId =
        tx.getEvents.asScala.headOption.value.toProtoEvent.getExercised.getTemplateId.getPackageId
      exercisedPackageId shouldBe QuxV1.PACKAGE_ID
    }
  }
}
