// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.error.TestGroup
import com.digitalasset.canton.health.ComponentHealthState.UnhealthyState
import com.digitalasset.canton.logging.pretty.PrettyUtil
import com.digitalasset.canton.topology.{DefaultTestIdentities, SynchronizerId, UniqueIdentifier}

final class ComponentStatusTest extends BaseTestWordSpec with PrettyUtil {
  "ComponentHealthState" should {
    "pretty print Ok" in {
      ComponentStatus(
        "component",
        ComponentHealthState.Ok(),
        labels = Map.empty,
      ).toString shouldBe "component : Ok()"
    }

    "pretty print Ok w/ details" in {
      ComponentStatus(
        "component",
        ComponentHealthState.Ok(Some("good stuff")),
        labels = Map.empty,
      ).toString shouldBe "component : Ok(good stuff)"
    }

    "pretty print Failed" in {
      ComponentStatus(
        "component",
        ComponentHealthState.failed("broken"),
        labels = Map.empty,
      ).toString shouldBe "component : Failed(broken)"
    }

    "pretty print Failed w/ error" in {
      loggerFactory.suppressErrors(
        ComponentStatus(
          "component",
          ComponentHealthState.Failed(
            UnhealthyState(Some("broken"), Some(TestGroup.NestedGroup.MyCode.MyError("bad")))
          ),
          labels = Map.empty,
        ).toString shouldBe s"component : Failed(broken, error = NESTED_CODE(2,0): this is my error)"
      )
    }

    "pretty print Degraded" in {
      ComponentStatus(
        "component",
        ComponentHealthState.degraded("broken"),
        labels = Map.empty,
      ).toString shouldBe "component : Degraded(broken)"
    }

    "pretty print Degraded w/ error" in {
      loggerFactory.suppressErrors(
        ComponentStatus(
          "component",
          ComponentHealthState.Degraded(
            UnhealthyState(Some("broken"), Some(TestGroup.NestedGroup.MyCode.MyError("bad")))
          ),
          labels = Map.empty,
        ).toString shouldBe s"component : Degraded(broken, error = NESTED_CODE(2,0): this is my error)"
      )
    }
  }

  private val da = DefaultTestIdentities.physicalSynchronizerId.toProtoPrimitive
  private val acme = SynchronizerId(
    UniqueIdentifier.tryCreate("acme", DefaultTestIdentities.namespace)
  ).toPhysical.toProtoPrimitive

  private def ok(name: String): ComponentStatus =
    ComponentStatus(name, ComponentHealthState.Ok(), labels = Map.empty)
  private def labeledOk(name: String, sync: String): ComponentStatus =
    ComponentStatus(
      name,
      ComponentHealthState.Ok(),
      labels = Map(ComponentStatus.SynchronizerLabelKey -> sync),
    )

  "renderGrouped" should {
    val sep = System.lineSeparator()

    "render unlabeled components as before" in {
      ComponentStatus.renderGrouped(
        Seq(
          ok("memory_storage"),
          ComponentStatus("indexer", ComponentHealthState.failed("down"), labels = Map.empty),
        )
      ) shouldBe Seq("memory_storage : Ok()", "indexer : Failed(down)")
    }

    "render a single synchronizer as one sub-section" in {
      ComponentStatus.renderGrouped(
        Seq(labeledOk(name = "sequencer-client", sync = da))
      ) shouldBe Seq(s"""Synchronizers:
                              |\t\t$da
                              |\t\t\tsequencer-client : Ok()""".stripMargin)
    }

    "render one sub-section per synchronizer, sorted by synchronizer id" in {
      ComponentStatus.renderGrouped(
        Seq(
          labeledOk(name = "sequencer-client", sync = da),
          labeledOk(name = "sequencer-client", sync = acme),
        )
      ) shouldBe Seq(
        s"Synchronizers:" +
          s"$sep\t\t$acme$sep\t\t\tsequencer-client : Ok()" +
          s"$sep\t\t$da$sep\t\t\tsequencer-client : Ok()"
      )
    }

    "keep each synchronizer's true state" in {
      ComponentStatus.renderGrouped(
        Seq(
          labeledOk(name = "sequencer-client", sync = acme),
          ComponentStatus(
            "sequencer-client",
            ComponentHealthState.failed("only 0 subscription(s) available"),
            labels = Map(ComponentStatus.SynchronizerLabelKey -> da),
          ),
        )
      ) shouldBe Seq(
        s"Synchronizers:" +
          s"$sep\t\t$acme$sep\t\t\tsequencer-client : Ok()" +
          s"$sep\t\t$da$sep\t\t\tsequencer-client : Failed(only 0 subscription(s) available)"
      )
    }

    "render node-level components first and the synchronizer section last" in {
      ComponentStatus.renderGrouped(
        Seq(
          ok("memory_storage"),
          labeledOk(name = "connected-synchronizer", sync = da),
          labeledOk(name = "sequencer-client", sync = da),
          ok("indexer"),
          labeledOk(name = "connected-synchronizer", sync = acme),
          labeledOk(name = "sequencer-client", sync = acme),
        )
      ) shouldBe Seq(
        "memory_storage : Ok()",
        "indexer : Ok()",
        s"Synchronizers:" +
          s"$sep\t\t$acme$sep\t\t\tconnected-synchronizer : Ok()$sep\t\t\tsequencer-client : Ok()" +
          s"$sep\t\t$da$sep\t\t\tconnected-synchronizer : Ok()$sep\t\t\tsequencer-client : Ok()",
      )
    }

    "ignore labels other than the synchronizer label" in {
      ComponentStatus.renderGrouped(
        Seq(
          ComponentStatus(
            "memory_storage",
            ComponentHealthState.Ok(),
            labels = Map("other" -> "value"),
          )
        )
      ) shouldBe Seq("memory_storage : Ok()")
    }

    "truncate full synchronizer ids to the display form" in {
      val fullId = s"da::${"0123456789abcdef" * 4}::35-0"
      val truncated = s"da::${"0123456789ab"}...::35-0"

      ComponentStatus.renderGrouped(
        Seq(labeledOk(name = "sequencer-client", sync = fullId))
      ) shouldBe Seq(s"Synchronizers:$sep\t\t$truncated$sep\t\t\tsequencer-client : Ok()")
    }

    "render nothing for empty input" in {
      ComponentStatus.renderGrouped(Seq.empty) shouldBe Seq.empty
    }
  }
}
